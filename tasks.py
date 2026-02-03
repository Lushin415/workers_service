"""
Фоновые задачи мониторинга
"""
import asyncio
import threading
from datetime import datetime
from typing import List
from loguru import logger

from parser import TelegramParser
from message_extractor import MessageExtractor
from filters import ItemFilter
from db_service import DBService
from tg_notifier import TelegramNotifier
from state_manager import state_manager
from models_db import FoundItem
from deduplicator import Deduplicator


class MonitoringTask:
    """Класс для управления фоновой задачей мониторинга"""

    def __init__(
        self,
        task_id: str,
        user_id: int,
        mode: str,
        chats: List[str],
        filters_dict: dict,
        api_id: int,
        api_hash: str,
        notification_bot_token: str,
        notification_chat_id: int,
        parse_history_days: int
    ):
        self.task_id = task_id
        self.user_id = user_id
        self.mode = mode
        self.chats = chats
        self.api_id = api_id
        self.api_hash = api_hash
        self.notification_bot_token = notification_bot_token
        self.notification_chat_id = notification_chat_id
        self.parse_history_days = parse_history_days

        # Создаем фильтр
        self.item_filter = ItemFilter(
            date_from=filters_dict['date_from'],
            date_to=filters_dict['date_to'],
            min_price=filters_dict['min_price'],
            max_price=filters_dict['max_price'],
            shk_filter=filters_dict['shk_filter']
        )

        # Сервисы
        self.db = DBService()
        self.parser = None
        self.notifier = TelegramNotifier(notification_bot_token, notification_chat_id)

        # Событие остановки
        self.stop_event = state_manager.create_task(task_id, mode)

    async def process_message(self, message, chat_name: str):
        """
        Обработать сообщение из Telegram

        Args:
            message: объект сообщения Pyrogram
            chat_name: имя чата
        """
        try:
            # Обновляем счетчик обработанных сообщений
            state_manager.update_stats(self.task_id, messages_scanned=1)

            # Извлекаем данные
            message_text = message.text
            message_date = message.date

            extracted = MessageExtractor.extract(message_text, message_date)

            if not extracted:
                return

            # Проверяем тип (должен соответствовать режиму)
            if extracted['type'] != self.mode:
                return

            # Применяем фильтры
            if not self.item_filter.matches(extracted):
                return

            # Формируем данные для сохранения
            author_username = message.from_user.username if message.from_user else None
            author_full_name = None
            if message.from_user:
                author_full_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip()

            # Формируем ссылку на сообщение
            message_link = f"https://t.me/{chat_name.lstrip('@')}/{message.id}"

            # Создаем хеш содержимого для умной дедупликации
            # (БЕЗ учета даты работы - если дата изменится, это новое объявление)
            content_hash = Deduplicator.create_content_hash(
                author_username=author_username,
                price=extracted['price'],
                location=extracted.get('location'),
                message_text=message_text
            )

            # Создаем объект для БД
            found_item = FoundItem(
                id=None,
                task_id=self.task_id,
                mode=self.mode,
                author_username=author_username,
                author_full_name=author_full_name,
                date=extracted['date'],
                price=extracted['price'],
                shk=extracted.get('shk'),
                location=extracted.get('location'),
                message_text=message_text,
                message_link=message_link,
                chat_name=chat_name,
                message_date=message_date.isoformat(),
                found_at=datetime.utcnow().isoformat(),
                notified=False,
                content_hash=content_hash
            )

            # Сохраняем в БД (с дедупликацией)
            item_id = await self.db.add_found_item(found_item)

            if item_id:
                # Обновляем статистику
                state_manager.update_stats(self.task_id, items_found=1)

                # Отправляем уведомление
                notification_data = {
                    'date': extracted['date'],
                    'price': extracted['price'],
                    'shk': extracted.get('shk'),
                    'location': extracted.get('location'),
                    'author_username': author_username,
                    'author_full_name': author_full_name,
                    'chat_name': chat_name,
                    'message_link': message_link,
                    'message_text': message_text
                }

                sent = await self.notifier.send_notification(notification_data, item_id, self.mode)

                if sent:
                    await self.db.mark_as_notified(item_id)
                    state_manager.update_stats(self.task_id, notifications_sent=1)
                    logger.info(f"Найдено и отправлено новое объявление: {message_link}")

        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")

    async def run_async(self):
        """Асинхронная часть задачи"""
        try:
            # Инициализируем БД
            await self.db.init_db()

            # Создаем парсер (используем общую сессию для всех задач)
            self.parser = TelegramParser(
                api_id=self.api_id,
                api_hash=self.api_hash,
                session_name="workers_session"  # Используем общую авторизованную сессию
            )

            # Запускаем клиент
            await self.parser.start()

            # Обновляем статус
            state_manager.update_status(self.task_id, "running")

            # Парсим историю
            logger.info(f"Начинаем парсинг истории для задачи {self.task_id}")
            for chat in self.chats:
                if self.stop_event.is_set():
                    break

                await self.parser.parse_history(
                    chat_username=chat,
                    days=self.parse_history_days,
                    handler=self.process_message
                )

            # Настраиваем real-time мониторинг
            if not self.stop_event.is_set():
                logger.info(f"Настраиваем real-time мониторинг для задачи {self.task_id}")
                self.parser.setup_realtime_handler(
                    chat_usernames=self.chats,
                    handler=self.process_message
                )

                # Ждем сигнала остановки
                await self.parser.run_until_stopped(self.stop_event)

        except Exception as e:
            logger.error(f"Ошибка в задаче {self.task_id}: {e}")
        finally:
            # Останавливаем парсер
            if self.parser:
                await self.parser.stop()

            # Обновляем статус
            state_manager.update_status(self.task_id, "stopped")
            logger.info(f"Задача {self.task_id} завершена")

    def run(self):
        """Запустить задачу в отдельном потоке"""
        # Создаем новый event loop для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.run_async())
        finally:
            loop.close()


def start_monitoring_task(
    task_id: str,
    user_id: int,
    mode: str,
    chats: List[str],
    filters_dict: dict,
    api_id: int,
    api_hash: str,
    notification_bot_token: str,
    notification_chat_id: int,
    parse_history_days: int
):
    """
    Запустить задачу мониторинга в фоновом потоке

    Args:
        task_id: ID задачи
        user_id: ID пользователя
        mode: "worker" или "employer"
        chats: список чатов для мониторинга
        filters_dict: словарь с фильтрами
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        notification_bot_token: токен бота для уведомлений
        notification_chat_id: ID чата для уведомлений
        parse_history_days: количество дней истории
    """
    task = MonitoringTask(
        task_id=task_id,
        user_id=user_id,
        mode=mode,
        chats=chats,
        filters_dict=filters_dict,
        api_id=api_id,
        api_hash=api_hash,
        notification_bot_token=notification_bot_token,
        notification_chat_id=notification_chat_id,
        parse_history_days=parse_history_days
    )

    # Запускаем в отдельном потоке
    thread = threading.Thread(target=task.run, daemon=True)
    thread.start()

    logger.info(f"Фоновая задача {task_id} запущена в потоке")
