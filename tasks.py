"""
Фоновые задачи мониторинга
"""
import asyncio
from datetime import datetime
from typing import List, Dict, Set, Optional
from loguru import logger

from config import config
from parser import TelegramParser
from message_extractor import MessageExtractor
from filters import ItemFilter
from geo_filter import geo_filter
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
        notification_chat_id: int,
        parse_history_days: int,
        session_path: str = None
    ):
        self.task_id = task_id
        self.user_id = user_id
        self.mode = mode
        self.api_id = api_id

        # Парсим чаты: поддерживаемый формат "@chat/topic_id#ГОРОД"
        #   @chat              — обычный чат, гео-фильтр по тексту
        #   @chat#МСК          — городской чат Москвы (весь), гео-фильтр пропускается
        #   @chat#СПБ          — городской чат СПб (весь),   гео-фильтр пропускается
        #   @chat/912#МСК      — конкретный топик + метка города
        #   @chat/912          — конкретный топик без метки, гео-фильтр по тексту
        #
        # chat_topic_filter:  chat → set разрешённых topic_id
        # chat_topic_city:    chat → {topic_id → city_tag}  (МСК/СПБ)
        # chat_city_override: chat → city_tag  (для чатов без топиков, @chat#МСК)
        self.chat_topic_filter: Dict[str, Set[int]] = {}
        self.chat_topic_city: Dict[str, Dict[int, str]] = {}
        self.chat_city_override: Dict[str, str] = {}
        parsed_chats = []
        for raw_chat in chats:
            # 1. Отделяем метку города (суффикс после последнего #)
            city_override = None
            if '#' in raw_chat:
                chat_part, city_tag = raw_chat.rsplit('#', 1)
                city_tag = city_tag.strip().upper()
                if city_tag in ('МСК', 'СПБ'):
                    city_override = city_tag
                else:
                    chat_part = raw_chat   # неизвестный тег — игнорируем
            else:
                chat_part = raw_chat

            # 2. Парсим топик: "@chat/912" → base_chat="@chat", topic=912
            if '/' in chat_part:
                parts = chat_part.rsplit('/', 1)
                try:
                    base_chat = parts[0]
                    topic_id = int(parts[1])
                    # Добавляем topic_id в множество разрешённых топиков
                    if base_chat not in self.chat_topic_filter:
                        self.chat_topic_filter[base_chat] = set()
                    self.chat_topic_filter[base_chat].add(topic_id)
                    # Сохраняем метку города для конкретного топика
                    if city_override:
                        if base_chat not in self.chat_topic_city:
                            self.chat_topic_city[base_chat] = {}
                        self.chat_topic_city[base_chat][topic_id] = city_override
                    if base_chat not in parsed_chats:
                        parsed_chats.append(base_chat)
                except ValueError:
                    if chat_part not in parsed_chats:
                        parsed_chats.append(chat_part)
                    if city_override:
                        self.chat_city_override[chat_part] = city_override
            else:
                if chat_part not in parsed_chats:
                    parsed_chats.append(chat_part)
                if city_override:
                    self.chat_city_override[chat_part] = city_override

        self.chats = parsed_chats
        self.api_hash = api_hash
        self.notification_chat_id = notification_chat_id
        self.parse_history_days = parse_history_days
        self.session_path = session_path or config.SESSION_PATH

        # Создаем фильтр
        self.item_filter = ItemFilter(
            date_from=filters_dict['date_from'],
            date_to=filters_dict['date_to'],
            min_price=filters_dict['min_price'],
            max_price=filters_dict['max_price'],
            shk_filter=filters_dict['shk_filter']
        )
        self.city_filter = filters_dict.get('city_filter', 'ALL')

        # Сервисы
        self.db = DBService(db_path=config.DB_PATH)
        self.parser = None
        # Используем общий BOT_TOKEN из конфига для всех уведомлений
        self.notifier = TelegramNotifier(config.BOT_TOKEN, notification_chat_id)

        # Кэш топиков: {chat_username: {topic_id: topic_name}}
        self.topics_cache = {}

        # Дедупликация: трекинг обработанных сообщений по chat_id:msg_id
        self.processed_messages: Set[str] = set()
        # Последний обработанный message_id для каждого чата (ключ = числовой chat.id)
        self.last_seen_msg_id: Dict[int, int] = {}

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
            # Дедупликация по message_id + chat_id (защита от двойной обработки
            # одного сообщения real-time handler'ом И polling fallback'ом)
            msg_key = f"{message.chat.id}:{message.id}"
            if msg_key in self.processed_messages:
                return  # Уже обработано
            self.processed_messages.add(msg_key)

            # Ограничиваем рост set (polling проверяет только 5 последних сообщений,
            # поэтому старые msg_key безопасно удалять)
            if len(self.processed_messages) > 10000:
                self.processed_messages.clear()

            # Обновляем last_seen_msg_id для polling fallback (ключ = числовой chat.id)
            self.last_seen_msg_id[message.chat.id] = max(
                message.id, self.last_seen_msg_id.get(message.chat.id, 0)
            )

            # Фильтр по топику: если чат указан как "@chat/topic_id" — пропускаем
            # сообщения из других топиков этого форума.
            #
            # В Pyrogram 2.0.106 нет message_thread_id; для forum-сообщений используем:
            #   reply_to_top_message_id  — topic_id при ответе внутри топика
            #   reply_to_message_id      — topic_id при первом сообщении в топик
            # (одно из них всегда равно ID топика)
            allowed_topics: Optional[Set[int]] = self.chat_topic_filter.get(chat_name)
            actual_topic: Optional[int] = None
            if allowed_topics is not None:
                actual_topic = (
                    getattr(message, 'reply_to_top_message_id', None)
                    or getattr(message, 'reply_to_message_id', None)
                )
                if actual_topic not in allowed_topics:
                    logger.debug(
                        f"[TOPIC FILTER] Пропущено: {chat_name} топик={actual_topic}, "
                        f"разрешены={allowed_topics}"
                    )
                    return

            # Обновляем счетчик обработанных сообщений
            state_manager.update_stats(self.task_id, messages_scanned=1)

            # Извлекаем данные
            message_text = (message.text or "").replace('\x00', '')
            message_date = message.date

            extracted = MessageExtractor.extract(message_text, message_date)

            if not extracted:
                logger.debug(f"[FILTER] Сообщение из {chat_name} НЕ распознано (нет даты/цены/типа)")
                return

            # Проверяем тип (должен соответствовать режиму)
            if extracted['type'] != self.mode:
                logger.debug(f"[FILTER] Сообщение из {chat_name} пропущено: тип '{extracted['type']}' != режим '{self.mode}'")
                return

            # Гео-фильтр: исключаем сообщения чужого города.
            #
            # Приоритет:
            #   1. Метка города на конкретном топике (@chat/912#МСК)
            #   2. Метка города на всём чате (@chat#МСК)
            #   3. Гео-фильтр по тексту сообщения
            # Гео-фильтр: исключаем сообщения чужого города.
            #
            # Логика:
            #   Топик с тегом (#МСК / #СПБ) — город известен точно:
            #     • тег совпадает с city_filter → берём без текстового гео-фильтра
            #     • тег не совпадает            → пропускаем
            #   Топик без тега (общий, напр. 8984) — текстовый гео-фильтр обязателен
            #   Чат с тегом (@chat#МСК) — аналогично топику с тегом
            skip_geo = False

            if actual_topic is not None and chat_name in self.chat_topic_city:
                topic_city = self.chat_topic_city[chat_name].get(actual_topic)
                if topic_city:
                    # Топик имеет тег города → доверяем тегу
                    skip_geo = True
                    if self.city_filter != 'ALL' and topic_city != self.city_filter:
                        logger.debug(
                            f"[GEO] {chat_name} топик={actual_topic} помечен {topic_city}, "
                            f"задача — {self.city_filter}: пропускаем"
                        )
                        return
                    # topic_city == city_filter → берём, гео-фильтр по тексту не нужен

            if not skip_geo:
                chat_city = self.chat_city_override.get(chat_name)
                if chat_city:
                    skip_geo = True
                    if self.city_filter != 'ALL' and chat_city != self.city_filter:
                        logger.debug(
                            f"[GEO] Чат {chat_name} помечен как {chat_city}, "
                            f"задача — {self.city_filter}: пропускаем"
                        )
                        return

            if not skip_geo:
                # Топик/чат без тега — текстовый гео-фильтр обязателен
                if self.city_filter == 'МСК':
                    if not geo_filter.should_take_for_moscow(message_text):
                        return
                elif self.city_filter == 'СПБ':
                    if not geo_filter.should_take_for_spb(message_text):
                        return

            # Применяем фильтры
            if not self.item_filter.matches(extracted):
                logger.debug(f"[FILTER] Сообщение из {chat_name} НЕ прошло фильтры (дата/цена/ШК)")
                return

            # Формируем данные для сохранения
            author_username = message.from_user.username if message.from_user else None
            author_full_name = None
            author_id = None  # Telegram User ID (не меняется, в отличие от username)
            if message.from_user:
                author_full_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip()
                author_id = message.from_user.id  # Сохраняем Telegram User ID

            # Извлекаем topic_id и topic_name (для форумов/супергрупп).
            # actual_topic уже вычислен выше при проверке topic filter;
            # если топик-фильтра нет — определяем по reply_to атрибутам.
            topic_id = actual_topic
            if topic_id is None:
                # Чат без топик-фильтра — попробуем определить топик по reply_to
                rid_top = getattr(message, 'reply_to_top_message_id', None)
                rid = getattr(message, 'reply_to_message_id', None)
                candidate = rid_top or rid
                if candidate:
                    chat_topics = self.topics_cache.get(chat_name, {})
                    if candidate in chat_topics:
                        topic_id = candidate
                        logger.debug(f"Сообщение из топика (cache lookup): topic_id={topic_id}")
            topic_name = None

            if topic_id:

                # Получаем название топика из кэша (вместо извлечения из текста!)
                if chat_name in self.topics_cache:
                    topics_map = self.topics_cache[chat_name]
                    if topic_id in topics_map:
                        topic_name = topics_map[topic_id]
                        logger.debug(f"Название топика из кэша: {topic_name}")
                    else:
                        logger.warning(f"Топик с ID {topic_id} не найден в кэше для {chat_name}")
                else:
                    logger.debug(f"Кэш топиков для {chat_name} пуст")

                # Fallback: попытка извлечь из текста (если не нашли в кэше)
                if not topic_name and message_text:
                    import re
                    # Ищем паттерны топиков (в начале сообщения или в любом месте)
                    topic_patterns = [
                        # Паттерн с дефисом: "МСК - Ozon", "СПБ - WB"
                        r'(МСК|СПБ|СБП|Москва|Питер|Мск|Спб)\s*[-–—]\s*(ВБ|Озон|Ozon|WB|Wildberries|Яндекс\.?Маркет|ЯМ|Я\.Маркет)',
                        # Паттерн со стрелкой: "СПБ -> Я.Маркет", "МСК -> Озон"
                        r'(МСК|СПБ|СБП|Москва|Питер|Мск|Спб)\s*->\s*(ВБ|Озон|Ozon|WB|Wildberries|Яндекс\.?Маркет|ЯМ|Я\.Маркет)',
                        # Паттерн с хэштегом: "#мск_озон", "#спб_вб"
                        r'#(мск|спб|москва|питер)[\s_]*(вб|озон|ozon|wb|wildberries|ям)',
                    ]
                    for pattern in topic_patterns:
                        match = re.search(pattern, message_text, re.IGNORECASE)
                        if match:
                            topic_name = match.group(0).strip()
                            logger.debug(f"Fallback: извлечено название топика из текста: {topic_name}")
                            break

            # Формируем ссылку на сообщение
            message_link = f"https://t.me/{chat_name.lstrip('@')}/{message.id}"

            # ДВУХУРОВНЕВАЯ ДЕДУПЛИКАЦИЯ:

            # Уровень 1: Content hash (защита от копипасты)
            content_hash = Deduplicator.create_content_hash(
                author_username=author_username,
                price=extracted['price'],
                location=extracted.get('location'),
                message_text=message_text
            )

            # Уровень 2: Author-based (защита от кросс-постов)
            # Проверяем: автор + дата + цена (если автор меняет цену → новое уведомление!)
            if author_username:
                is_author_duplicate = await self.db.check_duplicate_by_author(
                    author_username=author_username,
                    work_date=extracted['date'],
                    price=extracted['price'],
                    task_id=self.task_id,
                    hours_window=24
                )

                if is_author_duplicate:
                    logger.debug(
                        f"Пропущен дубликат по автору: {author_username}, "
                        f"дата={extracted['date']}, цена={extracted['price']}"
                    )
                    state_manager.update_stats(self.task_id, messages_scanned=1)
                    return  # Пропускаем дубликат
                else:
                    logger.debug(
                        f"Новое объявление от автора: {author_username}, "
                        f"дата={extracted['date']}, цена={extracted['price']}"
                    )

            # Создаем объект для БД
            found_item = FoundItem(
                id=None,
                task_id=self.task_id,
                mode=self.mode,
                author_username=author_username,
                author_full_name=author_full_name,
                author_id=author_id,  # Telegram User ID (для проверки в ЧС)
                date=extracted['date'],
                price=extracted['price'],
                shk=extracted.get('shk'),
                location=extracted.get('location'),  # Старое поле (для обратной совместимости)
                city=None,
                metro_station=None,
                district=None,
                message_text=message_text,
                message_link=message_link,
                chat_name=chat_name,
                message_date=message_date.isoformat(),
                found_at=datetime.utcnow().isoformat(),
                notified=False,
                content_hash=content_hash,
                topic_id=topic_id,  # ID топика (для форумов)
                topic_name=topic_name  # Название топика (МСК - Ozon, СПБ - WB и т.д.)
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
                    'location': extracted.get('location'),  # Старое поле (для обратной совместимости)
                    'city': None,
                    'metro_station': None,
                    'district': None,
                    'topic_name': topic_name,  # Название топика (МСК - Ozon и т.д.)
                    'author_username': author_username,
                    'author_full_name': author_full_name,
                    'author_id': author_id,  # Telegram User ID (для проверки в ЧС)
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
        """
        Асинхронная задача мониторинга.
        Запускается через asyncio.create_task() на event loop FastAPI.
        """
        try:
            # Инициализируем БД
            await self.db.init_db()

            # Создаем парсер (сессия из запроса или из конфига)
            self.parser = TelegramParser(
                api_id=self.api_id,
                api_hash=self.api_hash,
                session_name=self.session_path
            )

            # Запускаем клиент
            await self.parser.start()

            # Обновляем статус
            state_manager.update_status(self.task_id, "running")

            # Загружаем топики для каждого чата (если это форум)
            logger.info(f"Загружаем список топиков для чатов...")
            for chat in self.chats:
                topics = await self.parser.get_forum_topics(chat)
                if topics:
                    self.topics_cache[chat] = topics
                    logger.info(f"Загружено {len(topics)} топиков для {chat}")
                else:
                    logger.debug(f"Чат {chat} не является форумом или топики недоступны")

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
                await self.parser.setup_realtime_handler(
                    chat_usernames=self.chats,
                    handler=self.process_message
                )

                # Ждем сигнала остановки (с polling fallback)
                await self.parser.run_until_stopped(
                    self.stop_event,
                    chat_usernames=self.chats,
                    last_seen_msg_id=self.last_seen_msg_id,
                    message_handler=self.process_message
                )

        except asyncio.CancelledError:
            logger.info(f"Задача {self.task_id} отменена (CancelledError)")
        except Exception as e:
            if "AUTH_KEY_UNREGISTERED" in str(e) or "AUTH_KEY_INVALID" in str(e):
                logger.error(f"Сессия мониторинга аннулирована Telegram для задачи {self.task_id}")
                state_manager.update_status(self.task_id, "auth_error")
                try:
                    await self.notifier.send_text_message(
                        "⚠️ <b>Сессия авторизации не найдена</b>\n\n"
                        "Telegram аннулировал сессию мониторинга.\n"
                        "Пожалуйста, авторизуйтесь заново через меню \"👤 Мой аккаунт\"."
                    )
                except Exception as notify_err:
                    logger.error(f"Не удалось отправить уведомление об ошибке авторизации: {notify_err}")
            else:
                logger.error(f"Ошибка в задаче {self.task_id}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                state_manager.update_status(self.task_id, "failed")
        finally:
            # Останавливаем парсер
            if self.parser:
                await self.parser.stop()

            # Обновляем статус: "stopped" только если не было специфической ошибки
            current = state_manager.get_task(self.task_id)
            if current and current.get("status") not in ("auth_error", "failed"):
                state_manager.update_status(self.task_id, "stopped")
            logger.info(f"Задача {self.task_id} завершена")


def start_monitoring_task(
    task_id: str,
    user_id: int,
    mode: str,
    chats: List[str],
    filters_dict: dict,
    api_id: int,
    api_hash: str,
    notification_chat_id: int,
    parse_history_days: int,
    session_path: str = None
):
    """
    Запустить задачу мониторинга как asyncio.Task на event loop FastAPI.

    Вызывать из async контекста (FastAPI endpoint).
    Pyrogram работает на том же event loop, что и FastAPI — это необходимо
    для корректного получения real-time updates через MTProto.

    Args:
        task_id: ID задачи
        user_id: ID пользователя
        mode: "worker" или "employer"
        chats: список чатов для мониторинга
        filters_dict: словарь с фильтрами
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        notification_chat_id: ID чата для уведомлений (общий бот из config.BOT_TOKEN)
        parse_history_days: количество дней истории
        session_path: путь к Pyrogram сессии (из запроса ParserHub)
    """
    task = MonitoringTask(
        task_id=task_id,
        user_id=user_id,
        mode=mode,
        chats=chats,
        filters_dict=filters_dict,
        api_id=api_id,
        api_hash=api_hash,
        notification_chat_id=notification_chat_id,
        parse_history_days=parse_history_days,
        session_path=session_path
    )

    # Запускаем как asyncio Task на текущем event loop (FastAPI/uvicorn)
    asyncio_task = asyncio.create_task(task.run_async())
    state_manager.set_asyncio_task(task_id, asyncio_task)

    logger.info(f"Фоновая задача {task_id} запущена как asyncio.Task на event loop FastAPI")
