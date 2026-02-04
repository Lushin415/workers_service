"""
Парсинг Telegram чатов через Pyrogram (MTProto API)
"""
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.raw.functions.channels import GetForumTopics
from pyrogram.raw.types import InputPeerChannel
from datetime import datetime, timedelta
from typing import List, Callable, Dict
from loguru import logger


class TelegramParser:
    """Класс для парсинга Telegram чатов"""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_name: str = "workers_parser"
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.client: Client = None
        self.message_handler: Callable = None

    async def start(self):
        """Запустить клиент Pyrogram"""
        self.client = Client(
            name=self.session_name,
            api_id=self.api_id,
            api_hash=self.api_hash,
            workdir="."
        )

        await self.client.start()
        logger.info("Pyrogram клиент запущен")

    async def stop(self):
        """Остановить клиент"""
        if self.client:
            await self.client.stop()
            logger.info("Pyrogram клиент остановлен")

    async def get_forum_topics(self, chat_username: str) -> Dict[int, str]:
        """
        Получить список топиков форума через GetForumTopics (raw API)

        Args:
            chat_username: имя чата (например, @pvz_zamena)

        Returns:
            Словарь {topic_id: topic_name}
        """
        if not self.client:
            logger.error("Клиент не запущен")
            return {}

        try:
            # Получаем информацию о чате
            chat = await self.client.get_chat(chat_username)
            chat_id = chat.id

            logger.info(f"🔍 Получение топиков в {chat_username}")
            logger.info(f"   Chat ID: {chat_id}")
            logger.info(f"   Chat type: {chat.type}")
            logger.info(f"   Title: {chat.title}")

            topics_map = {}

            # Получаем peer (должен быть InputPeerChannel для supergroup/channel)
            peer = await self.client.resolve_peer(chat_id)

            # Проверяем, что это channel/supergroup
            if not isinstance(peer, InputPeerChannel):
                logger.error(f"❌ Чат {chat_username} не является channel/supergroup (тип: {type(peer).__name__})")
                return {}

            logger.info(f"   ✅ Peer type: {type(peer).__name__}")

            # Вызываем raw API: GetForumTopics
            result = await self.client.invoke(
                GetForumTopics(
                    channel=peer,
                    offset_date=0,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                )
            )

            # Извлекаем топики из результата
            logger.info(f"   Result type: {type(result).__name__}")

            if hasattr(result, 'topics'):
                logger.info(f"   📋 Найдено {len(result.topics)} топиков")

                for topic in result.topics:
                    topic_id = topic.id
                    topic_title = topic.title
                    topics_map[topic_id] = topic_title
                    logger.info(f"   ✅ Топик: ID={topic_id}, Название='{topic_title}'")

                logger.info(f"📊 Успешно загружено {len(topics_map)} топиков из {chat_username}")
                return topics_map
            else:
                logger.warning(f"⚠️  Result не содержит 'topics': {type(result)}")
                return {}

        except Exception as e:
            # CHANNEL_FORUM_MISSING - это нормально для обычных чатов (не форумов)
            if "CHANNEL_FORUM_MISSING" in str(e):
                logger.info(f"ℹ️  Чат {chat_username} не является форумом (топиков нет)")
                return {}
            else:
                logger.error(f"❌ Ошибка при получении топиков из {chat_username}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return {}

    async def parse_history(
        self,
        chat_username: str,
        days: int,
        handler: Callable
    ) -> int:
        """
        Парсинг истории чата

        Args:
            chat_username: имя чата (например, @pvz_workers)
            days: количество дней истории для парсинга
            handler: функция обработчик для каждого сообщения

        Returns:
            Количество обработанных сообщений
        """
        if not self.client:
            logger.error("Клиент не запущен")
            return 0

        try:
            # Получаем информацию о чате
            chat = await self.client.get_chat(chat_username)
            chat_id = chat.id
            logger.info(f"Начинаем парсинг истории чата {chat_username} за {days} дней")

            # Определяем временную границу
            time_limit = datetime.now() - timedelta(days=days)

            messages_count = 0

            # Итерируемся по истории сообщений
            async for message in self.client.get_chat_history(chat_id):
                # Проверяем дату
                if message.date < time_limit:
                    break

                # Пропускаем сервисные сообщения
                if not message.text:
                    continue

                # Вызываем обработчик
                await handler(message, chat_username)
                messages_count += 1

            logger.info(f"Обработано {messages_count} сообщений из истории {chat_username}")
            return messages_count

        except Exception as e:
            logger.error(f"Ошибка парсинга истории чата {chat_username}: {e}")
            return 0

    def setup_realtime_handler(
        self,
        chat_usernames: List[str],
        handler: Callable
    ):
        """
        Настроить обработчик для новых сообщений (real-time)

        Args:
            chat_usernames: список имен чатов для мониторинга
            handler: функция обработчик для новых сообщений
        """
        if not self.client:
            logger.error("Клиент не запущен")
            return

        # Создаем фильтр для указанных чатов
        chat_filter = filters.chat(chat_usernames)

        @self.client.on_message(chat_filter)
        async def message_handler(client, message: Message):
            # Пропускаем сервисные сообщения
            if not message.text:
                return

            # Определяем имя чата
            chat_username = None
            if message.chat.username:
                chat_username = f"@{message.chat.username}"
            else:
                chat_username = message.chat.title or str(message.chat.id)

            # Вызываем обработчик
            await handler(message, chat_username)

        logger.info(f"Настроен real-time мониторинг чатов: {', '.join(chat_usernames)}")

    async def run_until_stopped(self, stop_event):
        """
        Запустить клиент и ждать до остановки

        Args:
            stop_event: threading.Event для остановки
        """
        # Ждем сигнала остановки
        while not stop_event.is_set():
            await asyncio.sleep(1)

        logger.info("Получен сигнал остановки парсера")
