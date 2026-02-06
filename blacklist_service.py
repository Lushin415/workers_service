"""
Сервис для поиска в черном списке

Поиск происходит в реальном времени при запросе (без кеширования).
Поддерживает несколько чатов ЧС (список хранится в БД).
"""
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from loguru import logger

from pyrogram import Client
from pyrogram.raw.functions.messages import GetReplies
from pyrogram.raw.types import InputPeerChannel
from pyrogram.errors import FloodWait
import asyncio

from db_service import DBService


class BlacklistService:
    """Сервис для поиска в черном списке"""

    # Regex паттерны для поиска в сообщениях ЧС
    ID_PATTERN = re.compile(r'ID[:\s]*(\d+)', re.IGNORECASE)
    USERNAME_PATTERN = re.compile(r'(?:Ник[:\s]*)?(@[\w]+)', re.IGNORECASE)

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_name: str,
        db_service: DBService
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.db = db_service

    async def _get_topic_messages(
        self,
        client: Client,
        chat_id: int,
        topic_id: int,
        time_limit: datetime
    ) -> List:
        """
        Получить сообщения из конкретного топика через raw API GetReplies

        Args:
            client: Pyrogram клиент
            chat_id: ID чата
            topic_id: ID топика (корневого сообщения)
            time_limit: временная граница (сообщения старше игнорируются)

        Returns:
            Список raw сообщений
        """
        messages = []
        offset_id = 0

        peer = await client.resolve_peer(chat_id)

        while True:
            try:
                result = await client.invoke(
                    GetReplies(
                        peer=peer,
                        msg_id=topic_id,
                        offset_id=offset_id,
                        offset_date=0,
                        add_offset=0,
                        limit=100,
                        max_id=0,
                        min_id=0,
                        hash=0
                    )
                )

                if not result.messages:
                    break

                for raw_msg in result.messages:
                    # raw_msg.date — Unix timestamp (int)
                    msg_date = datetime.fromtimestamp(raw_msg.date)
                    if msg_date < time_limit:
                        return messages

                    messages.append(raw_msg)

                # Пагинация: offset_id = ID последнего сообщения
                offset_id = result.messages[-1].id

                # Если получили меньше 100, значит достигли конца
                if len(result.messages) < 100:
                    break

            except FloodWait as e:
                logger.warning(f"FloodWait при получении сообщений топика: ждём {e.value} сек")
                await asyncio.sleep(e.value)

        return messages

    async def search_in_blacklist(
        self,
        user_id: Optional[int] = None,
        username: Optional[str] = None,
        days: int = 365,
        session_name: Optional[str] = None
    ) -> Dict:
        """
        Поиск в черном списке по User ID или username

        Парсит ВСЕ чаты ЧС из БД в реальном времени и ищет совпадение.
        Если у чата указан topic_id — ищет только в этом топике (raw API).
        Если topic_id=NULL — ищет по всей истории чата (get_chat_history).

        Args:
            user_id: Telegram User ID для поиска
            username: @username для поиска (с или без @)
            days: сколько дней истории проверять
            session_name: путь к сессии (опционально)

        Returns:
            Словарь с результатом поиска
        """
        if not user_id and not username:
            return {
                "found": False,
                "error": "Не указан ни user_id, ни username для поиска"
            }

        # Нормализуем username (добавляем @ если нет)
        if username and not username.startswith("@"):
            username = f"@{username}"

        # Получаем список активных чатов ЧС из БД (теперь List[dict])
        blacklist_chats = await self.db.get_blacklist_chats(active_only=True)

        if not blacklist_chats:
            return {
                "found": False,
                "error": "Нет активных чатов черного списка. Добавьте чаты через API."
            }

        # Используем переданную сессию или дефолтную из конструктора
        effective_session = session_name or self.session_name

        logger.info(f"Поиск в ЧС: user_id={user_id}, username={username}, чатов: {len(blacklist_chats)}, сессия: {effective_session}")

        # Создаём клиент для поиска
        client = Client(
            name=effective_session,
            api_id=self.api_id,
            api_hash=self.api_hash,
        )

        total_messages_checked = 0
        chats_checked = []

        try:
            await client.start()
            logger.debug("Pyrogram клиент для поиска в ЧС запущен")

            # Временная граница
            time_limit = datetime.now() - timedelta(days=days)

            # Ищем по всем чатам ЧС
            for chat_entry in blacklist_chats:
                chat_username = chat_entry["chat_username"]
                topic_id = chat_entry.get("topic_id")
                topic_name = chat_entry.get("topic_name")

                try:
                    topic_info = f" (топик: {topic_name})" if topic_name else ""
                    logger.debug(f"Проверяем чат: {chat_username}{topic_info}")

                    # Получаем чат
                    chat = await client.get_chat(chat_username)
                    chat_id = chat.id
                    chats_checked.append(chat_username)

                    if topic_id:
                        # Поиск ТОЛЬКО в конкретном топике (raw API)
                        logger.debug(f"Используем GetReplies для топика {topic_id} ({topic_name})")
                        raw_messages = await self._get_topic_messages(client, chat_id, topic_id, time_limit)

                        for raw_msg in raw_messages:
                            try:
                                total_messages_checked += 1

                                # raw message: текст в .message (не .text)
                                text = getattr(raw_msg, 'message', None)
                                if not text:
                                    continue

                                # Ищем совпадение по User ID
                                if user_id:
                                    id_match = self.ID_PATTERN.search(text)
                                    if id_match:
                                        found_id = int(id_match.group(1))
                                        if found_id == user_id:
                                            logger.info(f"Найден в ЧС по ID: {user_id} в чате {chat_username}{topic_info}")
                                            return self._build_found_result_raw(raw_msg, text, "user_id", user_id, chat_username, topic_id)

                                # Ищем совпадение по username
                                if username:
                                    username_lower = username.lower()
                                    if username_lower in text.lower():
                                        logger.info(f"Найден в ЧС по username: {username} в чате {chat_username}{topic_info}")
                                        return self._build_found_result_raw(raw_msg, text, "username", username, chat_username, topic_id)

                                if total_messages_checked % 500 == 0:
                                    logger.debug(f"Проверено {total_messages_checked} сообщений...")

                            except Exception as e:
                                logger.error(f"Ошибка при проверке raw сообщения: {e}")
                                continue
                    else:
                        # Поиск по ВСЕЙ истории чата (старый способ)
                        async for message in client.get_chat_history(chat_id):
                            try:
                                if message.date < time_limit:
                                    break

                                total_messages_checked += 1

                                if not message.text:
                                    continue

                                text = message.text

                                # Ищем совпадение по User ID
                                if user_id:
                                    id_match = self.ID_PATTERN.search(text)
                                    if id_match:
                                        found_id = int(id_match.group(1))
                                        if found_id == user_id:
                                            logger.info(f"Найден в ЧС по ID: {user_id} в чате {chat_username}")
                                            return self._build_found_result(message, text, "user_id", user_id, chat_username)

                                # Ищем совпадение по username
                                if username:
                                    username_lower = username.lower()
                                    if username_lower in text.lower():
                                        logger.info(f"Найден в ЧС по username: {username} в чате {chat_username}")
                                        return self._build_found_result(message, text, "username", username, chat_username)

                                if total_messages_checked % 500 == 0:
                                    logger.debug(f"Проверено {total_messages_checked} сообщений...")

                            except FloodWait as e:
                                logger.warning(f"FloodWait: ждём {e.value} секунд")
                                await asyncio.sleep(e.value)
                            except Exception as e:
                                logger.error(f"Ошибка при проверке сообщения: {e}")
                                continue

                except Exception as e:
                    logger.error(f"Ошибка доступа к чату {chat_username}: {e}")
                    continue

            # Не нашли ни в одном чате
            logger.info(f"В ЧС не найден (проверено {total_messages_checked} сообщений в {len(chats_checked)} чатах)")
            return {
                "found": False,
                "user_id": user_id,
                "username": username,
                "messages_checked": total_messages_checked,
                "chats_checked": chats_checked,
                "message": f"В черном списке не найден"
            }

        except Exception as e:
            logger.error(f"Ошибка поиска в ЧС: {e}")
            return {
                "found": False,
                "error": str(e)
            }

        finally:
            await client.stop()
            logger.debug("Pyrogram клиент для поиска в ЧС остановлен")

    def _build_found_result_raw(self, raw_msg, text: str, match_type: str, match_value, chat_username: str, topic_id: Optional[int] = None) -> Dict:
        """Формирует результат при нахождении в ЧС (raw API сообщение)"""
        chat_name = chat_username.lstrip("@")

        # Для форумов с топиками ссылка включает topic_id
        if topic_id:
            message_link = f"https://t.me/{chat_name}/{topic_id}/{raw_msg.id}"
        else:
            message_link = f"https://t.me/{chat_name}/{raw_msg.id}"

        extracted = self._extract_info(text)

        # raw_msg.date — Unix timestamp (int)
        msg_date = datetime.fromtimestamp(raw_msg.date)

        return {
            "found": True,
            "match_type": match_type,
            "match_value": match_value,
            "chat": chat_username,
            "message_link": message_link,
            "message_id": raw_msg.id,
            "message_date": msg_date.isoformat(),
            "extracted_info": extracted,
            "message_text": text[:500]
        }

    def _build_found_result(self, message, text: str, match_type: str, match_value, chat_username: str) -> Dict:
        """Формирует результат при нахождении в ЧС"""
        # Формируем ссылку на сообщение
        chat_name = chat_username.lstrip("@")
        message_link = f"https://t.me/{chat_name}/{message.id}"

        # Извлекаем дополнительные данные из сообщения
        extracted = self._extract_info(text)

        return {
            "found": True,
            "match_type": match_type,
            "match_value": match_value,
            "chat": chat_username,  # В каком чате найден
            "message_link": message_link,
            "message_id": message.id,
            "message_date": message.date.isoformat(),
            "extracted_info": extracted,
            "message_text": text[:500]  # Первые 500 символов
        }

    def _extract_info(self, text: str) -> Dict:
        """Извлекает информацию из сообщения ЧС"""
        info = {}

        # ID
        id_match = self.ID_PATTERN.search(text)
        if id_match:
            info["user_id"] = int(id_match.group(1))

        # Username
        username_match = re.search(r'Ник[:\s]*(@[\w]+)', text, re.IGNORECASE)
        if username_match:
            info["username"] = username_match.group(1)

        # ФИО
        fio_match = re.search(r'ФИО[:\s]*([А-ЯЁа-яё\s]+?)(?:\n|$)', text, re.IGNORECASE)
        if fio_match:
            info["full_name"] = fio_match.group(1).strip()

        # Телефон
        phone_match = re.search(r'Тел[:\s]*([\+\d\s\*\-]+)', text, re.IGNORECASE)
        if phone_match:
            info["phone"] = phone_match.group(1).strip()

        # Роль
        text_lower = text.lower()
        if "работодатель" in text_lower:
            info["role"] = "employer"
        elif "сотрудник" in text_lower or "работник" in text_lower:
            info["role"] = "worker"

        return info
