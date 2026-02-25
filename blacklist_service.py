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

    async def _scan_chats(
        self,
        client: Client,
        blacklist_chats: List[dict],
        time_limit: datetime,
        *,
        username: Optional[str] = None,
        user_id: Optional[int] = None,
        fio_words: Optional[List[str]] = None,
        match_type: str,
        total_messages_checked: int = 0,
    ) -> Dict:
        """
        Один проход по всем чатам ЧС с одним критерием поиска.

        Args:
            match_type: "username" | "user_id" | "fio"  — для метки в результате
        Returns:
            {"found": True, ...} или {"found": False, "messages_checked": N, "chats_checked": [...]}
        """
        chats_checked = []

        for chat_entry in blacklist_chats:
            chat_username = chat_entry["chat_username"]
            topic_id = chat_entry.get("topic_id")
            topic_name = chat_entry.get("topic_name")

            # Старый формат "@chat/topic_id" → разбираем
            if '/' in chat_username:
                parts = chat_username.rsplit('/', 1)
                try:
                    if topic_id is None:
                        topic_id = int(parts[1])
                    chat_username = parts[0]
                except ValueError:
                    pass

            try:
                topic_info = f" (топик: {topic_name or topic_id})" if topic_id else ""
                chat = await client.get_chat(chat_username)
                chat_id_tg = chat.id
                chats_checked.append(chat_username)

                if topic_id:
                    raw_messages = await self._get_topic_messages(client, chat_id_tg, topic_id, time_limit)
                    for raw_msg in raw_messages:
                        try:
                            total_messages_checked += 1
                            text = getattr(raw_msg, 'message', None)
                            if not text:
                                continue

                            if self._matches(text, username=username, user_id=user_id, fio_words=fio_words):
                                logger.info(f"Найден в ЧС {match_type}: в чате {chat_username}{topic_info}")
                                return self._build_found_result_raw(raw_msg, text, match_type,
                                                                     username or user_id or " ".join(fio_words or []),
                                                                     chat_username, topic_id)
                            if total_messages_checked % 500 == 0:
                                logger.debug(f"[{match_type}] Проверено {total_messages_checked} сообщений...")
                        except Exception as e:
                            logger.error(f"Ошибка raw сообщения: {e}")
                else:
                    async for message in client.get_chat_history(chat_id_tg):
                        try:
                            if message.date < time_limit:
                                break
                            total_messages_checked += 1
                            text = message.text or message.caption
                            if not text:
                                continue

                            if self._matches(text, username=username, user_id=user_id, fio_words=fio_words):
                                logger.info(f"Найден в ЧС {match_type}: в чате {chat_username}")
                                return self._build_found_result(message, text, match_type,
                                                                username or user_id or " ".join(fio_words or []),
                                                                chat_username)
                            if total_messages_checked % 500 == 0:
                                logger.debug(f"[{match_type}] Проверено {total_messages_checked} сообщений...")
                        except FloodWait as e:
                            logger.warning(f"FloodWait: ждём {e.value} сек")
                            await asyncio.sleep(e.value)
                        except Exception as e:
                            logger.error(f"Ошибка сообщения: {e}")

            except Exception as e:
                logger.error(f"Ошибка доступа к чату {chat_username}: {e}")
                continue

        return {
            "found": False,
            "messages_checked": total_messages_checked,
            "chats_checked": chats_checked,
        }

    def _matches(
        self,
        text: str,
        *,
        username: Optional[str] = None,
        user_id: Optional[int] = None,
        fio_words: Optional[List[str]] = None,
    ) -> bool:
        """Проверяет, удовлетворяет ли текст сообщения критерию поиска."""
        text_lower = text.lower()

        if username:
            return username.lower() in text_lower

        if user_id:
            found_ids = [int(m) for m in self.ID_PATTERN.findall(text)]
            return user_id in found_ids

        if fio_words:
            return all(word.lower() in text_lower for word in fio_words)

        return False

    async def search_in_blacklist(
        self,
        username: Optional[str] = None,
        fio: Optional[str] = None,
        days: int = 365,
        session_name: Optional[str] = None,
        # Оставляем для совместимости с вызовами из check_blacklist_by_item
        user_id: Optional[int] = None,
    ) -> Dict:
        """
        Трёхступенчатый поиск в черном списке:
          1. По @username (строковое совпадение)
          2. По User ID (Pyrogram резолвит username → user_id)
          3. По ФИО (все слова присутствуют в тексте)

        Каждая ступень — полный проход по всем чатам ЧС.
        Останавливается при первом совпадении.
        """
        if not user_id and not username and not fio:
            return {"found": False, "error": "Необходимо указать username или ФИО для поиска"}

        if username and not username.startswith("@"):
            username = f"@{username}"

        blacklist_chats = await self.db.get_blacklist_chats(active_only=True)
        if not blacklist_chats:
            return {"found": False, "error": "Нет активных чатов черного списка."}

        effective_session = session_name or self.session_name
        logger.info(f"Поиск в ЧС: username={username}, fio={fio}, чатов: {len(blacklist_chats)}")

        client = Client(name=effective_session, api_id=self.api_id, api_hash=self.api_hash)
        steps_done = []
        total_checked = 0
        all_chats_checked = []

        try:
            await client.start()
            time_limit = datetime.now() - timedelta(days=days)

            # === ШАГ 1: поиск по username ===
            if username:
                steps_done.append("по никнейму")
                logger.info(f"ЧС шаг 1: поиск по username={username}")
                result = await self._scan_chats(
                    client, blacklist_chats, time_limit,
                    username=username, match_type="username",
                    total_messages_checked=total_checked,
                )
                total_checked = result.get("messages_checked", total_checked)
                all_chats_checked = result.get("chats_checked", [])
                if result["found"]:
                    return result

            # === ШАГ 2: резолвим user_id и ищем по нему ===
            if username:
                resolved_user_id = None
                try:
                    user_obj = await client.get_users(username.lstrip("@"))
                    resolved_user_id = user_obj.id
                    logger.info(f"ЧС шаг 2: {username} → user_id={resolved_user_id}")
                except Exception as e:
                    logger.warning(f"Не удалось резолвить {username} → user_id: {e}")

                if resolved_user_id:
                    steps_done.append("по User ID")
                    result = await self._scan_chats(
                        client, blacklist_chats, time_limit,
                        user_id=resolved_user_id, match_type="user_id",
                        total_messages_checked=total_checked,
                    )
                    total_checked = result.get("messages_checked", total_checked)
                    if result["found"]:
                        return result

            # === ШАГ 3: поиск по ФИО ===
            if fio:
                fio_words = [w for w in fio.strip().split() if len(w) >= 2]
                if fio_words:
                    steps_done.append("по ФИО")
                    logger.info(f"ЧС шаг 3: поиск по ФИО={fio_words}")
                    result = await self._scan_chats(
                        client, blacklist_chats, time_limit,
                        fio_words=fio_words, match_type="fio",
                        total_messages_checked=total_checked,
                    )
                    total_checked = result.get("messages_checked", total_checked)
                    if result["found"]:
                        return result

            logger.info(f"В ЧС не найден (проверено {total_checked} сообщений, шаги: {steps_done})")
            return {
                "found": False,
                "username": username,
                "messages_checked": total_checked,
                "chats_checked": all_chats_checked,
                "steps_done": steps_done,
                "message": "В черном списке не найден",
            }

        except Exception as e:
            logger.error(f"Ошибка поиска в ЧС: {e}")
            return {"found": False, "error": str(e)}

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
            "message_text": text
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
            "message_text": text
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
