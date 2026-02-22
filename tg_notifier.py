"""
Отправка уведомлений в Telegram через Bot API
"""
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from loguru import logger
from typing import Dict


class TelegramNotifier:
    """Класс для отправки уведомлений в Telegram"""

    def __init__(self, bot_token: str, chat_id: int):
        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id

    async def send_notification(self, item_data: Dict, item_id: int, mode: str):
        """
        Отправить уведомление о найденном объявлении

        Args:
            item_data: данные объявления
            item_id: ID записи в БД
            mode: "worker" или "employer"
        """
        # Форматирование сообщения
        if mode == "worker":
            header = "👷 Новый работник!"
        else:
            header = "🏢 Новая вакансия!"

        message_parts = [header, ""]

        # Основная информация
        message_parts.append(f"📅 Дата: {item_data.get('date', 'не указана')}")

        price_label = "💰 Цена:" if mode == "worker" else "💰 Оплата:"
        price_val = item_data.get('price')
        message_parts.append(f"{price_label} {price_val if price_val is not None else 'не указана'} руб/смену")

        if item_data.get('shk'):
            message_parts.append(f"📦 ШК: {item_data['shk']}")

        # Информация о топике (для форумов/супергрупп) - СРАЗУ после цены!
        if item_data.get('topic_name'):
            message_parts.append(f"🏷️ Топик: {item_data['topic_name']}")

        # Структурированная локация (город, метро, район)
        location_parts = []

        if item_data.get('city'):
            location_parts.append(f"🏙️ Город: {item_data['city']}")

        if item_data.get('metro_station'):
            location_parts.append(f"🚇 Метро: {item_data['metro_station']}")

        if item_data.get('district'):
            location_parts.append(f"📍 Район: {item_data['district']}")

        # Если есть старое поле location (для обратной совместимости)
        if not location_parts and item_data.get('location'):
            location_parts.append(f"📍 Локация: {item_data['location']}")

        message_parts.extend(location_parts)

        # Информация об авторе
        author_info = []
        if item_data.get('author_username'):
            author_info.append(f"@{item_data['author_username'].lstrip('@')}")
        if item_data.get('author_full_name'):
            author_info.append(f"({item_data['author_full_name']})")

        if author_info:
            message_parts.append(f"👤 {' '.join(author_info)}")

        # Информация о чате
        # message_parts.append(f"💬 Чат: {item_data.get('chat_name', 'не указан')}")

        # Ссылка на сообщение
        # if item_data.get('message_link'):
        #     message_parts.append(f"🔗 {item_data['message_link']}")

        # Полный текст сообщения
        message_parts.append("")
        message_parts.append("📝 Полный текст:")
        message_parts.append(f'"{item_data.get("message_text", "")}"')

        message_text = "\n".join(message_parts)

        # Кнопка "Связаться":
        # 1. Есть username → t.me/{username} (открывает личку)
        # 2. Нет username, но есть author_id → tg://user?id= (открывает профиль)
        # 3. Нет ничего → кнопку не показываем
        # Ссылку на мониторируемый чат (message_link) не используем намеренно.
        contact_button = None
        author_username = item_data.get('author_username')
        author_id = item_data.get('author_id')

        if author_username:
            clean_username = author_username.lstrip('@')
            contact_button = InlineKeyboardButton(
                "💬 Связаться",
                url=f"https://t.me/{clean_username}"
            )
        elif author_id is not None:
            contact_button = InlineKeyboardButton(
                "💬 Связаться",
                url=f"tg://user?id={author_id}"
            )

        # Кнопки — одинаковые для обоих режимов (worker и employer)
        buttons = [
            [
                InlineKeyboardButton(
                    "Проверить в ЧС",
                    callback_data=f"check_blacklist:{item_id}"
                )
            ],
        ]
        if contact_button:
            buttons.append([contact_button])
        buttons.append([
            InlineKeyboardButton(
                "Игнорировать",
                callback_data=f"ignore:{item_id}"
            )
        ])

        keyboard = InlineKeyboardMarkup(buttons)

        # Отправка
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message_text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            logger.info(f"Уведомление отправлено для объявления ID {item_id}")
            return True
        except TelegramError as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            return False

    async def send_text_message(self, text: str) -> bool:
        """Отправить произвольное текстовое сообщение пользователю"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML"
            )
            return True
        except TelegramError as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False
