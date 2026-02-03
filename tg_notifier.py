"""
Отправка уведомлений в Telegram через Bot API
"""
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from loguru import logger
from typing import Dict, Optional


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
        message_parts.append(f"{price_label} {item_data.get('price', 'не указана')} руб/смену")

        if item_data.get('shk'):
            message_parts.append(f"📦 ШК: {item_data['shk']}")

        if item_data.get('location'):
            message_parts.append(f"📍 Локация: {item_data['location']}")

        # Информация об авторе
        author_info = []
        if item_data.get('author_username'):
            author_info.append(f"@{item_data['author_username']}")
        if item_data.get('author_full_name'):
            author_info.append(f"({item_data['author_full_name']})")

        if author_info:
            message_parts.append(f"👤 {' '.join(author_info)}")

        # Информация о чате
        message_parts.append(f"💬 Чат: {item_data.get('chat_name', 'не указан')}")

        # Ссылка на сообщение
        if item_data.get('message_link'):
            message_parts.append(f"🔗 {item_data['message_link']}")

        # Полный текст сообщения
        message_parts.append("")
        message_parts.append("📝 Полный текст:")
        message_parts.append(f'"{item_data.get("message_text", "")}"')

        message_text = "\n".join(message_parts)

        # Кнопки
        if mode == "worker":
            buttons = [
                [
                    InlineKeyboardButton(
                        "Отправить на проверку в ЧС",
                        callback_data=f"check_blacklist:{item_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Игнорировать",
                        callback_data=f"ignore:{item_id}"
                    )
                ]
            ]
        else:
            buttons = [
                [
                    InlineKeyboardButton(
                        "Связаться",
                        url=item_data.get('message_link', '#')
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Игнорировать",
                        callback_data=f"ignore:{item_id}"
                    )
                ]
            ]

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
