"""
Конфигурация приложения из .env
"""
import os
from dotenv import load_dotenv

# Загружаем .env файл
load_dotenv()


class Config:
    """Настройки приложения"""

    # Telegram API
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")

    # Telegram Bot
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

    # User Settings
    USER_ID: int = int(os.getenv("USER_ID", "0"))
    CHATS: list = os.getenv("CHATS", "").split(",")

    # Server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8002"))

    # Paths
    DB_PATH: str = os.getenv("DB_PATH", "workers.db")
    LOG_PATH: str = os.getenv("LOG_PATH", "workers_service.log")
    SESSION_PATH: str = os.getenv("SESSION_PATH", "workers_session")

    # Parsing
    PARSE_HISTORY_DAYS: int = int(os.getenv("PARSE_HISTORY_DAYS", "3"))

    # Blacklist (Черный список) - поиск в реальном времени
    BLACKLIST_CHAT: str = os.getenv("BLACKLIST_CHAT", "@Blacklist_pvz")
    # Отдельная сессия для поиска в ЧС (чтобы не конфликтовать с основным парсером)
    BLACKLIST_SESSION_PATH: str = os.getenv("BLACKLIST_SESSION_PATH", "blacklist_session")


config = Config()
