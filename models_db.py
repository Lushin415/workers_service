"""
Модели для базы данных SQLite
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Task:
    """Модель задачи мониторинга"""
    task_id: str
    user_id: int
    mode: str  # "worker" или "employer"
    chats: str  # JSON строка
    filters: str  # JSON строка
    notification_chat_id: int  # Chat ID для уведомлений (общий бот из config.BOT_TOKEN)
    status: str  # "pending", "running", "stopped"
    created_at: str
    stopped_at: Optional[str] = None
    session_path: Optional[str] = None  # Путь к Pyrogram сессии парсера
    blacklist_session_path: Optional[str] = None  # Путь к сессии для ЧС


@dataclass
class FoundItem:
    """Модель найденного объявления"""
    id: Optional[int]
    task_id: str
    mode: str
    author_username: Optional[str]
    author_full_name: Optional[str]
    author_id: Optional[int]  # Telegram User ID (не меняется, в отличие от username)
    date: str
    price: int
    shk: Optional[str]
    location: Optional[str]  # Общее поле локации (для обратной совместимости)
    city: Optional[str]  # Город (Москва, СПБ)
    metro_station: Optional[str]  # Станция метро
    district: Optional[str]  # Район (ЮВАО, ЮАО)
    message_text: str
    message_link: str
    chat_name: str
    message_date: str
    found_at: str
    notified: bool = False
    content_hash: Optional[str] = None  # Хеш для умной дедупликации
    topic_id: Optional[int] = None  # ID топика (для форумов/супергрупп)
    topic_name: Optional[str] = None  # Название топика (например, "МСК - Ozon")


@dataclass
class BlacklistRecord:
    """Модель записи в черном списке"""
    id: Optional[int]
    telegram_user_id: int  # Telegram User ID (основной идентификатор для поиска)
    username: Optional[str]  # @username (может меняться)
    full_name: Optional[str]  # ФИО из сообщения в ЧС
    phone: Optional[str]  # Телефон (если указан)
    role: str  # "worker" или "employer"
    message_link: str  # Ссылка на сообщение в чате ЧС
    message_id: int  # ID сообщения в чате ЧС
    parsed_at: str  # Дата парсинга
