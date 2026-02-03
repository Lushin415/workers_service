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
    notification_bot_token: str
    notification_chat_id: int
    status: str  # "pending", "running", "stopped"
    created_at: str
    stopped_at: Optional[str] = None


@dataclass
class FoundItem:
    """Модель найденного объявления"""
    id: Optional[int]
    task_id: str
    mode: str
    author_username: Optional[str]
    author_full_name: Optional[str]
    date: str
    price: int
    shk: Optional[str]
    location: Optional[str]
    message_text: str
    message_link: str
    chat_name: str
    message_date: str
    found_at: str
    notified: bool = False
    content_hash: Optional[str] = None  # Хеш для умной дедупликации
