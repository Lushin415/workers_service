"""
Pydantic модели для REST API
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime


class Filters(BaseModel):
    """Фильтры для поиска объявлений"""
    date_from: date
    date_to: date
    min_price: int = 0
    max_price: int = 999_999_999
    shk_filter: str = "любое"  # "любое", конкретное значение или "мало"/"много"
    city_filter: str = "ALL"  # "МСК", "СПБ", "ALL"


class StartMonitoringRequest(BaseModel):
    """Запрос на запуск мониторинга"""
    user_id: int
    mode: str = Field(..., pattern="^(worker|employer)$")
    chats: List[str]
    filters: Filters
    api_id: Optional[int] = None  # Опционально, fallback на config
    api_hash: Optional[str] = None  # Опционально, fallback на config
    notification_chat_id: int  # Chat ID для уведомлений (общий бот из config.BOT_TOKEN)
    parse_history_days: int = 14
    session_path: Optional[str] = None  # Путь к Pyrogram сессии парсера (например /shared/sessions/338908929_parser)
    blacklist_session_path: Optional[str] = None  # Путь к сессии для ЧС (например /shared/sessions/338908929_blacklist)


class TaskStatusResponse(BaseModel):
    """Статус задачи"""
    task_id: str
    status: str
    mode: str
    stats: dict


class StartMonitoringResponse(BaseModel):
    """Ответ на запуск мониторинга"""
    task_id: str
    status: str
    message: str
    started_at: datetime


class StopMonitoringResponse(BaseModel):
    """Ответ на остановку мониторинга"""
    task_id: str
    status: str
    message: str


class FoundItemResponse(BaseModel):
    """Найденное объявление"""
    id: int
    author_username: Optional[str]
    author_full_name: Optional[str]
    date: str
    price: int
    shk: Optional[str]
    location: Optional[str]
    message_link: str
    found_at: str


class FoundItemsListResponse(BaseModel):
    """Список найденных объявлений"""
    task_id: str
    mode: str
    total: int
    items: List[FoundItemResponse]


class CheckBlacklistResponse(BaseModel):
    """Ответ на проверку черного списка (заглушка для check-blacklist endpoint)"""
    item_id: int
    check_status: str
    result: dict


# ========== Модели для Blacklist API ==========

class BlacklistCheckRequest(BaseModel):
    """Запрос на проверку пользователя в черном списке"""
    user_id: int


class BlacklistCheckResponse(BaseModel):
    """Ответ на проверку пользователя в черном списке"""
    is_blacklisted: bool
    user_id: int
    username: Optional[str] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    message_link: Optional[str] = None
    message: Optional[str] = None
    cache_updated: Optional[str] = None


class BlacklistRefreshResponse(BaseModel):
    """Ответ на обновление кеша черного списка"""
    status: str
    records_updated: int
    message: str


class BlacklistStatsResponse(BaseModel):
    """Статистика кеша черного списка"""
    blacklist_chat: str
    total_records: int
    workers: int
    employers: int
    last_cache_update: Optional[str] = None
    service_last_refresh: Optional[str] = None


class BlacklistChatInfo(BaseModel):
    """Информация о чате черного списка"""
    chat_username: str
    chat_title: Optional[str] = None
    added_at: str
    is_active: bool
    topic_id: Optional[int] = None
    topic_name: Optional[str] = None


class BlacklistChatsListResponse(BaseModel):
    """Список чатов черного списка"""
    chats: List[BlacklistChatInfo]
    total: int
    active: int


class BlacklistChatTopicInfo(BaseModel):
    """Информация о топике форума"""
    id: int
    name: str


class BlacklistChatTopicsResponse(BaseModel):
    """Ответ на запрос топиков чата"""
    is_forum: bool
    chat_title: str
    topics: List[BlacklistChatTopicInfo]
