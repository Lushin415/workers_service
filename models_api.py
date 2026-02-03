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


class StartMonitoringRequest(BaseModel):
    """Запрос на запуск мониторинга"""
    user_id: int
    mode: str = Field(..., pattern="^(worker|employer)$")
    chats: List[str]
    filters: Filters
    api_id: int
    api_hash: str
    notification_bot_token: str
    notification_chat_id: int
    parse_history_days: int = 14


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
    """Ответ на проверку черного списка (заглушка)"""
    item_id: int
    check_status: str
    result: dict
