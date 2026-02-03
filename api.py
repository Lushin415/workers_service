"""
FastAPI REST API для Workers Service
"""
import json
import uuid
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger

from models_api import (
    StartMonitoringRequest,
    StartMonitoringResponse,
    TaskStatusResponse,
    StopMonitoringResponse,
    FoundItemsListResponse,
    FoundItemResponse,
    CheckBlacklistResponse
)
from models_db import Task
from db_service import DBService
from state_manager import state_manager
from tasks import start_monitoring_task


# Настройка логирования
logger.add("workers_service.log", rotation="10 MB", level="INFO")

# Создание FastAPI приложения
app = FastAPI(
    title="Workers Service",
    description="Микросервис для мониторинга Telegram чатов и поиска объявлений о работе на ПВЗ",
    version="1.0.0"
)

# Сервис БД
db_service = DBService()


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    await db_service.init_db()
    logger.info("Workers Service запущен на порту 8002")


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "Workers Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.post("/workers/start", response_model=StartMonitoringResponse)
async def start_monitoring(request: StartMonitoringRequest):
    """
    Запустить мониторинг Telegram чатов

    Создает задачу мониторинга и запускает её в фоновом потоке.
    """
    try:
        # Генерируем task_id
        task_id = str(uuid.uuid4())

        # Сохраняем задачу в БД
        task = Task(
            task_id=task_id,
            user_id=request.user_id,
            mode=request.mode,
            chats=json.dumps(request.chats),
            filters=json.dumps({
                'date_from': request.filters.date_from.isoformat(),
                'date_to': request.filters.date_to.isoformat(),
                'min_price': request.filters.min_price,
                'max_price': request.filters.max_price,
                'shk_filter': request.filters.shk_filter
            }),
            notification_bot_token=request.notification_bot_token,
            notification_chat_id=request.notification_chat_id,
            status='pending',
            created_at=datetime.utcnow().isoformat()
        )

        await db_service.create_task(task)

        # Запускаем фоновую задачу
        start_monitoring_task(
            task_id=task_id,
            user_id=request.user_id,
            mode=request.mode,
            chats=request.chats,
            filters_dict={
                'date_from': request.filters.date_from,
                'date_to': request.filters.date_to,
                'min_price': request.filters.min_price,
                'max_price': request.filters.max_price,
                'shk_filter': request.filters.shk_filter
            },
            api_id=request.api_id,
            api_hash=request.api_hash,
            notification_bot_token=request.notification_bot_token,
            notification_chat_id=request.notification_chat_id,
            parse_history_days=request.parse_history_days
        )

        return StartMonitoringResponse(
            task_id=task_id,
            status='pending',
            message='Мониторинг запущен',
            started_at=datetime.utcnow()
        )

    except Exception as e:
        logger.error(f"Ошибка запуска мониторинга: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workers/status/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """
    Получить статус задачи мониторинга

    Возвращает текущий статус задачи и статистику.
    """
    try:
        # Получаем задачу из state_manager
        task_state = state_manager.get_task(task_id)

        if not task_state:
            raise HTTPException(status_code=404, detail="Задача не найдена")

        return TaskStatusResponse(
            task_id=task_id,
            status=task_state['status'],
            mode=task_state['mode'],
            stats=task_state['stats']
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка получения статуса задачи {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workers/stop/{task_id}", response_model=StopMonitoringResponse)
async def stop_monitoring(task_id: str):
    """
    Остановить мониторинг

    Останавливает фоновую задачу мониторинга.
    """
    try:
        # Проверяем существование задачи
        task_state = state_manager.get_task(task_id)

        if not task_state:
            raise HTTPException(status_code=404, detail="Задача не найдена")

        # Останавливаем задачу
        state_manager.stop_task(task_id)

        # Обновляем статус в БД
        await db_service.update_task_status(
            task_id=task_id,
            status='stopped',
            stopped_at=datetime.utcnow().isoformat()
        )

        return StopMonitoringResponse(
            task_id=task_id,
            status='stopped',
            message='Мониторинг остановлен'
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка остановки задачи {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workers/list/{task_id}", response_model=FoundItemsListResponse)
async def get_found_items(task_id: str, limit: int = 50):
    """
    Получить список найденных объявлений

    Возвращает список объявлений, найденных задачей.
    """
    try:
        # Проверяем существование задачи
        task = await db_service.get_task(task_id)

        if not task:
            raise HTTPException(status_code=404, detail="Задача не найдена")

        # Получаем найденные объявления
        items = await db_service.get_found_items(task_id, limit)

        # Подсчитываем общее количество
        total = await db_service.count_items(task_id)

        # Формируем ответ
        response_items = [
            FoundItemResponse(
                id=item.id,
                author_username=item.author_username,
                author_full_name=item.author_full_name,
                date=item.date,
                price=item.price,
                shk=item.shk,
                location=item.location,
                message_link=item.message_link,
                found_at=item.found_at
            )
            for item in items
        ]

        return FoundItemsListResponse(
            task_id=task_id,
            mode=task.mode,
            total=total,
            items=response_items
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка получения списка объявлений для задачи {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workers/{item_id}/check-blacklist", response_model=CheckBlacklistResponse)
async def check_blacklist(item_id: int, task_id: str):
    """
    Отправить на проверку в Черный Список (заглушка)

    ВАЖНО: Это заглушка. Сервис blacklist_service будет создан позже.
    Сейчас просто возвращает {"is_blacklisted": false}.
    """
    try:
        # Проверяем существование объявления
        item = await db_service.get_found_item_by_id(item_id)

        if not item:
            raise HTTPException(status_code=404, detail="Объявление не найдено")

        # Заглушка - всегда возвращаем False
        return CheckBlacklistResponse(
            item_id=item_id,
            check_status='completed',
            result={
                'is_blacklisted': False,
                'messages': []
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка проверки черного списка для объявления {item_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
