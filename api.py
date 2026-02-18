"""
FastAPI REST API для Workers Service
"""
import json
import uuid
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
from loguru import logger

from config import config
from typing import Optional
from models_api import (
    StartMonitoringRequest,
    StartMonitoringResponse,
    TaskStatusResponse,
    StopMonitoringResponse,
    FoundItemsListResponse,
    FoundItemResponse,
    CheckBlacklistResponse,
    BlacklistChatsListResponse,
    BlacklistChatTopicsResponse,
)
from models_db import Task
from db_service import DBService
from state_manager import state_manager
from tasks import start_monitoring_task
from blacklist_service import BlacklistService
from callback_handler import CallbackHandler


# Настройка логирования
logger.add(config.LOG_PATH, rotation="10 MB", level="INFO")

# Создание FastAPI приложения
app = FastAPI(
    title="Workers Service",
    description="Микросервис для мониторинга Telegram чатов и поиска объявлений о работе на ПВЗ",
    version="1.0.0"
)

# Сервис БД
db_service = DBService(db_path=config.DB_PATH)

# Сервис черного списка (инициализируется при startup)
blacklist_service: BlacklistService = None

# Фоновая задача auto-cleanup
cleanup_task = None


async def cleanup_old_items_periodically():
    """
    Фоновая задача для автоматической очистки старых записей

    Запускается раз в сутки и удаляет объявления старше 30 дней.
    Предотвращает бесконечный рост БД в production.
    """
    while True:
        try:
            # Ждём 24 часа (86400 секунд)
            await asyncio.sleep(86400)

            # Очищаем записи старше 30 дней
            deleted_count = await db_service.cleanup_old_items(days=30)

            if deleted_count > 0:
                logger.info(f"✅ Auto-cleanup: удалено {deleted_count} записей старше 30 дней")

            # Логируем статистику БД
            stats = await db_service.get_db_stats()
            logger.info(
                f"📊 Статистика БД: "
                f"задач={stats['tasks_count']}, "
                f"объявлений={stats['found_items_count']}, "
                f"кеш ЧС={stats['blacklist_cache_count']}"
            )

        except Exception as e:
            logger.error(f"❌ Ошибка в фоновой задаче cleanup: {e}")
            # Продолжаем работу даже при ошибке
            await asyncio.sleep(3600)  # Повтор через 1 час при ошибке


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    global blacklist_service, cleanup_task

    # Инициализация БД
    await db_service.init_db()

    # Инициализация сервиса черного списка (без запуска клиента!)
    # Используем ОТДЕЛЬНУЮ сессию чтобы не конфликтовать с основным парсером
    blacklist_service = BlacklistService(
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_name=config.BLACKLIST_SESSION_PATH,
        db_service=db_service
    )
    logger.info(f"BlacklistService инициализирован (сессия: {config.BLACKLIST_SESSION_PATH})")

    # Callback-кнопки обрабатываются в PurserHub (главном боте),
    # т.к. BOT_TOKEN общий и polling ведёт PurserHub.
    # Workers-service только отправляет уведомления через Bot API.
    logger.info("Callback handler отключён (обработка в PurserHub)")

    # Запускаем фоновую задачу auto-cleanup (раз в сутки)
    cleanup_task = asyncio.create_task(cleanup_old_items_periodically())
    logger.info("🧹 Auto-cleanup задача запущена (удаление записей старше 30 дней раз в сутки)")

    logger.info("Workers Service запущен на порту 8002")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке"""
    global cleanup_task

    logger.info("=" * 60)
    logger.info("ОСТАНОВКА WORKERS SERVICE")
    logger.info("=" * 60)

    # Graceful shutdown: останавливаем все активные задачи мониторинга
    logger.info("Остановка активных задач мониторинга...")
    active_tasks = list(state_manager._tasks.keys())

    for task_id in active_tasks:
        try:
            # Останавливаем задачу в state_manager
            state_manager.stop_task(task_id)

            # Обновляем статус в БД
            await db_service.update_task_status(
                task_id=task_id,
                status='stopped',
                stopped_at=datetime.utcnow().isoformat()
            )
            logger.debug(f"Задача {task_id} остановлена")
        except Exception as e:
            logger.error(f"Ошибка остановки задачи {task_id}: {e}")

    logger.info(f"✅ Остановлено {len(active_tasks)} задач мониторинга")

    # Останавливаем фоновую задачу cleanup
    if cleanup_task and not cleanup_task.done():
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            logger.info("🧹 Auto-cleanup задача остановлена")

    logger.success("Workers Service остановлен")


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "Workers Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Healthcheck для Docker"""
    return {"status": "healthy"}


@app.post("/workers/start", response_model=StartMonitoringResponse)
async def start_monitoring(request: StartMonitoringRequest):
    """
    Запустить мониторинг Telegram чатов

    Создает задачу мониторинга и запускает её в фоновом потоке.
    """
    try:
        # Генерируем task_id
        task_id = str(uuid.uuid4())

        # Определяем пути к сессиям (из запроса или fallback на конфиг)
        session_path = request.session_path or config.SESSION_PATH
        blacklist_session_path = request.blacklist_session_path or config.BLACKLIST_SESSION_PATH

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
                'shk_filter': request.filters.shk_filter,
                'city_filter': request.filters.city_filter
            }),
            notification_chat_id=request.notification_chat_id,
            status='pending',
            created_at=datetime.utcnow().isoformat(),
            session_path=session_path,
            blacklist_session_path=blacklist_session_path
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
                'shk_filter': request.filters.shk_filter,
                'city_filter': request.filters.city_filter
            },
            api_id=request.api_id or config.API_ID,
            api_hash=request.api_hash or config.API_HASH,
            notification_chat_id=request.notification_chat_id,
            parse_history_days=request.parse_history_days,
            session_path=session_path
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
async def check_blacklist_by_item(item_id: int, task_id: str):
    """
    Проверить автора объявления в Черном Списке

    Парсит @Blacklist_pvz в реальном времени и ищет совпадение по ID или username.
    """
    try:
        # Проверяем существование объявления
        item = await db_service.get_found_item_by_id(item_id)

        if not item:
            raise HTTPException(status_code=404, detail="Объявление не найдено")

        # Проверяем наличие author_id или author_username
        if not item.author_id and not item.author_username:
            return CheckBlacklistResponse(
                item_id=item_id,
                check_status='error',
                result={
                    'found': False,
                    'error': 'Telegram User ID и username автора неизвестны'
                }
            )

        # Получаем blacklist_session_path из задачи
        bl_session = await db_service.get_blacklist_session_by_item(item_id)

        # Ищем в черном списке (в реальном времени, с сессией пользователя)
        result = await blacklist_service.search_in_blacklist(
            user_id=item.author_id,
            username=item.author_username,
            session_name=bl_session
        )

        return CheckBlacklistResponse(
            item_id=item_id,
            check_status='completed',
            result=result
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка проверки черного списка для объявления {item_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/blacklist/check")
async def check_in_blacklist(username: str, blacklist_session_path: str = None):
    """
    Проверить пользователя в черном списке по username

    Второй сценарий: пользователь вводит @username и получает результат.
    Поиск идёт по ВСЕМ активным чатам ЧС.

    Args:
        username: Telegram username (с или без @)
        blacklist_session_path: путь к сессии ЧС (опционально, fallback на конфиг)
    """
    try:
        if not blacklist_service:
            raise HTTPException(status_code=503, detail="Сервис черного списка не инициализирован")

        # Ищем в черном списке (в реальном времени по всем чатам)
        result = await blacklist_service.search_in_blacklist(
            username=username,
            session_name=blacklist_session_path
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка проверки username {username} в черном списке: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== Управление чатами черного списка ==========

@app.get("/blacklist/chats", response_model=BlacklistChatsListResponse)
async def get_blacklist_chats():
    """
    Получить список чатов черного списка

    Возвращает все чаты (активные и неактивные) с информацией.
    """
    try:
        chats = await db_service.get_blacklist_chats_info()
        return {
            "chats": chats,
            "total": len(chats),
            "active": len([c for c in chats if c.get("is_active")])
        }

    except Exception as e:
        logger.error(f"Ошибка получения списка чатов ЧС: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/blacklist/chats/sync")
async def sync_blacklist_chats(chats: list = Body(...)):
    """Полная замена списка чатов ЧС (вызывается из parserhub при сохранении настроек)"""
    try:
        count = await db_service.sync_blacklist_chats(chats)
        return {"status": "ok", "synced": count}
    except Exception as e:
        logger.error(f"Ошибка синхронизации чатов ЧС: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/blacklist/chats/add")
async def add_blacklist_chat(
    chat_username: str,
    chat_title: Optional[str] = None,
    topic_id: Optional[int] = None,
    topic_name: Optional[str] = None,
):
    """
    Добавить чат в список черного списка

    Args:
        chat_username: username чата (например @Blacklist_pvz_2)
        chat_title: название чата (опционально)
        topic_id: ID топика форума (опционально, None = весь чат)
        topic_name: название топика (опционально)
    """
    try:
        result = await db_service.add_blacklist_chat(
            chat_username, chat_title,
            topic_id=topic_id, topic_name=topic_name,
        )

        topic_info = f" (топик: {topic_name})" if topic_name else ""
        if result:
            return {
                "status": "success",
                "message": f"Чат {chat_username}{topic_info} добавлен в список ЧС"
            }
        else:
            return {
                "status": "exists",
                "message": f"Чат {chat_username}{topic_info} уже существует"
            }

    except Exception as e:
        logger.error(f"Ошибка добавления чата ЧС: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/blacklist/chats/remove")
async def remove_blacklist_chat(chat_username: str, topic_id: Optional[int] = None):
    """
    Удалить (деактивировать) чат из списка черного списка

    Args:
        chat_username: username чата
        topic_id: ID топика (None = запись без топика)
    """
    try:
        result = await db_service.remove_blacklist_chat(chat_username, topic_id=topic_id)

        if result:
            return {
                "status": "success",
                "message": f"Чат {chat_username} удалён из списка ЧС"
            }
        else:
            raise HTTPException(status_code=404, detail=f"Чат {chat_username} не найден")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка удаления чата ЧС: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/blacklist/chats/topics", response_model=BlacklistChatTopicsResponse)
async def get_chat_topics(chat_username: str, blacklist_session_path: Optional[str] = None):
    """
    Получить список топиков форума (если чат является форумом)

    Args:
        chat_username: username чата (например @spbpvz)
        blacklist_session_path: путь к сессии ЧС (опционально)

    Returns:
        is_forum: bool, topics: [{id, name}]
    """
    from pyrogram import Client
    from pyrogram.raw.functions.channels import GetForumTopics
    from pyrogram.raw.types import InputPeerChannel

    effective_session = blacklist_session_path or config.BLACKLIST_SESSION_PATH

    client = Client(
        name=effective_session,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
    )

    try:
        await client.start()

        chat = await client.get_chat(chat_username)
        chat_id = chat.id
        chat_title = chat.title

        # Пробуем получить топики
        peer = await client.resolve_peer(chat_id)

        if not isinstance(peer, InputPeerChannel):
            return {
                "is_forum": False,
                "chat_title": chat_title,
                "topics": []
            }

        result = await client.invoke(
            GetForumTopics(
                channel=peer,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            )
        )

        topics = []
        if hasattr(result, 'topics'):
            for topic in result.topics:
                topics.append({
                    "id": topic.id,
                    "name": topic.title
                })

        return {
            "is_forum": len(topics) > 0,
            "chat_title": chat_title,
            "topics": topics
        }

    except Exception as e:
        error_str = str(e)
        if "CHANNEL_FORUM_MISSING" in error_str:
            return {
                "is_forum": False,
                "chat_title": chat_username,
                "topics": []
            }
        logger.error(f"Ошибка получения топиков чата {chat_username}: {e}")
        raise HTTPException(status_code=500, detail=error_str)

    finally:
        await client.stop()


# ========== Admin Endpoints ==========

@app.get("/admin/stats")
async def get_db_stats():
    """
    Получить статистику БД (для мониторинга)

    Returns:
        Словарь с количеством записей, датами, размером БД
    """
    try:
        stats = await db_service.get_db_stats()
        return {
            "status": "success",
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики БД: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/cleanup")
async def manual_cleanup(days: int = 30):
    """
    Ручная очистка старых записей

    Args:
        days: удалить записи старше N дней (по умолчанию 30)

    Returns:
        Количество удалённых записей
    """
    try:
        if days < 1 or days > 365:
            raise HTTPException(status_code=400, detail="days должно быть от 1 до 365")

        deleted_count = await db_service.cleanup_old_items(days=days)

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "message": f"Удалено записей старше {days} дней: {deleted_count}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка ручной очистки БД: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.HOST, port=config.PORT)
