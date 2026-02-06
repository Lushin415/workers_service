"""
Управление состоянием задач мониторинга
"""
import asyncio
import threading
from typing import Dict, Optional
from datetime import datetime
from loguru import logger


class StateManager:
    """Менеджер состояния задач"""

    def __init__(self):
        self._tasks: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def create_task(self, task_id: str, mode: str) -> asyncio.Event:
        """
        Создать новую задачу

        Returns:
            asyncio.Event для остановки задачи
        """
        stop_event = asyncio.Event()

        with self._lock:
            self._tasks[task_id] = {
                'task_id': task_id,
                'mode': mode,
                'status': 'pending',
                'stop_event': stop_event,
                'asyncio_task': None,
                'stats': {
                    'total_messages_scanned': 0,
                    'items_found': 0,
                    'notifications_sent': 0,
                    'last_update': datetime.utcnow().isoformat() + 'Z'
                }
            }

        logger.info(f"Задача {task_id} создана в state_manager")
        return stop_event

    def set_asyncio_task(self, task_id: str, asyncio_task: asyncio.Task):
        """Сохранить ссылку на asyncio.Task"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]['asyncio_task'] = asyncio_task

    def get_task(self, task_id: str) -> Optional[Dict]:
        """Получить информацию о задаче"""
        with self._lock:
            return self._tasks.get(task_id)

    def update_status(self, task_id: str, status: str):
        """Обновить статус задачи"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]['status'] = status
                self._tasks[task_id]['stats']['last_update'] = datetime.utcnow().isoformat() + 'Z'
                logger.info(f"Статус задачи {task_id} обновлён на '{status}'")

    def update_stats(
        self,
        task_id: str,
        messages_scanned: int = 0,
        items_found: int = 0,
        notifications_sent: int = 0
    ):
        """Обновить статистику задачи"""
        with self._lock:
            if task_id in self._tasks:
                stats = self._tasks[task_id]['stats']
                stats['total_messages_scanned'] += messages_scanned
                stats['items_found'] += items_found
                stats['notifications_sent'] += notifications_sent
                stats['last_update'] = datetime.utcnow().isoformat() + 'Z'

    def get_stats(self, task_id: str) -> Optional[Dict]:
        """Получить статистику задачи"""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                return task['stats'].copy()
            return None

    def stop_task(self, task_id: str):
        """Остановить задачу"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]['stop_event'].set()
                self._tasks[task_id]['status'] = 'stopped'
                # Отменяем asyncio task если есть
                asyncio_task = self._tasks[task_id].get('asyncio_task')
                if asyncio_task and not asyncio_task.done():
                    asyncio_task.cancel()
                logger.info(f"Задача {task_id} остановлена")

    def remove_task(self, task_id: str):
        """Удалить задачу из менеджера"""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                logger.info(f"Задача {task_id} удалена из state_manager")

    def is_stopped(self, task_id: str) -> bool:
        """Проверить, остановлена ли задача"""
        with self._lock:
            if task_id in self._tasks:
                return self._tasks[task_id]['stop_event'].is_set()
            return True


# Глобальный экземпляр
state_manager = StateManager()
