"""
Сервис для работы с SQLite базой данных
"""
import aiosqlite
import json
from typing import List, Optional
from datetime import datetime
from loguru import logger
from models_db import Task, FoundItem


class DBService:
    """Сервис для работы с базой данных"""

    def __init__(self, db_path: str = "workers.db"):
        self.db_path = db_path

    async def init_db(self):
        """Инициализация базы данных"""
        async with aiosqlite.connect(self.db_path) as db:
            # Таблица задач
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    chats TEXT NOT NULL,
                    filters TEXT NOT NULL,
                    notification_bot_token TEXT NOT NULL,
                    notification_chat_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    stopped_at TEXT
                )
            """)

            # Таблица найденных объявлений
            await db.execute("""
                CREATE TABLE IF NOT EXISTS found_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    author_username TEXT,
                    author_full_name TEXT,
                    date TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    shk TEXT,
                    location TEXT,
                    message_text TEXT NOT NULL,
                    message_link TEXT NOT NULL,
                    chat_name TEXT NOT NULL,
                    message_date TEXT NOT NULL,
                    found_at TEXT NOT NULL,
                    notified BOOLEAN DEFAULT 0,
                    content_hash TEXT,
                    UNIQUE(message_link)
                )
            """)

            # Добавляем индекс на content_hash для быстрого поиска дубликатов
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_content_hash
                ON found_items(content_hash, found_at)
            """)

            # Миграция: добавляем topic_id и topic_name (если еще не добавлены)
            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN topic_id INTEGER")
                logger.info("Добавлен столбец topic_id")
            except:
                pass  # Столбец уже существует

            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN topic_name TEXT")
                logger.info("Добавлен столбец topic_name")
            except:
                pass  # Столбец уже существует

            # Миграция: добавляем структурированные поля локации
            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN city TEXT")
                logger.info("Добавлен столбец city")
            except:
                pass

            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN metro_station TEXT")
                logger.info("Добавлен столбец metro_station")
            except:
                pass

            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN district TEXT")
                logger.info("Добавлен столбец district")
            except:
                pass

            await db.commit()
            logger.info("База данных инициализирована")

    async def create_task(self, task: Task):
        """Создать задачу"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO tasks
                (task_id, user_id, mode, chats, filters, notification_bot_token,
                 notification_chat_id, status, created_at, stopped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.task_id, task.user_id, task.mode, task.chats, task.filters,
                task.notification_bot_token, task.notification_chat_id,
                task.status, task.created_at, task.stopped_at
            ))
            await db.commit()
            logger.info(f"Задача {task.task_id} создана")

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Получить задачу по ID"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Task(**dict(row))
                return None

    async def update_task_status(self, task_id: str, status: str, stopped_at: Optional[str] = None):
        """Обновить статус задачи"""
        async with aiosqlite.connect(self.db_path) as db:
            if stopped_at:
                await db.execute(
                    "UPDATE tasks SET status = ?, stopped_at = ? WHERE task_id = ?",
                    (status, stopped_at, task_id)
                )
            else:
                await db.execute(
                    "UPDATE tasks SET status = ? WHERE task_id = ?",
                    (status, task_id)
                )
            await db.commit()
            logger.info(f"Статус задачи {task_id} обновлён на {status}")

    async def check_duplicate_smart(
        self,
        content_hash: str,
        work_date: str,
        hours_window: int = 24
    ) -> bool:
        """
        Умная проверка дубликатов с учетом временного окна и даты работы

        Логика:
        - Ищем записи с таким же content_hash за последние N часов
        - Если нашли запись с ТАКОЙ ЖЕ датой работы (date) → дубликат
        - Если нашли, но дата работы ДРУГАЯ → НЕ дубликат (обновление объявления)

        Args:
            content_hash: хеш содержимого
            work_date: дата работы (extracted['date'])
            hours_window: временное окно в часах (по умолчанию 24)

        Returns:
            True если дубликат, False если новое объявление
        """
        async with aiosqlite.connect(self.db_path) as db:
            # Временная метка N часов назад
            from datetime import datetime, timedelta
            time_threshold = (datetime.utcnow() - timedelta(hours=hours_window)).isoformat()

            # Ищем записи с таким же хешем за последние N часов
            async with db.execute("""
                SELECT date FROM found_items
                WHERE content_hash = ?
                  AND found_at > ?
            """, (content_hash, time_threshold)) as cursor:
                rows = await cursor.fetchall()

                # Если не нашли записей с таким хешем → не дубликат
                if not rows:
                    return False

                # Проверяем, есть ли запись с ТАКОЙ ЖЕ датой работы
                existing_dates = [row[0] for row in rows]

                if work_date in existing_dates:
                    # Нашли запись с той же датой работы → ДУБЛИКАТ
                    logger.debug(
                        f"Дубликат обнаружен: content_hash={content_hash[:8]}..., "
                        f"дата работы={work_date}"
                    )
                    return True
                else:
                    # Хеш тот же, но дата работы ДРУГАЯ → НОВОЕ объявление
                    logger.debug(
                        f"Обновление объявления: content_hash={content_hash[:8]}..., "
                        f"новая дата работы={work_date}, старые даты={existing_dates}"
                    )
                    return False

    async def check_duplicate_by_author(
        self,
        author_username: Optional[str],
        work_date: str,
        price: int,
        hours_window: int = 24
    ) -> bool:
        """
        Проверка дубликатов по автору (УРОВЕНЬ 2 дедупликации)

        Логика:
        - Автор + дата работы + цена = уникальная комбинация
        - Если автор меняет ЦЕНУ → НЕ дубликат (важно!)
        - Если автор ищет работу на ДРУГОЙ ДЕНЬ → НЕ дубликат
        - Если автор копирует в разные чаты → дубликат
        - Через 24 часа сброс (автор может снова разместить)

        Примеры:
        1. "Ivan" + "2026-02-03" + "3000" в чат1 (10:00) → сохранили
        2. "Ivan" + "2026-02-03" + "3000" в чат2 (11:00) → ДУБЛИКАТ ❌
        3. "Ivan" + "2026-02-03" + "2500" в чат3 (12:00) → НОВОЕ (цена!) ✅
        4. "Ivan" + "2026-02-05" + "3000" в чат4 (13:00) → НОВОЕ (дата!) ✅

        Args:
            author_username: username автора (может быть None для анонимных)
            work_date: дата работы (ISO format)
            price: цена за смену
            hours_window: временное окно в часах (по умолчанию 24)

        Returns:
            True если дубликат, False если новое объявление
        """
        # Если автора нет (анонимное сообщение) → проверка невозможна
        if not author_username:
            return False

        async with aiosqlite.connect(self.db_path) as db:
            # Временная метка N часов назад
            from datetime import datetime, timedelta
            time_threshold = (datetime.utcnow() - timedelta(hours=hours_window)).isoformat()

            # Ищем записи от ЭТОГО АВТОРА с ЭТОЙ ДАТОЙ и ЭТОЙ ЦЕНОЙ за последние N часов
            async with db.execute("""
                SELECT id FROM found_items
                WHERE author_username = ?
                  AND date = ?
                  AND price = ?
                  AND found_at > ?
                LIMIT 1
            """, (author_username, work_date, price, time_threshold)) as cursor:
                row = await cursor.fetchone()

                if row:
                    # Нашли запись от этого автора с той же датой и ценой → ДУБЛИКАТ
                    logger.debug(
                        f"Дубликат по автору: {author_username}, дата={work_date}, цена={price}"
                    )
                    return True
                else:
                    # Не нашли → это НОВОЕ объявление
                    # (автор либо изменил цену, либо ищет работу на другой день)
                    logger.debug(
                        f"Новое объявление от {author_username}: дата={work_date}, цена={price}"
                    )
                    return False

    async def add_found_item(self, item: FoundItem) -> Optional[int]:
        """
        Добавить найденное объявление (с умной дедупликацией)

        Проверяет:
        1. message_link (UNIQUE constraint) - защита от повторной обработки
        2. content_hash + дата работы + временное окно 24ч - умная дедупликация
        """
        # Умная проверка дубликатов (если есть content_hash)
        if item.content_hash:
            is_duplicate = await self.check_duplicate_smart(
                content_hash=item.content_hash,
                work_date=item.date,
                hours_window=24
            )

            if is_duplicate:
                logger.debug(
                    f"Умная дедупликация: объявление пропущено "
                    f"(hash={item.content_hash[:8]}..., дата={item.date})"
                )
                return None

        # Добавляем в БД
        async with aiosqlite.connect(self.db_path) as db:
            try:
                cursor = await db.execute("""
                    INSERT INTO found_items
                    (task_id, mode, author_username, author_full_name, date, price,
                     shk, location, city, metro_station, district,
                     message_text, message_link, chat_name,
                     message_date, found_at, notified, content_hash, topic_id, topic_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.task_id, item.mode, item.author_username, item.author_full_name,
                    item.date, item.price, item.shk, item.location,
                    item.city, item.metro_station, item.district,
                    item.message_text, item.message_link, item.chat_name, item.message_date,
                    item.found_at, item.notified, item.content_hash, item.topic_id, item.topic_name
                ))
                await db.commit()
                logger.info(f"Добавлено объявление: {item.message_link}")
                return cursor.lastrowid
            except aiosqlite.IntegrityError:
                logger.debug(f"Дубликат по message_link пропущен: {item.message_link}")
                return None

    async def get_found_items(self, task_id: str, limit: int = 50) -> List[FoundItem]:
        """Получить список найденных объявлений"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM found_items WHERE task_id = ? ORDER BY found_at DESC LIMIT ?",
                (task_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [FoundItem(**dict(row)) for row in rows]

    async def get_found_item_by_id(self, item_id: int) -> Optional[FoundItem]:
        """Получить объявление по ID"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM found_items WHERE id = ?", (item_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return FoundItem(**dict(row))
                return None

    async def mark_as_notified(self, item_id: int):
        """Отметить объявление как отправленное"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE found_items SET notified = 1 WHERE id = ?", (item_id,)
            )
            await db.commit()

    async def count_items(self, task_id: str) -> int:
        """Подсчитать количество найденных объявлений"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM found_items WHERE task_id = ?", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
