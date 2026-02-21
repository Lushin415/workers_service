"""
Сервис для работы с SQLite базой данных
"""
import aiosqlite
import json
from typing import List, Optional
from datetime import datetime
from loguru import logger
from models_db import Task, FoundItem, BlacklistRecord


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
                    notification_chat_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    stopped_at TEXT,
                    session_path TEXT,
                    blacklist_session_path TEXT
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

            # Миграция: добавляем author_id (Telegram User ID)
            try:
                await db.execute("ALTER TABLE found_items ADD COLUMN author_id INTEGER")
                logger.info("Добавлен столбец author_id")
            except:
                pass  # Столбец уже существует

            # Миграция: UNIQUE(message_link) → UNIQUE(task_id, message_link)
            # Иначе второй пользователь не получит уведомление об объявлении, уже найденном первым
            try:
                needs_migration = True
                async with db.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='found_items'"
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and 'UNIQUE(task_id, message_link)' in row[0]:
                        needs_migration = False

                if needs_migration:
                    await db.execute("""
                        CREATE TABLE found_items_new (
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
                            topic_id INTEGER,
                            topic_name TEXT,
                            city TEXT,
                            metro_station TEXT,
                            district TEXT,
                            author_id INTEGER,
                            UNIQUE(task_id, message_link)
                        )
                    """)
                    await db.execute("""
                        INSERT INTO found_items_new (
                            id, task_id, mode, author_username, author_full_name,
                            date, price, shk, location, message_text, message_link,
                            chat_name, message_date, found_at, notified, content_hash,
                            topic_id, topic_name, city, metro_station, district, author_id
                        )
                        SELECT
                            id, task_id, mode, author_username, author_full_name,
                            date, price, shk, location, message_text, message_link,
                            chat_name, message_date, found_at, notified, content_hash,
                            topic_id, topic_name, city, metro_station, district, author_id
                        FROM found_items
                    """)
                    await db.execute("DROP TABLE found_items")
                    await db.execute("ALTER TABLE found_items_new RENAME TO found_items")
                    await db.execute("""
                        CREATE INDEX IF NOT EXISTS idx_content_hash
                        ON found_items(content_hash, found_at)
                    """)
                    await db.commit()
                    logger.info("Миграция: found_items UNIQUE(message_link) → UNIQUE(task_id, message_link)")
            except Exception as e:
                logger.error(f"Ошибка миграции found_items unique constraint: {e}")

            # Миграция: добавляем session_path и blacklist_session_path в tasks
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN session_path TEXT")
                logger.info("Добавлен столбец session_path в tasks")
            except:
                pass

            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN blacklist_session_path TEXT")
                logger.info("Добавлен столбец blacklist_session_path в tasks")
            except:
                pass

            # Таблица кеша черного списка
            await db.execute("""
                CREATE TABLE IF NOT EXISTS blacklist_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_user_id INTEGER NOT NULL UNIQUE,
                    username TEXT,
                    full_name TEXT,
                    phone TEXT,
                    role TEXT,
                    message_link TEXT NOT NULL,
                    message_id INTEGER,
                    parsed_at TEXT NOT NULL
                )
            """)

            # Индекс для быстрого поиска по telegram_user_id
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_blacklist_user_id
                ON blacklist_cache(telegram_user_id)
            """)

            # Таблица чатов черного списка (для динамического управления)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS blacklist_chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_username TEXT NOT NULL,
                    chat_title TEXT,
                    added_at TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    topic_id INTEGER,
                    topic_name TEXT,
                    UNIQUE(chat_username, topic_id)
                )
            """)

            # Миграция: пересоздаём blacklist_chats с новым UNIQUE(chat_username, topic_id)
            # Нужно, т.к. старая таблица имела UNIQUE(chat_username) — нельзя DROP constraint в SQLite
            try:
                # Проверяем, нужна ли миграция (есть ли столбец topic_id)
                async with db.execute("PRAGMA table_info(blacklist_chats)") as cursor:
                    columns = [row[1] for row in await cursor.fetchall()]

                if "topic_id" not in columns:
                    logger.info("Миграция blacklist_chats: добавляем topic_id/topic_name...")
                    await db.execute("""
                        CREATE TABLE blacklist_chats_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            chat_username TEXT NOT NULL,
                            chat_title TEXT,
                            added_at TEXT NOT NULL,
                            is_active BOOLEAN DEFAULT 1,
                            topic_id INTEGER,
                            topic_name TEXT,
                            UNIQUE(chat_username, COALESCE(topic_id, 0))
                        )
                    """)
                    await db.execute("""
                        INSERT INTO blacklist_chats_new (id, chat_username, chat_title, added_at, is_active)
                        SELECT id, chat_username, chat_title, added_at, is_active FROM blacklist_chats
                    """)
                    await db.execute("DROP TABLE blacklist_chats")
                    await db.execute("ALTER TABLE blacklist_chats_new RENAME TO blacklist_chats")
                    logger.info("Миграция blacklist_chats завершена")
            except Exception as e:
                logger.error(f"Ошибка миграции blacklist_chats: {e}")

            # Уникальный индекс для защиты от дублей (chat_username + topic_id)
            await db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_blacklist_chats_unique
                ON blacklist_chats(chat_username, COALESCE(topic_id, -1))
            """)

            # Добавляем дефолтный чат если ещё не существует
            await db.execute("""
                INSERT OR IGNORE INTO blacklist_chats (chat_username, chat_title, added_at, is_active)
                VALUES ('@Blacklist_pvz', 'Чёрный Список ПВЗ', datetime('now'), 1)
            """)

            await db.commit()
            logger.info("База данных инициализирована")

    async def create_task(self, task: Task):
        """Создать задачу"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO tasks
                (task_id, user_id, mode, chats, filters,
                 notification_chat_id, status, created_at, stopped_at,
                 session_path, blacklist_session_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.task_id, task.user_id, task.mode, task.chats, task.filters,
                task.notification_chat_id,
                task.status, task.created_at, task.stopped_at,
                task.session_path, task.blacklist_session_path
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

    async def get_tasks_by_status(self, status: str) -> List[Task]:
        """Получить все задачи с заданным статусом"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM tasks WHERE status = ?", (status,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [Task(**dict(row)) for row in rows]

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
        task_id: str,
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
                  AND task_id = ?
                  AND found_at > ?
            """, (content_hash, task_id, time_threshold)) as cursor:
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
        task_id: str,
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
                  AND task_id = ?
                  AND found_at > ?
                LIMIT 1
            """, (author_username, work_date, price, task_id, time_threshold)) as cursor:
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
                task_id=item.task_id,
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
                    (task_id, mode, author_username, author_full_name, author_id, date, price,
                     shk, location, city, metro_station, district,
                     message_text, message_link, chat_name,
                     message_date, found_at, notified, content_hash, topic_id, topic_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.task_id, item.mode, item.author_username, item.author_full_name,
                    item.author_id, item.date, item.price, item.shk, item.location,
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

    async def count_notified_items(self, task_id: str) -> int:
        """Подсчитать количество отправленных уведомлений (notified=1)"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM found_items WHERE task_id = ? AND notified = 1", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    # ========== Методы для работы с черным списком ==========

    async def add_blacklist_record(self, record: BlacklistRecord) -> Optional[int]:
        """
        Добавить или обновить запись в кеше черного списка

        Если запись с таким telegram_user_id уже есть — обновляем её.
        """
        async with aiosqlite.connect(self.db_path) as db:
            try:
                cursor = await db.execute("""
                    INSERT INTO blacklist_cache
                    (telegram_user_id, username, full_name, phone, role, message_link, message_id, parsed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(telegram_user_id) DO UPDATE SET
                        username = excluded.username,
                        full_name = excluded.full_name,
                        phone = excluded.phone,
                        role = excluded.role,
                        message_link = excluded.message_link,
                        message_id = excluded.message_id,
                        parsed_at = excluded.parsed_at
                """, (
                    record.telegram_user_id, record.username, record.full_name,
                    record.phone, record.role, record.message_link,
                    record.message_id, record.parsed_at
                ))
                await db.commit()
                return cursor.lastrowid
            except Exception as e:
                logger.error(f"Ошибка добавления записи в blacklist_cache: {e}")
                return None

    async def find_in_blacklist(self, telegram_user_id: int) -> Optional[BlacklistRecord]:
        """
        Поиск пользователя в черном списке по Telegram User ID

        Args:
            telegram_user_id: Telegram User ID для поиска

        Returns:
            BlacklistRecord если найден, иначе None
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM blacklist_cache WHERE telegram_user_id = ?",
                (telegram_user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return BlacklistRecord(**dict(row))
                return None

    async def clear_blacklist_cache(self):
        """Очистить кеш черного списка"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM blacklist_cache")
            await db.commit()
            logger.info("Кеш черного списка очищен")

    async def get_blacklist_stats(self) -> dict:
        """
        Получить статистику кеша черного списка

        Returns:
            Словарь с количеством записей и датой последнего обновления
        """
        async with aiosqlite.connect(self.db_path) as db:
            # Общее количество записей
            async with db.execute("SELECT COUNT(*) FROM blacklist_cache") as cursor:
                row = await cursor.fetchone()
                total_count = row[0] if row else 0

            # Количество по ролям
            async with db.execute("""
                SELECT role, COUNT(*) FROM blacklist_cache GROUP BY role
            """) as cursor:
                rows = await cursor.fetchall()
                by_role = {row[0]: row[1] for row in rows}

            # Последнее обновление
            async with db.execute("""
                SELECT MAX(parsed_at) FROM blacklist_cache
            """) as cursor:
                row = await cursor.fetchone()
                last_update = row[0] if row and row[0] else None

            return {
                "total_records": total_count,
                "workers": by_role.get("worker", 0),
                "employers": by_role.get("employer", 0),
                "last_update": last_update
            }

    async def get_author_id_by_item(self, item_id: int) -> Optional[int]:
        """
        Получить author_id по ID объявления

        Args:
            item_id: ID записи в found_items

        Returns:
            author_id (Telegram User ID) или None
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT author_id FROM found_items WHERE id = ?", (item_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    return row[0]
                return None

    async def get_blacklist_session_by_item(self, item_id: int) -> Optional[str]:
        """
        Получить blacklist_session_path по ID объявления

        Ищет task_id по item_id, затем session_path по task_id.

        Args:
            item_id: ID записи в found_items

        Returns:
            blacklist_session_path или None
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT t.blacklist_session_path
                FROM found_items fi
                JOIN tasks t ON fi.task_id = t.task_id
                WHERE fi.id = ?
            """, (item_id,)) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    return row[0]
                return None

    # ========== Методы для управления чатами черного списка ==========

    async def get_blacklist_chats(self, active_only: bool = True) -> List[dict]:
        """
        Получить список чатов черного списка

        Args:
            active_only: только активные чаты

        Returns:
            Список словарей с chat_username, topic_id, topic_name
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if active_only:
                query = "SELECT chat_username, topic_id, topic_name FROM blacklist_chats WHERE is_active = 1"
            else:
                query = "SELECT chat_username, topic_id, topic_name FROM blacklist_chats"

            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def sync_blacklist_chats(self, chats: list) -> int:
        """
        Полная замена списка чатов ЧС.
        chats — список dict с ключами: chat_username, topic_id (опц.), topic_name (опц.)
        Возвращает количество добавленных чатов.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM blacklist_chats")
            count = 0
            for entry in chats:
                chat_username = entry.get("chat_username", "").lower()
                if not chat_username.startswith("@"):
                    chat_username = f"@{chat_username}"
                topic_id = entry.get("topic_id")
                topic_name = entry.get("topic_name")
                await db.execute("""
                    INSERT OR IGNORE INTO blacklist_chats (chat_username, added_at, is_active, topic_id, topic_name)
                    VALUES (?, datetime('now'), 1, ?, ?)
                """, (chat_username, topic_id, topic_name))
                count += 1
            await db.commit()
            logger.info(f"Синхронизировано чатов ЧС: {count}")
            return count

    async def add_blacklist_chat(
        self,
        chat_username: str,
        chat_title: Optional[str] = None,
        topic_id: Optional[int] = None,
        topic_name: Optional[str] = None,
    ) -> bool:
        """
        Добавить чат в список черного списка

        Args:
            chat_username: username чата (например @Blacklist_pvz)
            chat_title: название чата (опционально)
            topic_id: ID топика форума (опционально, None = весь чат)
            topic_name: название топика (опционально)

        Returns:
            True если добавлен, False если уже существует
        """
        # Нормализуем username (lowercase + @)
        chat_username = chat_username.lower()
        if not chat_username.startswith("@"):
            chat_username = f"@{chat_username}"

        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("""
                    INSERT OR IGNORE INTO blacklist_chats (chat_username, chat_title, added_at, is_active, topic_id, topic_name)
                    VALUES (?, ?, datetime('now'), 1, ?, ?)
                """, (chat_username, chat_title, topic_id, topic_name))
                await db.commit()
                topic_info = f" (топик: {topic_name})" if topic_name else ""
                logger.info(f"Добавлен чат ЧС: {chat_username}{topic_info}")
                return True
            except aiosqlite.IntegrityError:
                # Чат уже существует - активируем его
                if topic_id is not None:
                    await db.execute("""
                        UPDATE blacklist_chats SET is_active = 1 WHERE chat_username = ? AND topic_id = ?
                    """, (chat_username, topic_id))
                else:
                    await db.execute("""
                        UPDATE blacklist_chats SET is_active = 1 WHERE chat_username = ? AND topic_id IS NULL
                    """, (chat_username,))
                await db.commit()
                logger.info(f"Чат ЧС активирован: {chat_username}")
                return True

    async def remove_blacklist_chat(self, chat_username: str, topic_id: Optional[int] = None) -> bool:
        """
        Удалить (деактивировать) чат из списка черного списка

        Args:
            chat_username: username чата
            topic_id: ID топика (None = запись без топика)

        Returns:
            True если удалён, False если не найден
        """
        if not chat_username.startswith("@"):
            chat_username = f"@{chat_username}"

        async with aiosqlite.connect(self.db_path) as db:
            if topic_id is not None:
                cursor = await db.execute("""
                    UPDATE blacklist_chats SET is_active = 0 WHERE chat_username = ? AND topic_id = ?
                """, (chat_username, topic_id))
            else:
                cursor = await db.execute("""
                    UPDATE blacklist_chats SET is_active = 0 WHERE chat_username = ? AND topic_id IS NULL
                """, (chat_username,))
            await db.commit()

            if cursor.rowcount > 0:
                logger.info(f"Чат ЧС деактивирован: {chat_username}")
                return True
            return False

    async def get_blacklist_chats_info(self) -> List[dict]:
        """
        Получить полную информацию о чатах черного списка

        Returns:
            Список словарей с информацией о чатах
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT chat_username, chat_title, added_at, is_active, topic_id, topic_name
                FROM blacklist_chats
                ORDER BY added_at
            """) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    # ========== Cleanup методы ==========

    async def cleanup_old_items(self, days: int = 30) -> int:
        """
        Удалить найденные объявления старше N дней

        Используется для автоматической очистки БД от устаревших записей.
        Запускается фоновой задачей раз в сутки.

        Args:
            days: количество дней (записи старше будут удалены)

        Returns:
            Количество удалённых записей
        """
        from datetime import datetime, timedelta

        # Временная граница (записи старше этой даты удаляем)
        threshold = (datetime.utcnow() - timedelta(days=days)).isoformat()

        async with aiosqlite.connect(self.db_path) as db:
            # Удаляем старые записи
            cursor = await db.execute(
                "DELETE FROM found_items WHERE found_at < ?",
                (threshold,)
            )
            deleted_count = cursor.rowcount
            await db.commit()

            if deleted_count > 0:
                logger.info(f"Auto-cleanup: удалено {deleted_count} записей старше {days} дней")
            else:
                logger.debug(f"Auto-cleanup: старых записей не найдено (>{days} дней)")

            return deleted_count

    async def get_db_stats(self) -> dict:
        """
        Получить статистику БД (для мониторинга)

        Returns:
            Словарь с количеством записей по таблицам
        """
        async with aiosqlite.connect(self.db_path) as db:
            stats = {}

            # Количество задач
            async with db.execute("SELECT COUNT(*) FROM tasks") as cursor:
                row = await cursor.fetchone()
                stats['tasks_count'] = row[0] if row else 0

            # Количество найденных объявлений
            async with db.execute("SELECT COUNT(*) FROM found_items") as cursor:
                row = await cursor.fetchone()
                stats['found_items_count'] = row[0] if row else 0

            # Количество в кеше ЧС
            async with db.execute("SELECT COUNT(*) FROM blacklist_cache") as cursor:
                row = await cursor.fetchone()
                stats['blacklist_cache_count'] = row[0] if row else 0

            # Самая старая запись в found_items
            async with db.execute(
                "SELECT MIN(found_at) FROM found_items"
            ) as cursor:
                row = await cursor.fetchone()
                stats['oldest_found_item'] = row[0] if row and row[0] else None

            # Самая новая запись
            async with db.execute(
                "SELECT MAX(found_at) FROM found_items"
            ) as cursor:
                row = await cursor.fetchone()
                stats['newest_found_item'] = row[0] if row and row[0] else None

            return stats


