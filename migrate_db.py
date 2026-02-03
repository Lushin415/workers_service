"""
Скрипт миграции БД - добавление поля content_hash
"""
import asyncio
import sqlite3
from db_service import DBService
from loguru import logger


async def migrate():
    """Миграция базы данных"""
    print("=" * 70)
    print("МИГРАЦИЯ БАЗЫ ДАННЫХ")
    print("=" * 70)
    print()

    # Проверяем текущую структуру
    print("1️⃣  Проверка текущей структуры БД...")
    conn = sqlite3.connect("workers.db")
    cursor = conn.cursor()

    # Получаем схему таблицы found_items
    cursor.execute("PRAGMA table_info(found_items)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    print(f"   Текущие поля: {', '.join(column_names)}")

    if 'content_hash' in column_names:
        print("   ✅ Поле content_hash уже существует")
        print()
        print("   Хотите пересоздать БД? (все данные будут удалены)")
        print("   Введите 'yes' для пересоздания или Enter для отмены")
        response = input("   > ").strip().lower()

        if response != 'yes':
            print("   Миграция отменена")
            conn.close()
            return

        print()
        print("2️⃣  Удаление старых таблиц...")
        cursor.execute("DROP TABLE IF EXISTS found_items")
        cursor.execute("DROP TABLE IF EXISTS tasks")
        cursor.execute("DROP INDEX IF EXISTS idx_content_hash")
        conn.commit()
        print("   ✅ Старые таблицы удалены")
    else:
        print("   ℹ️  Поле content_hash отсутствует, добавляем...")
        print()
        print("2️⃣  Добавление поля content_hash...")
        try:
            cursor.execute("ALTER TABLE found_items ADD COLUMN content_hash TEXT")
            conn.commit()
            print("   ✅ Поле content_hash добавлено")
        except sqlite3.OperationalError as e:
            print(f"   ⚠️  Ошибка: {e}")
            print("   Пересоздаем таблицы...")
            cursor.execute("DROP TABLE IF EXISTS found_items")
            cursor.execute("DROP TABLE IF EXISTS tasks")
            cursor.execute("DROP INDEX IF EXISTS idx_content_hash")
            conn.commit()

    conn.close()

    print()
    print("3️⃣  Инициализация новой схемы...")

    # Используем DBService для создания таблиц с новой схемой
    db = DBService()
    await db.init_db()

    print("   ✅ База данных обновлена")
    print()
    print("=" * 70)
    print("МИГРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 70)
    print()
    print("📋 Новая структура таблицы found_items:")
    print("   - id, task_id, mode, author_username, author_full_name")
    print("   - date, price, shk, location")
    print("   - message_text, message_link, chat_name")
    print("   - message_date, found_at, notified")
    print("   - content_hash ← НОВОЕ ПОЛЕ для умной дедупликации")
    print()
    print("🎯 Умная дедупликация:")
    print("   ✅ Хеш создается из: author + price + location + text")
    print("   ✅ БЕЗ учета даты работы")
    print("   ✅ Временное окно: 24 часа")
    print("   ✅ Если дата работы изменилась → НОВОЕ объявление")
    print()


if __name__ == "__main__":
    asyncio.run(migrate())
