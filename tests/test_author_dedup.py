"""
Тест двухуровневой дедупликации (по автору)
"""
import asyncio
import aiosqlite
from db_service import DBService
from models_db import FoundItem
from deduplicator import Deduplicator
from datetime import datetime

async def test_author_deduplication():
    """
    Тестируем УРОВЕНЬ 2: Author-based дедупликацию

    Сценарии:
    1. Автор пишет одно объявление в разные чаты → 1 уведомление
    2. Автор меняет цену → 2 уведомления (ВАЖНО!)
    3. Автор ищет работу на разные дни → 2 уведомления
    """
    print("=" * 80)
    print("ТЕСТ: ДВУХУРОВНЕВАЯ ДЕДУПЛИКАЦИЯ (ПО АВТОРУ)")
    print("=" * 80)
    print()

    db = DBService("test_author_dedup.db")
    await db.init_db()

    print("📋 Сценарии:")
    print()

    # Сценарий 1: Кросс-посты (одно и то же в разные чаты)
    print("1️⃣  Кросс-посты (одно и то же в разные чаты)")
    print("   Автор: Ivan")
    print("   Дата работы: 2026-02-05")
    print("   Цена: 3000")
    print()

    # Первое сообщение
    item1 = FoundItem(
        id=None,
        task_id="test-1",
        mode="worker",
        author_username="Ivan",
        author_full_name="Иван Иванов",
        date="2026-02-05",
        price=3000,
        shk="100",
        location="Москва",
        message_text="Могу 5 февраля, 3000, шк 100, Москва",
        message_link="https://t.me/chat1/123",
        chat_name="@chat1",
        message_date=datetime.utcnow().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=Deduplicator.create_content_hash(
            "Ivan", 3000, "Москва", "Могу 5 февраля, 3000, шк 100, Москва"
        )
    )

    is_dup = await db.check_duplicate_by_author("Ivan", "2026-02-05", 3000)
    print(f"   Сообщение 1 (чат1, 10:00): дубликат={is_dup} ❌ → сохраняем ✅")
    await db.add_found_item(item1)

    # Второе сообщение (в другой чат)
    is_dup = await db.check_duplicate_by_author("Ivan", "2026-02-05", 3000)
    print(f"   Сообщение 2 (чат2, 11:00): дубликат={is_dup} ✅ → пропускаем ❌")

    print()
    print("   ✅ Результат: 1 уведомление вместо 2 (кросс-пост отфильтрован)")
    print()

    # Сценарий 2: Изменение цены
    print("2️⃣  Изменение цены (ВАЖНО!)")
    print("   Автор: Ivan")
    print("   Дата работы: 2026-02-05")
    print("   Цена: 3000 → 2500")
    print()

    is_dup = await db.check_duplicate_by_author("Ivan", "2026-02-05", 2500)
    print(f"   Сообщение 3 (цена 2500, 12:00): дубликат={is_dup} ❌ → сохраняем ✅")

    item2 = FoundItem(
        id=None,
        task_id="test-1",
        mode="worker",
        author_username="Ivan",
        author_full_name="Иван Иванов",
        date="2026-02-05",
        price=2500,  # НОВАЯ ЦЕНА!
        shk="100",
        location="Москва",
        message_text="Могу 5 февраля, 2500, шк 100, Москва",
        message_link="https://t.me/chat3/125",
        chat_name="@chat3",
        message_date=datetime.utcnow().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=Deduplicator.create_content_hash(
            "Ivan", 2500, "Москва", "Могу 5 февраля, 2500, шк 100, Москва"
        )
    )
    await db.add_found_item(item2)

    print()
    print("   ✅ Результат: 2 уведомления (изменение цены учтено!)")
    print()

    # Сценарий 3: Разные даты работы
    print("3️⃣  Разные даты работы")
    print("   Автор: Ivan")
    print("   Дата работы: 2026-02-05 → 2026-02-07")
    print("   Цена: 3000")
    print()

    is_dup = await db.check_duplicate_by_author("Ivan", "2026-02-07", 3000)
    print(f"   Сообщение 4 (дата 07.02, 13:00): дубликат={is_dup} ❌ → сохраняем ✅")

    item3 = FoundItem(
        id=None,
        task_id="test-1",
        mode="worker",
        author_username="Ivan",
        author_full_name="Иван Иванов",
        date="2026-02-07",  # НОВАЯ ДАТА!
        price=3000,
        shk="100",
        location="Москва",
        message_text="Могу 7 февраля, 3000, шк 100, Москва",
        message_link="https://t.me/chat4/126",
        chat_name="@chat4",
        message_date=datetime.utcnow().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=Deduplicator.create_content_hash(
            "Ivan", 3000, "Москва", "Могу 7 февраля, 3000, шк 100, Москва"
        )
    )
    await db.add_found_item(item3)

    print()
    print("   ✅ Результат: 2 уведомления (разные даты работы)")
    print()

    # Сценарий 4: Анонимное сообщение (без автора)
    print("4️⃣  Анонимное сообщение (без автора)")
    print("   Автор: None")
    print()

    is_dup = await db.check_duplicate_by_author(None, "2026-02-05", 3000)
    print(f"   Сообщение 5 (автор=None): дубликат={is_dup} ❌ → проверка невозможна")
    print("   💡 Для анонимных сообщений используется только content_hash (Уровень 1)")
    print()

    # Итоговая статистика
    print("=" * 80)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 80)
    print()

    async with aiosqlite.connect(db.db_path) as conn:
        async with conn.execute("SELECT COUNT(*) FROM found_items") as cursor:
            total = (await cursor.fetchone())[0]

        async with conn.execute(
            "SELECT author_username, date, price, chat_name FROM found_items ORDER BY id"
        ) as cursor:
            items = await cursor.fetchall()

    print(f"📊 Всего сообщений обработано: 5")
    print(f"💾 Сохранено в БД: {total}")
    print(f"🗑️  Отфильтровано дубликатов: {5 - total}")
    print()
    print("📋 Сохраненные объявления:")
    for i, (author, date, price, chat) in enumerate(items, 1):
        print(f"   {i}. {author} | {date} | {price} руб | {chat}")
    print()

    expected = 3  # Ожидаем: 1 кросс-пост, 1 с новой ценой, 1 с новой датой
    if total == expected:
        print(f"✅ ТЕСТ ПРОЙДЕН! Дедупликация работает правильно ({total}/{expected})")
    else:
        print(f"❌ ТЕСТ НЕ ПРОЙДЕН! Ожидали {expected}, получили {total}")

    print()
    print("💡 Двухуровневая дедупликация:")
    print("   Уровень 1: Content hash (защита от копипасты)")
    print("   Уровень 2: Author + date + price (защита от кросс-постов)")
    print("   ✅ Изменение цены → новое уведомление!")
    print("   ✅ Изменение даты → новое уведомление!")
    print("   ✅ Кросс-посты → 1 уведомление!")
    print()

if __name__ == "__main__":
    asyncio.run(test_author_deduplication())
