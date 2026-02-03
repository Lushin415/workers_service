"""
Тест дедупликации кросс-постинга (одно объявление в разные группы/чаты)
"""
import asyncio
from datetime import datetime
from db_service import DBService
from models_db import FoundItem
from deduplicator import Deduplicator


async def test_cross_post_deduplication():
    """Тест дедупликации при кросс-постинге"""
    print("=" * 80)
    print("ТЕСТ ДЕДУПЛИКАЦИИ КРОСС-ПОСТИНГА")
    print("=" * 80)
    print()
    print("📋 Сценарий:")
    print("   Работодатель отправил одно объявление в РАЗНЫЕ группы/чаты")
    print("   В одной группе указан author, в другой - нет")
    print()

    db = DBService()
    await db.init_db()

    test_text = "#озон #красногорск\n\nНа пункт выдачи OZON требуется сотрудник"
    test_price = 2600
    test_location = "Красногорск"

    # Хеш БЕЗ учета автора
    content_hash = Deduplicator.create_content_hash(
        author_username=None,  # Не используется
        price=test_price,
        location=test_location,
        message_text=test_text
    )

    print(f"🔑 Хеш содержимого (БЕЗ автора): {content_hash[:16]}...")
    print()

    # Тест 1: Сообщение с автором в группе 1
    print("1️⃣  Группа 1: Сообщение С автором (@T_Alesya96)")
    item1 = FoundItem(
        id=None,
        task_id="test-cross-post",
        mode="employer",
        author_username="T_Alesya96",  # ЕСТЬ автор
        author_full_name="Alesya",
        date="2026-02-05",
        price=test_price,
        shk="300",
        location=test_location,
        message_text=test_text,
        message_link="https://t.me/Rabota_Zamena_PVZ/999991",  # Группа 1 (тестовый ID)
        chat_name="@Rabota_Zamena_PVZ",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash  # Одинаковый хеш!
    )

    result1 = await db.add_found_item(item1)
    if result1:
        print(f"   ✅ Добавлено (ID: {result1})")
    else:
        print(f"   ❌ Не добавлено")
    print()

    # Тест 2: То же сообщение БЕЗ автора в группе 2
    print("2️⃣  Группа 2: То же сообщение БЕЗ автора (author=None)")
    item2 = FoundItem(
        id=None,
        task_id="test-cross-post",
        mode="employer",
        author_username=None,  # НЕТ автора!
        author_full_name=None,
        date="2026-02-05",  # Та же дата работы!
        price=test_price,  # Та же цена!
        shk="300",
        location=test_location,  # Та же локация!
        message_text=test_text,  # Тот же текст!
        message_link="https://t.me/Rabota_Zamena_PVZ/999992",  # Группа 2 (тестовый ID)
        chat_name="@Rabota_Zamena_PVZ",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash  # ОДИНАКОВЫЙ хеш!
    )

    result2 = await db.add_found_item(item2)
    if result2:
        print(f"   ❌ ОШИБКА: Кросс-пост добавлен (ID: {result2})")
        print(f"   💡 Должен быть отклонен как дубликат!")
    else:
        print(f"   ✅ Кросс-пост правильно отклонен как дубликат")
    print()

    # Тест 3: То же сообщение в другом чате
    print("3️⃣  Другой чат (@pvz_zamena): То же объявление")
    item3 = FoundItem(
        id=None,
        task_id="test-cross-post",
        mode="employer",
        author_username="AnotherUser",  # Другой автор
        author_full_name="Another",
        date="2026-02-05",  # Та же дата!
        price=test_price,  # Та же цена!
        shk="300",
        location=test_location,  # Та же локация!
        message_text=test_text,  # Тот же текст!
        message_link="https://t.me/pvz_zamena/999993",  # Другой чат (тестовый ID)
        chat_name="@pvz_zamena",  # Другой чат!
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash  # ОДИНАКОВЫЙ хеш!
    )

    result3 = await db.add_found_item(item3)
    if result3:
        print(f"   ❌ ОШИБКА: Кросс-пост в другом чате добавлен (ID: {result3})")
        print(f"   💡 Должен быть отклонен как дубликат!")
    else:
        print(f"   ✅ Кросс-пост правильно отклонен как дубликат")
    print()

    # Проверка результатов
    print("=" * 80)
    print("ПРОВЕРКА РЕЗУЛЬТАТОВ")
    print("=" * 80)
    print()

    items = await db.get_found_items("test-cross-post", limit=10)
    print(f"📊 Всего записей в БД: {len(items)}")
    print()

    for idx, item in enumerate(items, 1):
        print(f"{idx}. Автор: {item.author_username or 'None'}, "
              f"Чат: {item.chat_name}, "
              f"Ссылка: {item.message_link}")
    print()

    print("=" * 80)
    print("ОЖИДАЕМЫЙ РЕЗУЛЬТАТ")
    print("=" * 80)
    print()
    print("✅ Должна быть 1 запись:")
    print("   - Первое сообщение из группы 1")
    print()
    print("❌ НЕ должно быть:")
    print("   - Дубликата из группы 2 (то же объявление без автора)")
    print("   - Дубликата из другого чата (то же объявление с другим автором)")
    print()

    if len(items) == 1:
        print("🎉 ТЕСТ ПРОЙДЕН! Кросс-пост дедупликация работает!")
    else:
        print(f"⚠️  ТЕСТ НЕ ПРОЙДЕН! Ожидалась 1 запись, получено {len(items)}")

    print()


if __name__ == "__main__":
    asyncio.run(test_cross_post_deduplication())
