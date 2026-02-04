"""
Тест умной дедупликации
"""
import asyncio
from datetime import datetime
from db_service import DBService
from models_db import FoundItem
from deduplicator import Deduplicator


async def test_deduplication():
    """Тест умной дедупликации"""
    print("=" * 80)
    print("ТЕСТ УМНОЙ ДЕДУПЛИКАЦИИ")
    print("=" * 80)
    print()

    db = DBService()
    await db.init_db()

    # Тестовые данные
    test_user = "@test_user"
    test_price = 3000
    test_location = "Москва"
    test_text = "Требуется сотрудник на ПВЗ Wildberries, опыт желателен"

    # Создаем хеш
    content_hash = Deduplicator.create_content_hash(
        author_username=test_user,
        price=test_price,
        location=test_location,
        message_text=test_text
    )

    print(f"📋 Тестовые данные:")
    print(f"   Автор: {test_user}")
    print(f"   Цена: {test_price}")
    print(f"   Локация: {test_location}")
    print(f"   Текст: {test_text[:60]}...")
    print(f"   Хеш: {content_hash[:16]}...")
    print()

    # Тест 1: Первое добавление
    print("1️⃣  Тест: Добавление первого объявления (дата работы: 2026-02-05)")
    item1 = FoundItem(
        id=None,
        task_id="test-task-1",
        mode="employer",
        author_username=test_user,
        author_full_name="Test User",
        date="2026-02-05",  # Дата работы
        price=test_price,
        shk="300",
        location=test_location,
        message_text=test_text,
        message_link="https://t.me/test/1",
        chat_name="@test_chat",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash
    )

    result1 = await db.add_found_item(item1)
    if result1:
        print(f"   ✅ Добавлено (ID: {result1})")
    else:
        print(f"   ❌ Не добавлено")
    print()

    # Тест 2: Дубликат (тот же текст, та же дата работы)
    print("2️⃣  Тест: Дубликат (тот же текст, та же дата: 2026-02-05)")
    item2 = FoundItem(
        id=None,
        task_id="test-task-1",
        mode="employer",
        author_username=test_user,
        author_full_name="Test User",
        date="2026-02-05",  # ТА ЖЕ дата работы
        price=test_price,
        shk="300",
        location=test_location,
        message_text=test_text,
        message_link="https://t.me/test/2",  # Другой message_link!
        chat_name="@test_chat",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash
    )

    result2 = await db.add_found_item(item2)
    if result2:
        print(f"   ❌ ОШИБКА: Дубликат добавлен (ID: {result2})")
    else:
        print(f"   ✅ Дубликат правильно отклонен")
    print()

    # Тест 3: Обновление (тот же текст, но ДРУГАЯ дата работы)
    print("3️⃣  Тест: Обновление (тот же текст, но новая дата: 2026-02-10)")
    item3 = FoundItem(
        id=None,
        task_id="test-task-1",
        mode="employer",
        author_username=test_user,
        author_full_name="Test User",
        date="2026-02-10",  # ДРУГАЯ дата работы!
        price=test_price,
        shk="300",
        location=test_location,
        message_text=test_text,
        message_link="https://t.me/test/3",
        chat_name="@test_chat",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash
    )

    result3 = await db.add_found_item(item3)
    if result3:
        print(f"   ✅ Обновление добавлено (ID: {result3}) - дата изменилась!")
    else:
        print(f"   ❌ ОШИБКА: Обновление отклонено")
    print()

    # Тест 4: Другой автор, но тот же текст
    print("4️⃣  Тест: Другой автор (@another_user), тот же текст")
    content_hash_another = Deduplicator.create_content_hash(
        author_username="@another_user",  # Другой автор
        price=test_price,
        location=test_location,
        message_text=test_text
    )

    item4 = FoundItem(
        id=None,
        task_id="test-task-1",
        mode="employer",
        author_username="@another_user",  # Другой автор!
        author_full_name="Another User",
        date="2026-02-05",
        price=test_price,
        shk="300",
        location=test_location,
        message_text=test_text,
        message_link="https://t.me/test/4",
        chat_name="@test_chat",
        message_date=datetime.now().isoformat(),
        found_at=datetime.utcnow().isoformat(),
        notified=False,
        content_hash=content_hash_another
    )

    result4 = await db.add_found_item(item4)
    if result4:
        print(f"   ✅ Добавлено (ID: {result4}) - другой автор")
    else:
        print(f"   ❌ ОШИБКА: Не добавлено")
    print()

    # Проверка результатов
    print("=" * 80)
    print("ПРОВЕРКА РЕЗУЛЬТАТОВ")
    print("=" * 80)
    print()

    items = await db.get_found_items("test-task-1", limit=10)
    print(f"📊 Всего записей в БД: {len(items)}")
    print()

    for idx, item in enumerate(items, 1):
        print(f"{idx}. Автор: {item.author_username}, Дата работы: {item.date}")
        print(f"   Ссылка: {item.message_link}")
    print()

    print("=" * 80)
    print("ОЖИДАЕМЫЙ РЕЗУЛЬТАТ")
    print("=" * 80)
    print()
    print("✅ Должно быть 3 записи:")
    print("   1. @test_user, дата: 2026-02-05 (первое добавление)")
    print("   2. @test_user, дата: 2026-02-10 (обновление - дата изменилась)")
    print("   3. @another_user, дата: 2026-02-05 (другой автор)")
    print()
    print("❌ НЕ должно быть:")
    print("   - Дубликата @test_user с датой 2026-02-05 (тест 2)")
    print()

    if len(items) == 3:
        print("🎉 ТЕСТ ПРОЙДЕН! Умная дедупликация работает корректно!")
    else:
        print(f"⚠️  ТЕСТ НЕ ПРОЙДЕН! Ожидалось 3 записи, получено {len(items)}")

    print()


if __name__ == "__main__":
    asyncio.run(test_deduplication())
