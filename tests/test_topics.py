"""
Тест получения топиков через forum_topic_created.name
"""
import asyncio
from parser import TelegramParser
from loguru import logger

async def test_get_topics():
    """Тест получения топиков"""

    API_ID = 31834015
    API_HASH = "7435e77a18431b9d3ac0c3bc7ee8bfc8"

    print("=" * 80)
    print("ТЕСТ: Получение топиков через forum_topic_created.name")
    print("=" * 80)
    print()

    parser = TelegramParser(api_id=API_ID, api_hash=API_HASH, session_name="workers_session")

    try:
        await parser.start()
        print("✅ Pyrogram клиент запущен")
        print()

        # Получаем топики
        chat = "@pvz_zamena"
        print(f"🔍 Сканируем топики в {chat}...")
        print()

        topics = await parser.get_forum_topics(chat)

        print()
        print("=" * 80)
        print("РЕЗУЛЬТАТЫ")
        print("=" * 80)
        print()

        if topics:
            print(f"✅ Найдено {len(topics)} топиков:")
            print()
            for topic_id, topic_name in topics.items():
                print(f"  📌 ID: {topic_id}")
                print(f"     Название: {topic_name}")
                print()
        else:
            print("❌ Топики не найдены")
            print()
            print("Возможные причины:")
            print("  1. Чат не является форумом")
            print("  2. Нет доступа к истории чата")
            print("  3. forum_topic_created недоступен")
            print()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await parser.stop()
        print("Pyrogram клиент остановлен")

if __name__ == "__main__":
    asyncio.run(test_get_topics())
