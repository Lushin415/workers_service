"""
Тест получения топиков из форума @pvz_zamena
"""
import asyncio
from pyrogram import Client
from pyrogram.raw.functions.channels import GetForumTopics
from pyrogram.raw.types import InputPeerChannel

# Конфигурация из вашего примера
API_ID = 31834015
API_HASH = "7435e77a18431b9d3ac0c3bc7ee8bfc8"
GROUP_USERNAME = "pvz_zamena"

async def test_forum_topics():
    """Тестирование получения топиков"""

    # Используем существующую сессию
    app = Client(
        "workers_parser",  # Используем ту же сессию что и в основном парсере
        api_id=API_ID,
        api_hash=API_HASH,
        workdir="."
    )

    async with app:
        print(f"\n{'='*60}")
        print(f"🔍 Тест получения топиков из {GROUP_USERNAME}")
        print(f"{'='*60}\n")

        # Получаем информацию о чате
        chat = await app.get_chat(GROUP_USERNAME)

        print(f"📊 Информация о чате:")
        print(f"   Chat ID: {chat.id}")
        print(f"   Title: {chat.title}")
        print(f"   Type: {chat.type}")
        print(f"   Is forum: {getattr(chat, 'is_forum', 'N/A')}")
        print()

        # Получаем peer
        peer = await app.resolve_peer(chat.id)

        # Проверяем тип
        if not isinstance(peer, InputPeerChannel):
            print(f"❌ Ошибка: чат не является channel/supergroup")
            print(f"   Тип: {type(peer).__name__}")
            return

        print(f"✅ Peer type: {type(peer).__name__}")
        print()

        # Вызываем GetForumTopics
        print(f"🔄 Запрашиваем топики через GetForumTopics...")

        result = await app.invoke(
            GetForumTopics(
                channel=peer,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            )
        )

        print(f"   Result type: {type(result).__name__}")
        print()

        # Выводим топики
        if hasattr(result, 'topics'):
            topics_count = len(result.topics)
            print(f"📋 Найдено топиков: {topics_count}")
            print()

            if topics_count > 0:
                print(f"{'ID':<10} {'НАЗВАНИЕ'}")
                print(f"{'-'*60}")

                for topic in result.topics:
                    print(f"{topic.id:<10} {topic.title}")

                print()
                print(f"✅ Успешно загружено {topics_count} топиков!")
            else:
                print(f"⚠️  Топики не найдены")
        else:
            print(f"❌ Result не содержит 'topics'")
            print(f"   Атрибуты result: {dir(result)}")

if __name__ == "__main__":
    asyncio.run(test_forum_topics())
