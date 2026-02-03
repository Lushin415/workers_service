"""
Скрипт для проверки доступа к Telegram чатам
"""
import asyncio
from pyrogram import Client
from loguru import logger

# Credentials
API_ID = 31834015
API_HASH = "7435e77a18431b9d3ac0c3bc7ee8bfc8"

# Чаты для проверки (разные форматы)
CHATS_TO_CHECK = [
    "Rabota_Zamena_PVZ",
    "pvz_zamena",
    "@Rabota_Zamena_PVZ",
    "@pvz_zamena",
    "https://t.me/Rabota_Zamena_PVZ",
    "t.me/pvz_zamena"
]


async def check_chat_access():
    """Проверить доступ к чатам"""
    print("=" * 70)
    print("ПРОВЕРКА ДОСТУПА К TELEGRAM ЧАТАМ")
    print("=" * 70)
    print()

    # Создаем клиент (используем существующую сессию)
    client = Client(
        name="workers_session",
        api_id=API_ID,
        api_hash=API_HASH,
        workdir="."
    )

    try:
        print("🔄 Запуск Pyrogram клиента...")
        await client.start()
        print("✅ Клиент запущен\n")

        # Проверяем каждый чат
        successful_chats = []

        for chat_name in CHATS_TO_CHECK:
            print(f"🔍 Проверка: {chat_name}")

            try:
                # Пытаемся получить информацию о чате
                chat = await client.get_chat(chat_name)

                print(f"   ✅ Доступен!")
                print(f"   📌 ID: {chat.id}")
                print(f"   📌 Тип: {chat.type}")
                print(f"   📌 Название: {chat.title or 'N/A'}")
                print(f"   📌 Username: @{chat.username}" if chat.username else "   📌 Username: Нет")
                print(f"   📌 Участников: {chat.members_count or 'N/A'}")

                # Пробуем получить несколько последних сообщений
                try:
                    messages = []
                    async for message in client.get_chat_history(chat.id, limit=3):
                        if message.text:
                            messages.append(message)

                    print(f"   📨 Последних сообщений получено: {len(messages)}")

                    if messages:
                        print(f"   📝 Последнее сообщение: {messages[0].text[:50]}...")

                        # Формат для API
                        api_format = f"@{chat.username}" if chat.username else str(chat.id)
                        successful_chats.append(api_format)
                        print(f"   🎯 Формат для API: {api_format}")

                except Exception as e:
                    print(f"   ⚠️  Не удалось получить сообщения: {e}")

                print()

            except Exception as e:
                print(f"   ❌ Ошибка: {e}")
                print()

        # Итоговый результат
        print("=" * 70)
        print("ИТОГОВЫЙ РЕЗУЛЬТАТ")
        print("=" * 70)

        if successful_chats:
            print(f"\n✅ Доступных чатов: {len(successful_chats)}")
            print("\n📋 Используйте эти форматы в API запросе:")
            print(f"   {successful_chats}")
            print("\nПример JSON для API:")
            print(f'''
{{
  "user_id": 338908929,
  "mode": "worker",
  "chats": {successful_chats},
  "filters": {{
    "date_from": "2026-02-02",
    "date_to": "2026-02-12",
    "min_price": 2000,
    "max_price": 5000,
    "shk_filter": "любое"
  }},
  "api_id": {API_ID},
  "api_hash": "{API_HASH}",
  "notification_bot_token": "8374925023:AAG9QwKAfXnwDZ4ZvtjP6zaoaqkEXeZn6p8",
  "notification_chat_id": 338908929,
  "parse_history_days": 7
}}
''')
        else:
            print("\n❌ Доступных чатов не найдено")
            print("\nВозможные причины:")
            print("1. Чаты приватные (требуется инвайт)")
            print("2. Неверные имена чатов")
            print("3. Аккаунт не состоит в этих чатах")

        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        logger.error(f"Ошибка проверки чатов: {e}")

    finally:
        print("\n🔄 Остановка клиента...")
        await client.stop()
        print("✅ Клиент остановлен")


if __name__ == "__main__":
    print("\n⚠️  ВАЖНО: При первом запуске потребуется авторизация в Telegram")
    print("   Введите номер телефона, код подтверждения и 2FA пароль (если есть)")
    print()

    asyncio.run(check_chat_access())
