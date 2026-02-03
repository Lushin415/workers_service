"""
Скрипт для авторизации в Telegram (первый запуск)
Запустите этот скрипт в терминале: python auth_telegram.py
"""
import asyncio
from pyrogram import Client

# Ваши credentials
API_ID = 31834015
API_HASH = "7435e77a18431b9d3ac0c3bc7ee8bfc8"


async def authorize():
    """Авторизация в Telegram"""
    print("=" * 70)
    print("АВТОРИЗАЦИЯ В TELEGRAM")
    print("=" * 70)
    print("\nСоздается сессия для доступа к Telegram API...")
    print()

    # Создаем клиент
    client = Client(
        name="workers_session",
        api_id=API_ID,
        api_hash=API_HASH,
        workdir="."
    )

    try:
        print("Pyrogram запросит:")
        print("1. Ваш номер телефона (в международном формате, например: +79991234567)")
        print("2. Код подтверждения из Telegram")
        print("3. 2FA пароль (если включен)")
        print()
        print("-" * 70)

        # Запускаем клиент (авторизация произойдет автоматически)
        await client.start()

        print("-" * 70)
        print()
        print("✅ Авторизация успешна!")
        print()

        # Получаем информацию о текущем пользователе
        me = await client.get_me()
        print(f"Авторизован как: {me.first_name} {me.last_name or ''}")
        print(f"Username: @{me.username}" if me.username else "Username: не установлен")
        print(f"User ID: {me.id}")
        print(f"Номер телефона: {me.phone_number}")
        print()

        # Останавливаем клиент
        await client.stop()

        print("=" * 70)
        print("Session файл создан: workers_session.session")
        print("Теперь можно запустить test_chats.py для проверки доступа к чатам")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Ошибка авторизации: {e}")
        await client.stop()


if __name__ == "__main__":
    asyncio.run(authorize())
