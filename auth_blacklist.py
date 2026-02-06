"""
Авторизация Telegram для сессии поиска в черном списке
"""
import asyncio
from pyrogram import Client
from config import config

async def main():
    print("=== Авторизация сессии для поиска в ЧС ===")
    print(f"Сессия: {config.BLACKLIST_SESSION_PATH}")
    print()
    
    client = Client(
        name=config.BLACKLIST_SESSION_PATH,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        workdir="."
    )
    
    await client.start()
    me = await client.get_me()
    print(f"Авторизован как: {me.first_name} (@{me.username})")
    await client.stop()
    print("Сессия создана успешно!")

if __name__ == "__main__":
    asyncio.run(main())
