"""
Реальный тест мониторинга с настоящими Telegram чатами
"""
import requests
import json
import time
from datetime import date, timedelta

API_URL = "http://localhost:8002"

# Данные для запроса
REQUEST_DATA = {
    "user_id": 338908929,
    "mode": "worker",  # Ищем работников
    "chats": ["@Rabota_Zamena_PVZ", "@pvz_zamena"],
    "filters": {
        "date_from": date.today().isoformat(),
        "date_to": (date.today() + timedelta(days=10)).isoformat(),
        "min_price": 1500,
        "max_price": 6000,
        "shk_filter": "любое"
    },
    "api_id": 31834015,
    "api_hash": "7435e77a18431b9d3ac0c3bc7ee8bfc8",
    "notification_bot_token": "8374925023:AAG9QwKAfXnwDZ4ZvtjP6zaoaqkEXeZn6p8",
    "notification_chat_id": 338908929,
    "parse_history_days": 3  # Парсим последние 3 дня
}


def test_monitoring():
    """Тест мониторинга с реальными чатами"""
    print("=" * 80)
    print("ТЕСТ МОНИТОРИНГА С РЕАЛЬНЫМИ TELEGRAM ЧАТАМИ")
    print("=" * 80)
    print()

    # 1. Проверка доступности API
    print("1️⃣  Проверка API...")
    try:
        response = requests.get(f"{API_URL}/", timeout=5)
        if response.status_code == 200:
            print("   ✅ API доступен")
            print(f"   📊 {response.json()}")
        else:
            print(f"   ❌ API недоступен (код {response.status_code})")
            return
    except Exception as e:
        print(f"   ❌ Ошибка подключения: {e}")
        print("   💡 Запустите API: python api.py")
        return

    print()

    # 2. Запуск мониторинга
    print("2️⃣  Запуск мониторинга...")
    print(f"   📋 Режим: {REQUEST_DATA['mode']}")
    print(f"   💬 Чаты: {REQUEST_DATA['chats']}")
    print(f"   📅 Период: {REQUEST_DATA['filters']['date_from']} - {REQUEST_DATA['filters']['date_to']}")
    print(f"   💰 Цена: {REQUEST_DATA['filters']['min_price']} - {REQUEST_DATA['filters']['max_price']}")
    print()

    try:
        response = requests.post(
            f"{API_URL}/workers/start",
            json=REQUEST_DATA,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            task_id = result['task_id']
            print(f"   ✅ Мониторинг запущен!")
            print(f"   🆔 Task ID: {task_id}")
            print(f"   📍 Статус: {result['status']}")
            print(f"   ⏰ Запущен: {result['started_at']}")
        else:
            print(f"   ❌ Ошибка запуска (код {response.status_code})")
            print(f"   📄 Ответ: {response.text}")
            return

    except Exception as e:
        print(f"   ❌ Ошибка запроса: {e}")
        return

    print()

    # 3. Мониторинг статуса (30 секунд)
    print("3️⃣  Мониторинг статуса (следим 30 секунд)...")
    print()

    for i in range(6):
        time.sleep(5)

        try:
            response = requests.get(f"{API_URL}/workers/status/{task_id}", timeout=5)

            if response.status_code == 200:
                status_data = response.json()
                stats = status_data['stats']

                print(f"   ⏱️  [{i*5+5}с] Статус: {status_data['status']}")
                print(f"       📨 Сообщений просканировано: {stats['total_messages_scanned']}")
                print(f"       🎯 Объявлений найдено: {stats['items_found']}")
                print(f"       📬 Уведомлений отправлено: {stats['notifications_sent']}")
                print(f"       🕐 Обновлено: {stats['last_update']}")
                print()

            else:
                print(f"   ⚠️  Не удалось получить статус (код {response.status_code})")

        except Exception as e:
            print(f"   ⚠️  Ошибка получения статуса: {e}")

    # 4. Получение найденных объявлений
    print("4️⃣  Получение найденных объявлений...")
    print()

    try:
        response = requests.get(f"{API_URL}/workers/list/{task_id}?limit=10", timeout=5)

        if response.status_code == 200:
            items_data = response.json()
            total = items_data['total']
            items = items_data['items']

            print(f"   📊 Всего найдено: {total}")
            print()

            if items:
                print("   📋 Последние найденные объявления:")
                print()

                for idx, item in enumerate(items[:5], 1):
                    print(f"   {idx}. 💰 {item['price']} руб | 📅 {item['date']}")
                    if item.get('author_username'):
                        print(f"      👤 @{item['author_username']}")
                    if item.get('shk'):
                        print(f"      📦 ШК: {item['shk']}")
                    if item.get('location'):
                        print(f"      📍 {item['location']}")
                    print(f"      🔗 {item['message_link']}")
                    print()
            else:
                print("   ℹ️  Пока ничего не найдено")
                print("   💡 Попробуйте расширить фильтры или подождать дольше")

        else:
            print(f"   ⚠️  Не удалось получить список (код {response.status_code})")

    except Exception as e:
        print(f"   ⚠️  Ошибка получения списка: {e}")

    print()

    # 5. Остановка мониторинга
    print("5️⃣  Остановка мониторинга...")

    try:
        response = requests.post(f"{API_URL}/workers/stop/{task_id}", timeout=5)

        if response.status_code == 200:
            stop_data = response.json()
            print(f"   ✅ Мониторинг остановлен")
            print(f"   📍 Статус: {stop_data['status']}")
        else:
            print(f"   ⚠️  Не удалось остановить (код {response.status_code})")

    except Exception as e:
        print(f"   ⚠️  Ошибка остановки: {e}")

    print()
    print("=" * 80)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 80)
    print()
    print("💡 Проверьте:")
    print("   1. Telegram бот (@ваш_бот) - должны прийти уведомления")
    print("   2. База данных workers.db - должны быть записи")
    print("   3. Логи workers_service.log - должна быть активность")
    print()


if __name__ == "__main__":
    test_monitoring()
