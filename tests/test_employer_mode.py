"""
Тест режима EMPLOYER - поиск работодателей (ищут сотрудников)
"""
import requests
import json
import time
from datetime import date, timedelta

API_URL = "http://localhost:8002"

# Данные для запроса - РЕЖИМ EMPLOYER
REQUEST_DATA = {
    "user_id": 338908929,
    "mode": "employer",  # ⚠️ ИЩЕМ РАБОТОДАТЕЛЕЙ
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
    "parse_history_days": 3
}


def test_employer_mode():
    """Тест режима EMPLOYER"""
    print("=" * 80)
    print("ТЕСТ РЕЖИМА EMPLOYER - ПОИСК РАБОТОДАТЕЛЕЙ")
    print("=" * 80)
    print()
    print("🏢 Режим: EMPLOYER (ищем работодателей)")
    print("🔍 Ключевые слова: 'Ищу сотрудника', 'Нужен работник', 'Требуется'")
    print("📬 Формат уведомления: '🏢 Новая вакансия!' с кнопкой [Связаться]")
    print()

    # 1. Проверка API
    try:
        response = requests.get(f"{API_URL}/", timeout=5)
        if response.status_code != 200:
            print("❌ API недоступен. Запустите: python api.py")
            return
        print("✅ API доступен")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        print("💡 Запустите API: python api.py")
        return

    print()

    # 2. Запуск мониторинга
    print("🚀 Запуск мониторинга работодателей...")
    try:
        response = requests.post(
            f"{API_URL}/workers/start",
            json=REQUEST_DATA,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            task_id = result['task_id']
            print(f"✅ Мониторинг запущен!")
            print(f"🆔 Task ID: {task_id}")
            print(f"📍 Режим: {REQUEST_DATA['mode']}")
        else:
            print(f"❌ Ошибка запуска: {response.text}")
            return

    except Exception as e:
        print(f"❌ Ошибка запроса: {e}")
        return

    print()
    print("⏳ Мониторим 20 секунд...")
    print()

    # 3. Мониторинг (20 секунд)
    for i in range(4):
        time.sleep(5)

        try:
            response = requests.get(f"{API_URL}/workers/status/{task_id}", timeout=5)

            if response.status_code == 200:
                status_data = response.json()
                stats = status_data['stats']

                print(f"[{i*5+5}с] 📨 Сообщений: {stats['total_messages_scanned']} | "
                      f"🏢 Вакансий найдено: {stats['items_found']} | "
                      f"📬 Уведомлений: {stats['notifications_sent']}")

        except Exception as e:
            print(f"⚠️ Ошибка получения статуса: {e}")

    print()

    # 4. Получение найденных вакансий
    print("📋 Найденные вакансии от работодателей:")
    print()

    try:
        response = requests.get(f"{API_URL}/workers/list/{task_id}?limit=10", timeout=5)

        if response.status_code == 200:
            items_data = response.json()
            total = items_data['total']
            items = items_data['items']

            print(f"📊 Всего найдено: {total}")
            print()

            if items:
                for idx, item in enumerate(items[:5], 1):
                    print(f"{idx}. 🏢 Вакансия")
                    print(f"   💰 Оплата: {item['price']} руб")
                    print(f"   📅 Дата: {item['date']}")
                    if item.get('author_username'):
                        print(f"   👤 @{item['author_username']}")
                    if item.get('location'):
                        print(f"   📍 {item['location']}")
                    print(f"   🔗 {item['message_link']}")
                    print()
            else:
                print("ℹ️  Вакансий не найдено за последние 3 дня")
                print()
                print("💡 Возможные причины:")
                print("   - В чатах больше работников, чем работодателей")
                print("   - Работодатели используют другие ключевые слова")
                print("   - Попробуйте увеличить parse_history_days")

        else:
            print(f"⚠️ Не удалось получить список (код {response.status_code})")

    except Exception as e:
        print(f"⚠️ Ошибка получения списка: {e}")

    print()

    # 5. Остановка
    print("🛑 Остановка мониторинга...")
    try:
        response = requests.post(f"{API_URL}/workers/stop/{task_id}", timeout=5)
        if response.status_code == 200:
            print("✅ Мониторинг остановлен")
    except Exception as e:
        print(f"⚠️ Ошибка остановки: {e}")

    print()
    print("=" * 80)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 80)
    print()
    print("📝 Сравните результаты:")
    print("   - Mode: worker → найдено 31 объявление (работники)")
    print(f"   - Mode: employer → найдено {total if 'total' in locals() else '?'} вакансий (работодатели)")
    print()


if __name__ == "__main__":
    test_employer_mode()
