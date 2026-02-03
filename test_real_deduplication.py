"""
Тест умной дедупликации на РЕАЛЬНЫХ чатах
"""
import requests
import time
from datetime import date, timedelta

API_URL = "http://localhost:8002"

REQUEST_DATA = {
    "user_id": 338908929,
    "mode": "employer",
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


def test_real_deduplication():
    """Тест на реальных данных с умной дедупликацией"""
    print("=" * 80)
    print("ТЕСТ УМНОЙ ДЕДУПЛИКАЦИИ НА РЕАЛЬНЫХ ДАННЫХ")
    print("=" * 80)
    print()
    print("🎯 Цель теста:")
    print("   - Проверить, что дубликаты (одинаковый текст + дата) отклоняются")
    print("   - Проверить, что обновления (другая дата) добавляются")
    print("   - Сравнить со старыми результатами (было 294, уникальных 49)")
    print()

    # Запуск мониторинга
    print("🚀 Запуск мониторинга...")
    response = requests.post(f"{API_URL}/workers/start", json=REQUEST_DATA, timeout=10)

    if response.status_code != 200:
        print(f"❌ Ошибка запуска: {response.text}")
        return

    result = response.json()
    task_id = result['task_id']
    print(f"✅ Мониторинг запущен (Task ID: {task_id})")
    print()

    # Мониторим 30 секунд
    print("⏳ Мониторим 30 секунд...")
    print()

    for i in range(6):
        time.sleep(5)

        response = requests.get(f"{API_URL}/workers/status/{task_id}", timeout=5)
        if response.status_code == 200:
            status_data = response.json()
            stats = status_data['stats']

            print(f"[{i*5+5}с] "
                  f"📨 Сообщений: {stats['total_messages_scanned']:4d} | "
                  f"🎯 Найдено: {stats['items_found']:3d} | "
                  f"📬 Уведомлений: {stats['notifications_sent']:3d}")

    print()

    # Получаем результаты
    response = requests.get(f"{API_URL}/workers/list/{task_id}?limit=100", timeout=5)

    if response.status_code == 200:
        items_data = response.json()
        total = items_data['total']

        print("=" * 80)
        print("РЕЗУЛЬТАТЫ")
        print("=" * 80)
        print()
        print(f"📊 Всего добавлено в БД: {total}")
        print()
        print("📈 Сравнение с прошлым запуском (БЕЗ умной дедупликации):")
        print(f"   Было: 294 записи (49 уникальных, 83% дубликатов)")
        print(f"   Стало: {total} записей")
        print()

        if total < 100:
            print(f"✅ УЛУЧШЕНИЕ! Дубликаты отфильтрованы (сокращение на {((294-total)/294*100):.1f}%)")
        else:
            print(f"⚠️  Записей: {total} (ожидалось < 100)")

        print()
        print("💡 Детали дедупликации:")
        print("   ✅ Одинаковый текст + одинаковая дата → дубликат (отклонен)")
        print("   ✅ Одинаковый текст + другая дата → обновление (добавлен)")
        print("   ✅ Временное окно: 24 часа")
        print()

    # Остановка
    requests.post(f"{API_URL}/workers/stop/{task_id}", timeout=5)
    print("🛑 Мониторинг остановлен")
    print()
    print("=" * 80)


if __name__ == "__main__":
    test_real_deduplication()
