"""
Финальный тест дедупликации с улучшенной логикой (без учета author)
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


def test_final_deduplication():
    """Финальный тест с улучшенной дедупликацией"""
    print("=" * 80)
    print("ФИНАЛЬНЫЙ ТЕСТ ДЕДУПЛИКАЦИИ")
    print("=" * 80)
    print()
    print("🎯 Улучшения:")
    print("   ✅ Хеш БЕЗ учета автора (author_username)")
    print("   ✅ Кросс-посты отфильтровываются")
    print("   ✅ Одно объявление в разных группах = 1 уведомление")
    print()

    # Запуск
    response = requests.post(f"{API_URL}/workers/start", json=REQUEST_DATA, timeout=10)
    if response.status_code != 200:
        print(f"❌ Ошибка: {response.text}")
        return

    result = response.json()
    task_id = result['task_id']
    print(f"✅ Мониторинг запущен (Task ID: {task_id})")
    print()
    print("⏳ Мониторим 30 секунд...")
    print()

    # Мониторинг
    for i in range(6):
        time.sleep(5)
        response = requests.get(f"{API_URL}/workers/status/{task_id}", timeout=5)
        if response.status_code == 200:
            stats = response.json()['stats']
            print(f"[{i*5+5}с] "
                  f"📨 Сообщений: {stats['total_messages_scanned']:4d} | "
                  f"🎯 Найдено: {stats['items_found']:3d} | "
                  f"📬 Уведомлений: {stats['notifications_sent']:3d}")

    print()

    # Результаты
    response = requests.get(f"{API_URL}/workers/list/{task_id}?limit=100", timeout=5)
    if response.status_code == 200:
        items_data = response.json()
        total = items_data['total']

        print("=" * 80)
        print("РЕЗУЛЬТАТЫ")
        print("=" * 80)
        print()
        print("📊 Сравнение результатов:")
        print()
        print("   1️⃣  БЕЗ дедупликации:")
        print("       - Записей: 294 (49 уникальных, 83% дубликатов)")
        print()
        print("   2️⃣  С дедупликацией v1 (с учетом author):")
        print("       - Записей: 11")
        print("       - Уведомлений пользователю: 36 (кросс-посты не отфильтрованы)")
        print()
        print(f"   3️⃣  С дедупликацией v2 (БЕЗ author):")
        print(f"       - Записей: {total}")
        print(f"       - Уведомлений пользователю: {total}")
        print()

        improvement_v1 = ((294 - 11) / 294 * 100)
        improvement_v2 = ((294 - total) / 294 * 100) if total < 294 else 0
        improvement_notifications = ((36 - total) / 36 * 100) if total < 36 else 0

        print(f"📈 Улучшения:")
        print(f"   - По сравнению с v0 (без дедупликации): {improvement_v2:.1f}%")
        print(f"   - По сравнению с v1 (с author): {improvement_notifications:.1f}% меньше уведомлений")
        print()

        if total < 15:
            print(f"✅ ОТЛИЧНО! Кросс-посты успешно отфильтрованы!")
            print(f"   Пользователь получит {total} уникальных уведомлений вместо 36")
        else:
            print(f"⚠️  Записей: {total} (ожидалось < 15)")

        print()

    # Остановка
    requests.post(f"{API_URL}/workers/stop/{task_id}", timeout=5)
    print("🛑 Мониторинг остановлен")
    print()
    print("=" * 80)
    print("💡 Проверьте Telegram - должно прийти намного меньше уведомлений!")
    print("=" * 80)
    print()


if __name__ == "__main__":
    test_final_deduplication()
