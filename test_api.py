"""
Тестовый скрипт для проверки API
"""
import requests
import json
from datetime import date, timedelta

BASE_URL = "http://localhost:8002"

def test_start_monitoring():
    """Тест запуска мониторинга"""
    print("🧪 Тест: POST /workers/start")

    payload = {
        "user_id": 338908929,
        "mode": "worker",
        "chats": ["@test_channel"],
        "filters": {
            "date_from": date.today().isoformat(),
            "date_to": (date.today() + timedelta(days=10)).isoformat(),
            "min_price": 2000,
            "max_price": 5000,
            "shk_filter": "любое"
        },
        "api_id": 12345678,
        "api_hash": "test_api_hash",
        "notification_bot_token": "test_bot_token",
        "notification_chat_id": 338908929,
        "parse_history_days": 14
    }

    try:
        response = requests.post(f"{BASE_URL}/workers/start", json=payload)
        print(f"Статус: {response.status_code}")
        print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")

        if response.status_code == 200:
            task_id = response.json()["task_id"]
            print(f"✅ Задача создана: {task_id}")
            return task_id
        else:
            print("❌ Ошибка создания задачи")
            return None

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None


def test_get_status(task_id):
    """Тест получения статуса"""
    print(f"\n🧪 Тест: GET /workers/status/{task_id}")

    try:
        response = requests.get(f"{BASE_URL}/workers/status/{task_id}")
        print(f"Статус: {response.status_code}")
        print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")

        if response.status_code == 200:
            print("✅ Статус получен")
        else:
            print("❌ Ошибка получения статуса")

    except Exception as e:
        print(f"❌ Ошибка: {e}")


def test_stop_monitoring(task_id):
    """Тест остановки мониторинга"""
    print(f"\n🧪 Тест: POST /workers/stop/{task_id}")

    try:
        response = requests.post(f"{BASE_URL}/workers/stop/{task_id}")
        print(f"Статус: {response.status_code}")
        print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")

        if response.status_code == 200:
            print("✅ Мониторинг остановлен")
        else:
            print("❌ Ошибка остановки")

    except Exception as e:
        print(f"❌ Ошибка: {e}")


def test_root():
    """Тест корневого endpoint"""
    print("🧪 Тест: GET /")

    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"Статус: {response.status_code}")
        print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")

        if response.status_code == 200:
            print("✅ Корневой endpoint работает")
        else:
            print("❌ Ошибка корневого endpoint")

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ WORKERS SERVICE API")
    print("=" * 60)

    # Тест корневого endpoint
    test_root()

    print("\n" + "=" * 60)

    # Тест запуска мониторинга
    task_id = test_start_monitoring()

    if task_id:
        # Небольшая пауза
        import time
        time.sleep(2)

        # Тест получения статуса
        test_get_status(task_id)

        # Тест остановки
        test_stop_monitoring(task_id)

    print("\n" + "=" * 60)
    print("ТЕСТЫ ЗАВЕРШЕНЫ")
    print("=" * 60)
