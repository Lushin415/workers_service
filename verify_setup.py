#!/usr/bin/env python
"""
Скрипт проверки готовности Workers Service
"""
import sys
import os

def check_file_exists(filepath, description):
    """Проверить существование файла"""
    if os.path.exists(filepath):
        print(f"✅ {description}: {filepath}")
        return True
    else:
        print(f"❌ {description} ОТСУТСТВУЕТ: {filepath}")
        return False

def check_imports():
    """Проверить импорты модулей"""
    print("\n🔍 Проверка импортов модулей:")

    modules = [
        "api",
        "models_api",
        "models_db",
        "db_service",
        "message_extractor",
        "filters",
        "state_manager",
        "tg_notifier",
        "parser",
        "tasks"
    ]

    all_ok = True
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"✅ {module_name}")
        except ImportError as e:
            print(f"❌ {module_name}: {e}")
            all_ok = False

    return all_ok

def check_regex_patterns():
    """Проверить regex паттерны на тестовых данных"""
    print("\n🧪 Проверка regex паттернов:")

    from message_extractor import MessageExtractor
    from datetime import datetime

    test_messages = [
        "Готов выйти сегодня, цена за смену 3000",
        "Выйду на замену, завтра, оплата 2500 руб",
        "Могу 15 февраля, 2800₽, шк - 100",
        "Работа ПВЗ, послезавтра, 3500, шк мало",
        "Ищу сотрудника на замену, Мытищи, 20 февраля, 2800",
        "Нужен работник в ПВЗ, завтра, оплата 3000 р, шк - 200",
        "15.02 могу выйти, зп 3200, штрихкодов 150"
    ]

    test_date = datetime(2026, 2, 1, 12, 0, 0)
    all_ok = True

    for msg in test_messages:
        result = MessageExtractor.extract(msg, test_date)
        if result:
            print(f"✅ '{msg[:40]}...'")
        else:
            print(f"❌ '{msg[:40]}...'")
            all_ok = False

    return all_ok

def main():
    """Главная функция проверки"""
    print("=" * 70)
    print("  ПРОВЕРКА ГОТОВНОСТИ WORKERS SERVICE")
    print("=" * 70)

    all_checks_passed = True

    # Проверка структуры файлов
    print("\n📁 Проверка структуры проекта:")
    files_to_check = [
        ("requirements.txt", "Зависимости"),
        ("api.py", "FastAPI приложение"),
        ("models_api.py", "Pydantic модели API"),
        ("models_db.py", "Модели БД"),
        ("parser.py", "Telegram парсер"),
        ("message_extractor.py", "Извлечение данных"),
        ("filters.py", "Фильтры"),
        ("db_service.py", "Сервис БД"),
        ("tg_notifier.py", "Уведомления"),
        ("state_manager.py", "Менеджер состояния"),
        ("tasks.py", "Фоновые задачи"),
        ("README.md", "Документация")
    ]

    for filepath, description in files_to_check:
        if not check_file_exists(filepath, description):
            all_checks_passed = False

    # Проверка импортов
    if not check_imports():
        all_checks_passed = False

    # Проверка regex
    if not check_regex_patterns():
        all_checks_passed = False

    # Итоговый результат
    print("\n" + "=" * 70)
    if all_checks_passed:
        print("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!")
        print("\nСледующие шаги:")
        print("1. Запустите сервис: python api.py")
        print("2. Откройте Swagger UI: http://localhost:8002/docs")
        print("3. Протестируйте API: python test_api.py")
        print("\n⚠️  Важно:")
        print("- Для работы с Telegram требуется API_ID и API_HASH")
        print("- Получить на: https://my.telegram.org")
        print("- Требуется токен бота для уведомлений")
        print("=" * 70)
        return 0
    else:
        print("❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ!")
        print("Проверьте ошибки выше и исправьте их.")
        print("=" * 70)
        return 1

if __name__ == "__main__":
    sys.exit(main())
