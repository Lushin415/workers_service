# Workers Service

Микросервис для мониторинга чатов Telegram и работы с черными списками. Работает на базе FastAPI и Pyrogram.

## Структура проекта

*   `api.py` — Точка входа (FastAPI сервер).
*   `parser.py` — Клиент Pyrogram для парсинга истории и real-time сообщений.
*   `blacklist_service.py` — Сервис поиска по черным спискам.
*   `db_service.py` — Работа с БД SQLite (`workers.db`).
*   `deduplicator.py` — Логика дедупликации объявлений.
*   `message_extractor.py` — Парсинг текста (цена, дата, тип объявления).

## Установка и запуск

1.  **Установка зависимостей:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Настройка окружения:**
    Создайте файл `.env` (см. `.env.example`):
    ```ini
    API_ID=123456
    API_HASH=your_hash
    BOT_TOKEN=your_bot_token
    
    # Настройки сервера
    HOST=0.0.0.0
    PORT=8002
    
    # Пути
    DB_PATH=workers.db
    SESSION_PATH=sessions/workers_session
    BLACKLIST_SESSION_PATH=sessions/blacklist_session
    ```

3.  **Запуск:**
    ```bash
    python api.py
    ```
    Swagger документация будет доступна по адресу: `http://localhost:8002/docs`

## API Endpoints

*   `POST /workers/start` — Запуск задачи мониторинга.
*   `POST /workers/stop/{task_id}` — Остановка задачи.
*   `GET /workers/status/{task_id}` — Статус задачи.
*   `GET /workers/list/{task_id}` — Получение найденных объявлений.
*   `POST /blacklist/check` — Проверка пользователя в ЧС.
*   `GET /blacklist/chats` — Управление чатами ЧС.

## Docker

```bash
docker-compose up -d --build