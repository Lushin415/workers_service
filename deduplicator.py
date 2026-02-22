"""
Умная дедупликация объявлений (двухуровневая)

Уровень 1: Content hash (защита от копипасты)
Уровень 2: Author-based (защита от кросс-постов + изменение цены)
"""
import hashlib
from typing import Optional


class Deduplicator:
    """Класс для создания хешей и дедупликации объявлений"""

    @staticmethod
    def create_content_hash(
        author_username: Optional[str],
        price: Optional[int],
        location: Optional[str],
        message_text: str
    ) -> str:
        """
        Создать хеш содержимого объявления (БЕЗ учета даты работы и автора!)

        УРОВЕНЬ 1: Content-based deduplication

        Логика:
        - Одинаковый текст с той же ценой = дубликат (независимо от автора!)
        - НО если ДАТА РАБОТЫ изменилась = новое объявление (проверяется отдельно)
        - Author НЕ учитывается, т.к. одно объявление может быть:
          * От имени канала (без author)
          * Форвардом (другой author)
          * Отправлено через бота

        Args:
            author_username: username автора (НЕ используется в хеше)
            price: цена за смену
            location: локация (или None)
            message_text: полный текст сообщения

        Returns:
            SHA256 хеш в виде hex строки
        """
        # Нормализуем данные (БЕЗ author!)
        loc = location.lower() if location else "unknown"
        text = message_text.strip().lower()
        price_str = str(price) if price is not None else ""

        # Создаем строку для хеширования (БЕЗ author!)
        content = f"{price_str}|{loc}|{text}"

        # Создаем SHA256 хеш
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        return hash_obj.hexdigest()

    @staticmethod
    def create_author_key(
        author_username: str,
        work_date: str,
        price: Optional[int]
    ) -> str:
        """
        Создать ключ для author-based дедупликации

        УРОВЕНЬ 2: Author-based deduplication

        Логика:
        - Автор + дата работы + цена = уникальная комбинация
        - Если автор меняет ЦЕНУ → новое уведомление (ВАЖНО!)
        - Если автор ищет работу на ДРУГОЙ ДЕНЬ → новое уведомление
        - Если автор копирует одно и то же в разные чаты → дубликат
        - Через 24 часа старые записи удаляются → автор может снова разместить

        Примеры:
        1. "Ivan" + "2026-02-03" + "3000" → ключ1
        2. "Ivan" + "2026-02-03" + "2500" → ключ2 (другая цена → НОВОЕ!)
        3. "Ivan" + "2026-02-05" + "3000" → ключ3 (другой день → НОВОЕ!)

        Args:
            author_username: username автора
            work_date: дата работы (ISO format)
            price: цена за смену

        Returns:
            Строковый ключ для проверки дубликатов
        """
        price_key = str(price) if price is not None else ""
        return f"{author_username}|{work_date}|{price_key}"

    @staticmethod
    def is_duplicate(
        content_hash: str,
        work_date: str,
        existing_hashes: dict
    ) -> bool:
        """
        Проверить, является ли объявление дубликатом

        Args:
            content_hash: хеш содержимого
            work_date: дата работы (extracted['date'])
            existing_hashes: словарь {content_hash: set(dates)}

        Returns:
            True если дубликат, False если новое объявление
        """
        # Если такого хеша нет - точно не дубликат
        if content_hash not in existing_hashes:
            return False

        # Если хеш есть, проверяем даты работы
        existing_dates = existing_hashes[content_hash]

        # Если дата работы УЖЕ БЫЛА в течение 24 часов - дубликат
        if work_date in existing_dates:
            return True

        # Если дата работы НОВАЯ - не дубликат (это обновление объявления)
        return False
