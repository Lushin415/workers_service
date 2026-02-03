"""
Умная дедупликация объявлений
"""
import hashlib
from typing import Optional


class Deduplicator:
    """Класс для создания хешей и дедупликации объявлений"""

    @staticmethod
    def create_content_hash(
        author_username: Optional[str],
        price: int,
        location: Optional[str],
        message_text: str
    ) -> str:
        """
        Создать хеш содержимого объявления (БЕЗ учета даты работы и автора!)

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
        price_str = str(price)

        # Создаем строку для хеширования (БЕЗ author!)
        content = f"{price_str}|{loc}|{text}"

        # Создаем SHA256 хеш
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        return hash_obj.hexdigest()

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
