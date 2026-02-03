"""
Фильтрация объявлений по критериям
"""
from typing import Dict
from datetime import date
from loguru import logger


class ItemFilter:
    """Класс для фильтрации найденных объявлений"""

    def __init__(
        self,
        date_from: date,
        date_to: date,
        min_price: int,
        max_price: int,
        shk_filter: str
    ):
        self.date_from = date_from
        self.date_to = date_to
        self.min_price = min_price
        self.max_price = max_price
        self.shk_filter = shk_filter

    def matches(self, extracted_data: Dict) -> bool:
        """
        Проверить, подходит ли объявление по фильтрам

        Args:
            extracted_data: словарь с извлеченными данными
                - date: str (ISO format)
                - price: int
                - shk: Optional[str]

        Returns:
            True если объявление подходит, False иначе
        """
        # Проверка даты
        try:
            item_date = date.fromisoformat(extracted_data['date'])
            if not (self.date_from <= item_date <= self.date_to):
                logger.debug(f"Дата {item_date} не в диапазоне {self.date_from} - {self.date_to}")
                return False
        except (ValueError, KeyError):
            logger.debug("Некорректная дата")
            return False

        # Проверка цены
        price = extracted_data.get('price')
        if price is None:
            logger.debug("Цена отсутствует")
            return False

        if not (self.min_price <= price <= self.max_price):
            logger.debug(f"Цена {price} не в диапазоне {self.min_price} - {self.max_price}")
            return False

        # Проверка ШК (если фильтр задан и не "любое")
        if self.shk_filter != "любое":
            shk = extracted_data.get('shk')

            # Если фильтр задан, но ШК нет в объявлении
            if not shk:
                logger.debug(f"ШК не найден, а фильтр требует: {self.shk_filter}")
                return False

            # Проверка на конкретное значение или качественную оценку
            if shk.lower() != self.shk_filter.lower():
                logger.debug(f"ШК '{shk}' не совпадает с фильтром '{self.shk_filter}'")
                return False

        logger.debug("Объявление прошло все фильтры")
        return True
