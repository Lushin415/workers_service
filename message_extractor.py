import re
from typing import Optional, Dict
from datetime import datetime, timedelta
from loguru import logger


class MessageExtractor:
    """Извлечение структурированных данных из текста"""

    EMPLOYER_KEYWORDS = [
        "требуется", "требуются", "вакансия", "ищем", "набираем",
        "приглашаем", "нужен сотрудник", "нужен работник",
        "нужен человек", "ищем продавца", "оператора",
        "на постоянную работу", "график работы", "оформление",
        "выплаты", "зп 2 раза", "условия", "требования"
    ]

    WORKER_KEYWORDS = [
        "выйду", "могу выйти", "ищу работу", "ищу смену",
        "ищу подработку", "возьму смену", "рассмотрю смены",
        "устроюсь", "устроимся", "свободен", "готов работать",
        "ищу пункт", "могу"
    ]

    WEEKDAYS = {
        "понедельник": 0, "вторник": 1, "среда": 2, "среду": 2,
        "четверг": 3, "пятница": 4, "пятницу": 4,
        "суббота": 5, "субботу": 5,
        "воскресенье": 6
    }

    WEEKDAY_ABBR = {
        "пн": 0, "вт": 1, "ср": 2, "чт": 3, "пт": 4, "сб": 5, "вс": 6
    }

    MONTHS = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }

    PRICE_PATTERNS = [
        r'(\d+(?:[.,]\d+)?)\s*к\b(?!\s*\d)',  # 2к / 3 к / 2,5к — но НЕ "67 к 3" (адрес)
        r'(\d+)\s*тыс',
        r'(\d{3,5})\s*(?:₽|руб|р\.?)',
        r'(?:ставка|зп|оплата)[^\d]{0,10}(\d{3,5})',
        r'\b(\d{4,5})\b'
    ]

    SHK_PATTERNS = [
        r'(\d{2,4})[^\S\n]*[-–][^\S\n]*(\d{2,4})[^\S\n]*шк',  # 150-200 шк (одна строка)
        r'шк[^\S\n]*[-:—]?[^\S\n]*(\d{2,4})[^\S\n]*[-–][^\S\n]*(\d{2,4})',  # шк: 150-200
        r'(\d{2,4})\s*шк',                               # 150 шк
        r'шк\s+до\s+(\d{2,4})',                           # шк до 500
        r'шк\s*[-:—]?\s*(\d{2,4})',                       # шк: 150 / шк 150
        r'шк\s*[-:—]?\s*(мало|много|средне)',              # шк мало
    ]

    @staticmethod
    def detect_type(text: str) -> Optional[str]:
        text = text.lower()

        for k in MessageExtractor.EMPLOYER_KEYWORDS:
            if k in text:
                return "employer"

        for k in MessageExtractor.WORKER_KEYWORDS:
            if k in text:
                return "worker"

        return None

    @staticmethod
    def _nearest_weekday(target: int, base: datetime) -> datetime:
        days_ahead = target - base.weekday()
        if days_ahead < 0:
            days_ahead += 7
        return base + timedelta(days=days_ahead)

    @staticmethod
    def extract_date(text: str, message_date: datetime) -> Optional[str]:
        text = text.lower()

        if "послезавтра" in text:
            return (message_date + timedelta(days=2)).date().isoformat()
        if "завтра" in text:
            return (message_date + timedelta(days=1)).date().isoformat()
        if "сегодня" in text:
            return message_date.date().isoformat()

        for word, num in MessageExtractor.WEEKDAYS.items():
            if re.search(r'\b' + re.escape(word) + r'\b', text):
                return MessageExtractor._nearest_weekday(num, message_date).date().isoformat()

        abbr = re.search(r'\b(пн|вт|ср|чт|пт|сб|вс)\b', text)
        if abbr:
            num = MessageExtractor.WEEKDAY_ABBR[abbr.group(1)]
            return MessageExtractor._nearest_weekday(num, message_date).date().isoformat()

        m = re.search(r'(\d{1,2})[-\s]?(?:го|числа)', text)
        if m:
            day = int(m.group(1))
            month = message_date.month
            year = message_date.year
            try:
                dt = datetime(year, month, day)
                if dt.date() < message_date.date():
                    next_month = month + 1 if month < 12 else 1
                    next_year = year if month < 12 else year + 1
                    dt = datetime(next_year, next_month, day)
                return dt.date().isoformat()
            except ValueError:
                pass

        m = re.search(r'(\d{1,2})[./](\d{1,2})', text)
        if m:
            day, month = map(int, m.groups())
            year = message_date.year
            try:
                dt = datetime(year, month, day)
                if dt.date() < message_date.date():
                    dt = datetime(year + 1, month, day)
                return dt.date().isoformat()
            except ValueError:
                pass

        m = re.search(r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)', text)
        if m:
            day = int(m.group(1))
            month = MessageExtractor.MONTHS[m.group(2)]
            year = message_date.year
            dt = datetime(year, month, day)
            if dt.date() < message_date.date():
                dt = datetime(year + 1, month, day)
            return dt.date().isoformat()

        return None

    @staticmethod
    def extract_price(text: str, msg_type: Optional[str]) -> Optional[int]:
        text = text.lower()
        prices = []

        for pattern in MessageExtractor.PRICE_PATTERNS:
            for m in re.finditer(pattern, text):
                try:
                    val = m.group(1).replace(",", ".")
                    price = float(val)
                    if "к" in m.group(0) or "тыс" in m.group(0):
                        price *= 1000
                    prices.append(int(price))
                except (ValueError, IndexError):
                    continue

        if not prices:
            return None

        return min(prices) if msg_type == "worker" else max(prices)

    @staticmethod
    def extract_shk(text: str) -> Optional[str]:
        text = text.lower()

        for pattern in MessageExtractor.SHK_PATTERNS:
            m = re.search(pattern, text)
            if m:
                if m.lastindex and m.lastindex >= 2:
                    return f"{m.group(1)}-{m.group(2)}"
                return m.group(1)

        return None

    @staticmethod
    def has_worker_intent(text: str) -> bool:
        return bool(re.search(r'выйду|ищу|устроюсь|свободен|готов', text.lower()))

    @staticmethod
    def extract(text: str, message_date: datetime) -> Optional[Dict]:
        msg_type = MessageExtractor.detect_type(text)

        date = MessageExtractor.extract_date(text, message_date)
        shk = MessageExtractor.extract_shk(text)

        if not date:
            date = message_date.date().isoformat()

        # Нормализуем тип ДО извлечения цены — влияет на выбор min/max
        effective_type = msg_type
        if effective_type is None and MessageExtractor.has_worker_intent(text):
            effective_type = "worker"

        price = MessageExtractor.extract_price(text, effective_type)

        # Финальная нормализация типа
        if msg_type is None:
            if effective_type == "worker":
                # Intent без явного keyword — работник
                msg_type = "worker"
            elif price is not None:
                # Нет ни keyword, ни intent — чаты специализированные, дефолт employer
                msg_type = "employer"
            else:
                logger.debug(f"Цена и тип не найдены: {text[:50]}")
                return None

        return {
            "type": msg_type,
            "price": price,
            "date": date,
            "shk": shk,
            "location": None
        }