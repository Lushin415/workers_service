"""Тесты гео-фильтра.

Запуск:
    pytest tests/test_geo_filter.py -v

Чтобы добавить реальные сообщения — вставь их в секции
REAL_MOSCOW_MESSAGES и REAL_SPB_MESSAGES ниже.
"""
import sys
import os

import pytest

# Подключаем корень проекта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from geo_filter import geo_filter


# =========================================================================== #
# ВСТАВЬ СЮДА РЕАЛЬНЫЕ СООБЩЕНИЯ ИЗ МОСКОВСКИХ ЧАТОВ                          #
# Формат: строка текста сообщения                                              #
# Ожидание: эти сообщения ДОЛЖНЫ проходить в режиме Москва (should_take=True)  #
#           и ДОЛЖНЫ блокироваться в режиме СПб (should_take=False)            #
# =========================================================================== #
REAL_MOSCOW_MESSAGES = [

    # --- вставляй реальные сообщения сюда ---
    # "Выйду завтра, метро Войковская, оплата 1800",
    # "Ищу работу Химки, WB, дата 26.02",
]

# =========================================================================== #
# ВСТАВЬ СЮДА РЕАЛЬНЫЕ СООБЩЕНИЯ ИЗ ПИТЕРСКИХ ЧАТОВ                           #
# Ожидание: эти сообщения ДОЛЖНЫ проходить в режиме СПб (should_take=True)    #
#           и ДОЛЖНЫ блокироваться в режиме Москва (should_take=False)         #
# =========================================================================== #
REAL_SPB_MESSAGES = [ "Москва, ОЗОН Выйду на замену 23, 24, 26 февраля ШК до 200 Ставка от 3000  18 лет, гражданство рф Опыт год, могу сделать отчет об открытии и закрытии с фото"
    # --- вставляй реальные сообщения сюда ---
    # "Выйду завтра, метро Невский проспект, оплата 1800",
    # "Ищу работу Гатчина, Ozon, дата 26.02",
]

# =========================================================================== #
# ВСТАВЬ СЮДА СООБЩЕНИЯ БЕЗ ГОРОДА (должны проходить в ОБОИХ режимах)         #
# =========================================================================== #
NEUTRAL_MESSAGES = [
    # --- вставляй реальные сообщения сюда ---
    # "Ищу подработку грузчиком, 2500 руб/смена, дата 26.02",
]


# =========================================================================== #
# Синтетические тесты (базовая корректность, не трогай)                        #
# =========================================================================== #

class TestExplicitCity:
    """Уровень 1: явные названия городов и областей."""

    def test_moscow_nominative(self):
        assert geo_filter.should_take_for_moscow("работа Москва склад") is True
        assert geo_filter.should_take_for_spb("работа Москва склад") is False

    def test_moscow_prepositional(self):
        assert geo_filter.should_take_for_moscow("работа в Москве") is True
        assert geo_filter.should_take_for_spb("работа в Москве") is False

    def test_spb_nominative(self):
        assert geo_filter.should_take_for_moscow("работа Санкт-Петербург") is False
        assert geo_filter.should_take_for_spb("работа Санкт-Петербург") is True

    def test_spb_prepositional(self):
        assert geo_filter.should_take_for_moscow("работа в Питере") is False
        assert geo_filter.should_take_for_spb("работа в Питере") is True

    def test_leningrad(self):
        assert geo_filter.should_take_for_moscow("из Ленинграда") is False
        assert geo_filter.should_take_for_spb("из Ленинграда") is True

    def test_mo_settlement_nominative(self):
        assert geo_filter.should_take_for_spb("работа Химки WB") is False

    def test_mo_settlement_prepositional(self):
        assert geo_filter.should_take_for_spb("работа в Химках") is False
        assert geo_filter.should_take_for_spb("работа в Малаховке") is False
        assert geo_filter.should_take_for_spb("работа в Люберцах") is False

    def test_mo_city_prepositional(self):
        assert geo_filter.should_take_for_spb("работа в Красногорске") is False
        assert geo_filter.should_take_for_spb("работа в Жуковском") is False

    def test_lo_settlement(self):
        assert geo_filter.should_take_for_moscow("работа Гатчина") is False
        assert geo_filter.should_take_for_spb("работа Гатчина") is True


class TestMetro:
    """Уровень 2: станции метро."""

    def test_moscow_metro(self):
        assert geo_filter.should_take_for_moscow("метро Арбатская") is True
        assert geo_filter.should_take_for_spb("метро Арбатская") is False

    def test_spb_metro(self):
        assert geo_filter.should_take_for_moscow("метро Невский") is False
        assert geo_filter.should_take_for_spb("метро Невский") is True

    def test_moscow_metro_abbreviation(self):
        assert geo_filter.should_take_for_spb("м. Войковская, склад WB") is False

    def test_metro_conflict_taken_both(self):
        # Станции обоих городов → коллизия → берём в обоих режимах
        assert geo_filter.should_take_for_moscow("метро Арбатская или Невский") is True
        assert geo_filter.should_take_for_spb("метро Арбатская или Невский") is True


class TestStreets:
    """Уровень 3: улицы."""

    def test_moscow_street(self):
        assert geo_filter.should_take_for_spb("работа на Тверской") is False

    def test_spb_street(self):
        assert geo_filter.should_take_for_moscow("работа на Невском") is False


class TestNoSignal:
    """Нет сигнала → берём в обоих режимах."""

    def test_no_signal_moscow(self):
        assert geo_filter.should_take_for_moscow("ищу работу грузчиком, 2500 руб") is True

    def test_no_signal_spb(self):
        assert geo_filter.should_take_for_spb("ищу работу грузчиком, 2500 руб") is True

    def test_both_cities_conflict(self):
        # Оба города → коллизия → берём в обоих режимах
        assert geo_filter.should_take_for_moscow("работаю и в Москве и в Питере") is True
        assert geo_filter.should_take_for_spb("работаю и в Москве и в Питере") is True


class TestNormalization:
    """Нормализация: ё→е, дефисы, сокращения."""

    def test_yo(self):
        assert geo_filter.should_take_for_moscow("Санкт-Петербург, Невский пр.") is False

    def test_abbreviation_ul(self):
        assert geo_filter.should_take_for_moscow("ул. Тверская") is False or \
               geo_filter.should_take_for_spb("ул. Тверская") is False

    def test_city_prefix_g(self):
        assert geo_filter.should_take_for_spb("г. Химки, склад") is False


# =========================================================================== #
# Тесты на реальных сообщениях                                                 #
# =========================================================================== #

@pytest.mark.skipif(not REAL_MOSCOW_MESSAGES, reason="нет реальных московских сообщений")
class TestRealMoscow:
    @pytest.mark.parametrize("text", REAL_MOSCOW_MESSAGES)
    def test_taken_for_moscow(self, text):
        assert geo_filter.should_take_for_moscow(text) is True, \
            f"Московское сообщение было отфильтровано: {text!r}"

    @pytest.mark.parametrize("text", REAL_MOSCOW_MESSAGES)
    def test_blocked_for_spb(self, text):
        assert geo_filter.should_take_for_spb(text) is False, \
            f"Московское сообщение прошло в режим СПб: {text!r}"


@pytest.mark.skipif(not REAL_SPB_MESSAGES, reason="нет реальных питерских сообщений")
class TestRealSpb:
    @pytest.mark.parametrize("text", REAL_SPB_MESSAGES)
    def test_taken_for_spb(self, text):
        assert geo_filter.should_take_for_spb(text) is True, \
            f"Питерское сообщение было отфильтровано: {text!r}"

    @pytest.mark.parametrize("text", REAL_SPB_MESSAGES)
    def test_blocked_for_moscow(self, text):
        assert geo_filter.should_take_for_moscow(text) is False, \
            f"Питерское сообщение прошло в режим Москва: {text!r}"


@pytest.mark.skipif(not NEUTRAL_MESSAGES, reason="нет нейтральных сообщений")
class TestRealNeutral:
    @pytest.mark.parametrize("text", NEUTRAL_MESSAGES)
    def test_taken_for_moscow(self, text):
        assert geo_filter.should_take_for_moscow(text) is True, \
            f"Нейтральное сообщение было отфильтровано в режиме Москва: {text!r}"

    @pytest.mark.parametrize("text", NEUTRAL_MESSAGES)
    def test_taken_for_spb(self, text):
        assert geo_filter.should_take_for_spb(text) is True, \
            f"Нейтральное сообщение было отфильтровано в режиме СПб: {text!r}"


# =========================================================================== #
# Диагностика: python tests/test_geo_filter.py                                 #
# Печатает результат geo-фильтра для каждого сообщения из всех трёх секций.   #
# =========================================================================== #

if __name__ == '__main__':
    from loguru import logger
    import sys
    logger.remove()
    logger.add(sys.stdout, level='DEBUG', format='{message}')

    def _report(label: str, messages: list) -> None:
        if not messages:
            print(f'\n[{label}] — список пуст\n')
            return
        print(f'\n{"=" * 60}')
        print(f'  {label}')
        print('=' * 60)
        for text in messages:
            mask, level = geo_filter._get_mask(text)
            city = {0: 'нет сигнала', 1: 'Москва', 2: 'СПб', 3: 'коллизия'}[mask]
            msk = '✓ берём' if geo_filter.should_take_for_moscow(text) else '✗ отсев'
            spb = '✓ берём' if geo_filter.should_take_for_spb(text)    else '✗ отсев'
            print(f'\n  Текст : {text[:80]}')
            print(f'  Сигнал: {city} (уровень: {level})')
            print(f'  МСК   : {msk}   СПб: {spb}')

    _report('МОСКОВСКИЕ сообщения', REAL_MOSCOW_MESSAGES)
    _report('ПИТЕРСКИЕ сообщения',  REAL_SPB_MESSAGES)
    _report('НЕЙТРАЛЬНЫЕ сообщения', NEUTRAL_MESSAGES)
