"""Гео-фильтр сообщений по тексту — Москва / Санкт-Петербург.

Бизнес-логика (режим исключения):
  Режим Москва: сообщение берётся; исключается ТОЛЬКО при однозначном сигнале СПб.
  Режим СПб:   сообщение берётся; исключается ТОЛЬКО при однозначном сигнале Москвы.

Уровни детекции (ранний выход по приоритету):
  1. Явный город — алиасы + населённые пункты области  (early exit)
  2. Станции метро  — одного города → определён; двух → коллизия → улицы
  3. Названия улиц  — одного города → определён; двух → коллизия → нет сигнала

Поиск — по токенам нормализованного текста, не по подстроке (O(1) dict lookup).
Кеш   — LRU in-memory, 15 000 записей, ключ = нормализованный текст, значение = маска.
"""
import re
from collections import OrderedDict
from pathlib import Path
from typing import Tuple

from loguru import logger

# Бит-маски городов
MOSCOW: int = 1
SPB: int = 2

_DATA_DIR = Path(__file__).parent / "data" / "geo"

# --------------------------------------------------------------------------- #
# Нормализация                                                                  #
# --------------------------------------------------------------------------- #

_RE_CITY_PREFIX = re.compile(r'\bг\.?\s+')          # «г.» / «г » перед словом
_RE_HYPHEN      = re.compile(r'(\w)-(\w)')           # дефис между символами
_RE_SPECIAL     = re.compile(r'[^\w\s]')             # спецсимволы (не буква/цифра/пробел)
_RE_SPACES      = re.compile(r'\s+')                 # серия пробелов

# Аббревиатуры и типы улиц.
# Порядок важен: сначала раскрываем сокращения, затем удаляем полные слова типов.
_ABBR: tuple = (
    # --- раскрытие сокращений ---
    (re.compile(r'\bпр-кт\b'),   'проспект'),
    (re.compile(r'\bпросп\b'),   'проспект'),
    (re.compile(r'\bбул\b'),     'бульвар'),
    (re.compile(r'\bнаб\b'),     'набережная'),
    (re.compile(r'\bш\b'),       'шоссе'),
    (re.compile(r'\bпр\b'),      'проспект'),
    # --- удаление (сокращения) ---
    (re.compile(r'\bул\b'),      ''),
    # --- удаление (полные слова типов улиц) ---
    (re.compile(r'\bулица\b'),       ''),
    (re.compile(r'\bпроспект\b'),    ''),
    (re.compile(r'\bбульвар\b'),     ''),
    (re.compile(r'\bнабережная\b'),  ''),
    (re.compile(r'\bшоссе\b'),       ''),
    (re.compile(r'\bпереулок\b'),    ''),
    (re.compile(r'\bтупик\b'),       ''),
    (re.compile(r'\bплощадь\b'),     ''),
    (re.compile(r'\bаллея\b'),       ''),
    (re.compile(r'\bпроезд\b'),      ''),
    (re.compile(r'\bпросека\b'),     ''),
)


def _normalize(text: str) -> str:
    """Привести текст к единому виду для словарного поиска."""
    text = text.lower()
    text = text.replace('ё', 'е')
    text = _RE_CITY_PREFIX.sub('', text)      # г. Красногорск → красногорск
    text = text.replace('.', ' ')             # м. → м, пр. → пр
    text = _RE_HYPHEN.sub(r'\1 \2', text)     # санкт-петербург → санкт петербург
    for pattern, replacement in _ABBR:
        text = pattern.sub(replacement, text)
    text = _RE_SPECIAL.sub(' ', text)
    return _RE_SPACES.sub(' ', text).strip()


# --------------------------------------------------------------------------- #
# Загрузка словарей                                                             #
# --------------------------------------------------------------------------- #

def _load_dict(path: Path, city_mask: int, target: dict) -> None:
    """Загрузить словарь из файла, добавив записи в target с OR-маскированием."""
    if not path.exists():
        logger.warning(f"geo: словарь не найден: {path}")
        return
    loaded = 0
    with path.open(encoding='utf-8') as fh:
        for raw in fh:
            entry = raw.strip()
            if not entry or entry.startswith('#'):
                continue
            key = _normalize(entry)
            if not key:
                continue
            target[key] = target.get(key, 0) | city_mask
            loaded += 1
    logger.debug(f"geo: {path.name}: загружено {loaded} записей")


# --------------------------------------------------------------------------- #
# Сканирование                                                                  #
# --------------------------------------------------------------------------- #

def _scan(tokens: list, lookup: dict, max_n: int) -> int:
    """Пройти по токенам, генерируя n-граммы длиной 1..max_n.

    Возвращает накопленную битовую маску (0 / 1 / 2 / 3).
    Останавливается досрочно, если обе стороны уже найдены.
    """
    n = len(tokens)
    mask = 0
    for size in range(1, min(max_n, n) + 1):
        for i in range(n - size + 1):
            hit = lookup.get(' '.join(tokens[i:i + size]), 0)
            if hit:
                mask |= hit
                if mask == (MOSCOW | SPB):
                    return mask
    return mask


# --------------------------------------------------------------------------- #
# GeoFilter                                                                     #
# --------------------------------------------------------------------------- #

class GeoFilter:
    """Гео-фильтр с LRU-кешем.

    Публичный API:
        should_take_for_moscow(text: str) -> bool
        should_take_for_spb(text: str)    -> bool
    """

    _CACHE_SIZE: int = 15_000

    def __init__(self, data_dir: Path = _DATA_DIR) -> None:
        self._alias_dict:  dict = {}
        self._metro_dict:  dict = {}
        self._street_dict: dict = {}

        _load_dict(data_dir / 'moscow_aliases.txt', MOSCOW, self._alias_dict)
        _load_dict(data_dir / 'spb_aliases.txt',    SPB,    self._alias_dict)
        _load_dict(data_dir / 'metro_moscow.txt',   MOSCOW, self._metro_dict)
        _load_dict(data_dir / 'metro_spb.txt',      SPB,    self._metro_dict)
        _load_dict(data_dir / 'streets_moscow.txt', MOSCOW, self._street_dict)
        _load_dict(data_dir / 'streets_spb.txt',    SPB,    self._street_dict)

        self._max_alias_n:  int = max((len(k.split()) for k in self._alias_dict),  default=1)
        self._max_metro_n:  int = max((len(k.split()) for k in self._metro_dict),  default=1)
        self._max_street_n: int = max((len(k.split()) for k in self._street_dict), default=1)

        self._cache: OrderedDict = OrderedDict()

        logger.info(
            f"geo: словари загружены — алиасов={len(self._alias_dict)}, "
            f"метро={len(self._metro_dict)}, улиц={len(self._street_dict)}"
        )

    # ------------------------------------------------------------------ #

    def _detect(self, normalized: str) -> Tuple[int, str]:
        """Определить гео-маску по нормализованному тексту.

        Возвращает (mask, level):
          mask  — 0=нет сигнала, 1=Москва, 2=СПб, 3=коллизия
          level — 'explicit' | 'metro' | 'street' | 'none'
        """
        tokens = normalized.split()
        if not tokens:
            logger.debug('geo: no_signal')
            return 0, 'none'

        # Уровень 1: явный город (early exit — метро и улицы не проверяются)
        alias_mask = _scan(tokens, self._alias_dict, self._max_alias_n)
        if alias_mask:
            return alias_mask, 'explicit'

        # Уровень 2: метро
        metro_mask = _scan(tokens, self._metro_dict, self._max_metro_n)
        if metro_mask in (MOSCOW, SPB):
            return metro_mask, 'metro'
        if metro_mask == (MOSCOW | SPB):
            logger.debug('geo: conflict_metro')

        # Уровень 3: улицы (проверяем при metro_mask == 0 или коллизии)
        street_mask = _scan(tokens, self._street_dict, self._max_street_n)
        if street_mask in (MOSCOW, SPB):
            return street_mask, 'street'
        if street_mask == (MOSCOW | SPB):
            logger.debug('geo: conflict_street')

        logger.debug('geo: no_signal')
        return 0, 'none'

    def _get_mask(self, text: str) -> Tuple[int, str]:
        """Вернуть (mask, level) с LRU-кешированием."""
        norm = _normalize(text)
        if norm in self._cache:
            self._cache.move_to_end(norm)
            return self._cache[norm]
        result = self._detect(norm)
        if len(self._cache) >= self._CACHE_SIZE:
            self._cache.popitem(last=False)
        self._cache[norm] = result
        return result

    # ------------------------------------------------------------------ #

    def should_take_for_moscow(self, text: str) -> bool:
        """Взять сообщение в режиме Москва?

        Исключает только сообщения с однозначным сигналом СПб.
        """
        mask, level = self._get_mask(text)
        if mask == SPB:
            logger.info(f'geo: excluded {level}_spb')
            return False
        return True

    def should_take_for_spb(self, text: str) -> bool:
        """Взять сообщение в режиме СПб?

        Исключает только сообщения с однозначным сигналом Москвы.
        """
        mask, level = self._get_mask(text)
        if mask == MOSCOW:
            logger.info(f'geo: excluded {level}_moscow')
            return False
        return True


# Синглтон: словари загружаются один раз при первом импорте модуля
geo_filter = GeoFilter()
