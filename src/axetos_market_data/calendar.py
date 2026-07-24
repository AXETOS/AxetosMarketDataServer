from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta


@dataclass(frozen=True, slots=True)
class MarketClosure:
    date: date
    calendar_key: str
    name: str
    source: str = "built-in"


def _easter_sunday(year: int) -> date:
    """Gregorian Easter using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    current = date(year, month, 1)
    delta = (weekday - current.weekday()) % 7
    return current + timedelta(days=delta + (occurrence - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    first_next = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    current = first_next - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


class MarketCalendar:
    """Expected-session calendar used to prevent false candle-gap reports.

    FX is treated as a 24x5 market (Sunday 22:00 UTC through Friday 22:00 UTC).
    Crypto is 24x7. Index/metal/energy symbols use weekday sessions and a small,
    deterministic exchange-holiday set. The class is deliberately dependency-free
    and can be overlaid with explicit closures supplied by operators.
    """

    _US_SYMBOLS = {"US500", "US100", "US30", "XAU/USD", "XAG/USD", "OILWTI", "NATGAS"}
    _GB_SYMBOLS = {"GB100", "OILBRNT"}
    _EU_SYMBOLS = {"DE30", "FR40", "EU50"}

    def __init__(self, closures: list[MarketClosure] | None = None) -> None:
        self._overlays = {(x.calendar_key.upper(), x.date): x for x in closures or []}

    def is_expected_open(self, instrument: str, moment: datetime) -> bool:
        moment = moment.astimezone(UTC)
        symbol = instrument.upper().replace(" ", "")
        if self._is_crypto(symbol):
            return True
        if self._is_fx(symbol):
            return self._fx_open(moment)
        if moment.weekday() >= 5:
            return False
        return self.closure_for(symbol, moment.date()) is None

    def closure_for(self, instrument: str, day: date) -> MarketClosure | None:
        key = self.calendar_key(instrument)
        if key is None:
            return None
        overlay = self._overlays.get((key, day))
        if overlay:
            return overlay
        return self._built_in_closures(key, day.year).get(day)

    def closures(self, instrument: str, start: date, end: date) -> list[MarketClosure]:
        values: list[MarketClosure] = []
        cursor = start
        while cursor <= end:
            closure = self.closure_for(instrument, cursor)
            if closure:
                values.append(closure)
            cursor += timedelta(days=1)
        return values

    @classmethod
    def calendar_key(cls, instrument: str) -> str | None:
        symbol = instrument.upper().replace(" ", "")
        if symbol in cls._US_SYMBOLS:
            return "US"
        if symbol in cls._GB_SYMBOLS:
            return "GB"
        if symbol in cls._EU_SYMBOLS:
            return "EU"
        if symbol == "JP225":
            return "JP"
        if symbol == "AU200":
            return "AU"
        return None

    @staticmethod
    def _is_crypto(symbol: str) -> bool:
        return any(token in symbol for token in ("BTC", "ETH", "SOL", "XRP", "DOGE"))

    @staticmethod
    def _is_fx(symbol: str) -> bool:
        compact = symbol.replace("/", "")
        return len(compact) == 6 and compact.isalpha()

    @staticmethod
    def _fx_open(moment: datetime) -> bool:
        weekday = moment.weekday()  # Monday=0
        clock = moment.time()
        if weekday == 5:  # Saturday
            return False
        if weekday == 6:  # Sunday reopen
            return clock >= time(22, 0)
        if weekday == 4:  # Friday close
            return clock < time(22, 0)
        return True

    @staticmethod
    def _built_in_closures(key: str, year: int) -> dict[date, MarketClosure]:
        easter = _easter_sunday(year)
        rows: list[tuple[date, str]] = [(date(year, 1, 1), "New Year's Day"), (date(year, 12, 25), "Christmas Day")]
        if key in {"US", "GB", "EU", "AU"}:
            rows.append((easter - timedelta(days=2), "Good Friday"))
        if key == "US":
            rows.extend([
                (_nth_weekday(year, 1, 0, 3), "Martin Luther King Jr. Day"),
                (_last_weekday(year, 5, 0), "Memorial Day"),
                (date(year, 7, 4), "Independence Day"),
                (_nth_weekday(year, 9, 0, 1), "Labor Day"),
                (_nth_weekday(year, 11, 3, 4), "Thanksgiving Day"),
            ])
        elif key in {"GB", "EU"}:
            rows.append((easter + timedelta(days=1), "Easter Monday"))
        elif key == "AU":
            rows.extend([(date(year, 1, 26), "Australia Day"), (date(year, 4, 25), "Anzac Day")])
        return {d: MarketClosure(d, key, name) for d, name in rows}
