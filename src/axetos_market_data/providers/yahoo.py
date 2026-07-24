from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from ..domain import Candle


_INTERVALS = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "60m", "4h": "1h", "1d": "1d"}
_DEFAULT_SYMBOLS = {
    "EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X",
    "AUD/USD": "AUDUSD=X", "USD/CAD": "CAD=X", "USD/CHF": "CHF=X",
    "XAU/USD": "GC=F", "XAG/USD": "SI=F", "OILWTI": "CL=F", "OILBRNT": "BZ=F",
    "US500": "^GSPC", "US100": "^IXIC", "US30": "^DJI", "BTC/USD": "BTC-USD",
}


class YahooHistoricalProvider:
    """Historical fallback adapter for Yahoo Finance's chart endpoint.

    It is intentionally historical-only; it does not participate in live tick routing.
    Yahoo interval/range restrictions remain authoritative and errors are surfaced rather
    than silently fabricating candles.
    """

    def __init__(self, provider_key: str = "Yahoo.Fallback", timeout_seconds: float = 20.0) -> None:
        self.provider_key = provider_key
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def resolve_symbol(symbol_or_instrument: str) -> str:
        return _DEFAULT_SYMBOLS.get(symbol_or_instrument.upper(), symbol_or_instrument)

    def fetch_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[Candle]:
        interval = _INTERVALS.get(timeframe)
        if interval is None:
            raise ValueError(f"Unsupported Yahoo timeframe: {timeframe}")
        ticker = self.resolve_symbol(symbol)
        params = urlencode({"period1": int(start.astimezone(UTC).timestamp()), "period2": int(end.astimezone(UTC).timestamp()), "interval": interval, "events": "history"})
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}?{params}"
        request = Request(url, headers={"User-Agent": "AxetosMarketDataServer/0.12"})
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - fixed HTTPS host
            payload = json.load(response)
        return self._parse(payload, timeframe)

    def test_connection(self, symbol: str = "EURUSD=X") -> dict[str, object]:
        now = datetime.now(UTC)
        candles = self.fetch_candles(symbol, "1d", now - timedelta(days=5), now)
        return {"ok": bool(candles), "symbol": symbol, "candles_received": len(candles), "message": "Yahoo chart endpoint returned valid candles." if candles else "Yahoo returned no candles."}

    def _parse(self, payload: dict[str, object], timeframe: str) -> list[Candle]:
        chart = payload.get("chart") if isinstance(payload, dict) else None
        results = chart.get("result") if isinstance(chart, dict) else None
        if not isinstance(results, list) or not results:
            error = chart.get("error") if isinstance(chart, dict) else None
            raise RuntimeError(f"Yahoo chart response contained no result: {error}")
        result = results[0]
        timestamps = result.get("timestamp", [])
        quotes = (((result.get("indicators") or {}).get("quote") or [{}])[0])
        opens, highs, lows, closes = (quotes.get(k, []) for k in ("open", "high", "low", "close"))
        count = min(map(len, (timestamps, opens, highs, lows, closes)))
        candles: list[Candle] = []
        for i in range(count):
            values = (opens[i], highs[i], lows[i], closes[i])
            if any(value is None for value in values):
                continue
            try:
                open_, high, low, close = (Decimal(str(value)) for value in values)
                if high < low:
                    high, low = low, high
                high = max(high, open_, close)
                low = min(low, open_, close)
                candles.append(Candle(self.provider_key, "unresolved", timeframe, datetime.fromtimestamp(int(timestamps[i]), UTC), open_, high, low, close, 0, complete=True))
            except (ValueError, TypeError, ArithmeticError):
                continue
        deduped = {c.open_time: c for c in candles}
        return [deduped[key] for key in sorted(deduped)]
