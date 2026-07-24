from __future__ import annotations
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from ..domain import Candle, Tick

class MetaTrader5TickProvider:
    name = "mt5"
    def __init__(self, symbols: list[str], terminal_path: str | None = None, provider_name: str = "mt5") -> None:
        self.symbols=symbols; self.terminal_path=terminal_path; self.name=provider_name
    @staticmethod
    def _canonical_symbol(symbol: str) -> str:
        upper=symbol.upper()
        for suffix in (".PRO",".RAW",".ECN","_PRO","-PRO"):
            if upper.endswith(suffix): upper=upper[:-len(suffix)]; break
        return f"{upper[:3]}/{upper[3:]}" if len(upper)==6 and upper.isalpha() else upper
    def _module(self):
        try: import MetaTrader5 as mt5
        except ImportError as exc: raise RuntimeError("Install with: pip install -e '.[mt5]'") from exc
        return mt5
    def _initialize(self, mt5):
        ok=mt5.initialize(path=self.terminal_path) if self.terminal_path else mt5.initialize()
        if not ok: raise RuntimeError(f"MetaTrader5 initialization failed: {mt5.last_error()}")
    def stream(self) -> Iterator[Tick]:
        mt5=self._module(); self._initialize(mt5)
        try:
            for symbol in self.symbols:
                if not mt5.symbol_select(symbol,True): raise RuntimeError(f"Could not select MT5 symbol {symbol}: {mt5.last_error()}")
            last={}
            while True:
                emitted=False
                for symbol in self.symbols:
                    q=mt5.symbol_info_tick(symbol)
                    if q is None or q.bid<=0 or q.ask<=0: continue
                    ms=int(getattr(q,"time_msc",int(q.time)*1000))
                    if ms<=last.get(symbol,0): continue
                    last[symbol]=ms; emitted=True
                    yield Tick(self.name,self._canonical_symbol(symbol),datetime.fromtimestamp(ms/1000,tz=timezone.utc),Decimal(str(q.bid)),Decimal(str(q.ask)),Decimal(str(q.volume_real)) if q.volume_real else None)
                if not emitted: time.sleep(.05)
        finally: mt5.shutdown()
    def fetch_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[Candle]:
        mt5=self._module(); self._initialize(mt5)
        mapping={"1m":mt5.TIMEFRAME_M1,"5m":mt5.TIMEFRAME_M5,"15m":mt5.TIMEFRAME_M15,"30m":mt5.TIMEFRAME_M30,"1h":mt5.TIMEFRAME_H1,"4h":mt5.TIMEFRAME_H4,"1d":mt5.TIMEFRAME_D1}
        try:
            rates=mt5.copy_rates_range(symbol,mapping[timeframe],start,end)
            if rates is None: raise RuntimeError(f"MT5 history request failed for {symbol}: {mt5.last_error()}")
            return [Candle(self.name,self._canonical_symbol(symbol),timeframe,datetime.fromtimestamp(int(r['time']),tz=timezone.utc),Decimal(str(r['open'])),Decimal(str(r['high'])),Decimal(str(r['low'])),Decimal(str(r['close'])),int(r['tick_volume']),Decimal(str(r['real_volume'])) if r['real_volume'] else None,True) for r in rates]
        finally: mt5.shutdown()
