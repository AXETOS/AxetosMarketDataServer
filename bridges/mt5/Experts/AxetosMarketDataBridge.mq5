#property copyright "AxetosOS"
#property version   "1.10"
#property strict
#property description "Provider-agnostic MT5 market-data bridge for Axetos Market Data Server."

input string InpProviderKey       = "Oanda.MT5";
input string InpServerUrl         = "http://127.0.0.1:8000";
input string InpBridgeToken       = ""; // Optional. Leave blank for local loopback use.
input string InpSymbols           = "EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD,AUDUSD,NZDUSD";
input bool   InpDiscoverAllSymbols = true;
input int    InpDiscoveryBatchSize = 75;
input int    InpBackfillBarsM1     = 2000;
input int    InpBackfillBarsM15    = 2000;
input int    InpBackfillBarsH1     = 2000;
input int    InpBackfillBarsD1     = 4000;
input int    InpBackfillStableChecks = 5;
input int    InpBackfillRetrySeconds = 2;
input int    InpBackfillMaxAttempts = 90;
input int    InpRequestTimeoutMs   = 5000;
input int    InpHeartbeatSeconds   = 1;
input int    InpSelectionRefreshSeconds = 15;
input bool   InpUseServerSelection = true;
input bool   InpSendHistoricalBars = true;

string g_symbols[];
datetime g_last_m1_bar[];
int g_backfill_symbol_index = 0;
int g_backfill_interval_index = 0;
datetime g_last_heartbeat = 0;
datetime g_last_selection_refresh = 0;
string g_terminal_id = "";
bool g_backfill_enabled = true;
int g_backfill_attempt_count = 0;
int g_backfill_stable_count = 0;
int g_backfill_last_count = -1;
datetime g_backfill_last_oldest = 0;
datetime g_backfill_next_attempt = 0;

// Provider-native week/month history is never requested. The server builds
// Monday-Sunday weeks and true calendar months exclusively from canonical daily data.
ENUM_TIMEFRAMES g_timeframes[4] = { PERIOD_D1, PERIOD_H1, PERIOD_M15, PERIOD_M1 };
string g_intervals[4] = { "1d", "1h", "15m", "1m" };

int OnInit()
{
   string raw[];
   int count = StringSplit(InpSymbols, ',', raw);
   if(count <= 0)
   {
      Print("Axetos MT5 Bridge: no symbols configured.");
      return INIT_FAILED;
   }

   ArrayResize(g_symbols, count);
   ArrayResize(g_last_m1_bar, count);
   int accepted = 0;
   for(int i = 0; i < count; i++)
   {
      string symbol = raw[i];
      StringTrimLeft(symbol);
      StringTrimRight(symbol);
      if(symbol == "")
         continue;

      string resolved_symbol = ResolveProviderSymbol(symbol);
      if(resolved_symbol == "" || !SymbolSelect(resolved_symbol, true))
      {
         PrintFormat("Axetos MT5 Bridge: symbol '%s' is unavailable in this terminal.", symbol);
         continue;
      }

      g_symbols[accepted] = resolved_symbol;
      // Start from the currently open minute so attaching/restarting the EA does not
      // immediately resend the same recent candle batch.
      g_last_m1_bar[accepted] = iTime(resolved_symbol, PERIOD_M1, 0);
      accepted++;
   }

   ArrayResize(g_symbols, accepted);
   ArrayResize(g_last_m1_bar, accepted);
   if(accepted == 0)
      return INIT_FAILED;

   g_terminal_id = BuildTerminalId();
   EventSetTimer(1);
   SendHeartbeat();
   if(InpDiscoverAllSymbols)
      SendDiscoveredInstrumentCatalogue();
   else
      SendStreamingInstrumentCatalogue();

   if(InpUseServerSelection)
   {
      RefreshServerSelection();
      RefreshRepairRequest();
   }
   PrintFormat("Axetos MT5 Bridge started: provider=%s terminal=%s streaming-symbols=%d server=%s",
               InpProviderKey, g_terminal_id, accepted, InpServerUrl);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
}

void OnTimer()
{
   datetime now = TimeCurrent();
   if(g_last_heartbeat == 0 || now - g_last_heartbeat >= InpHeartbeatSeconds)
      SendHeartbeat();

   if(InpUseServerSelection &&
      (g_last_selection_refresh == 0 || now - g_last_selection_refresh >= InpSelectionRefreshSeconds))
   {
      RefreshServerSelection();
      RefreshRepairRequest();
   }

   SendCurrentTicks();
   SendCompletedMinuteBars();

   if(InpSendHistoricalBars && g_backfill_enabled)
      SendNextBackfillJob();
}

string ResolveProviderSymbol(string configured_symbol)
{
   if(SymbolInfoInteger(configured_symbol, SYMBOL_EXIST))
      return configured_symbol;

   string wanted = NormalizeProviderSymbol(configured_symbol);
   int total = SymbolsTotal(false);
   for(int i = 0; i < total; i++)
   {
      string candidate = SymbolName(i, false);
      if(NormalizeProviderSymbol(candidate) == wanted)
         return candidate;
   }
   return "";
}

string NormalizeProviderSymbol(string value)
{
   string normalized = "";
   int length = StringLen(value);
   for(int i = 0; i < length; i++)
   {
      ushort c = StringGetCharacter(value, i);
      if((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))
         normalized += StringSubstr(value, i, 1);
      else
         break;
   }
   StringToUpper(normalized);
   return normalized;
}

string BuildTerminalId()
{
   long login = AccountInfoInteger(ACCOUNT_LOGIN);
   string server = AccountInfoString(ACCOUNT_SERVER);
   string company = AccountInfoString(ACCOUNT_COMPANY);
   return StringFormat("%s-%s-%I64d", SanitizeId(company), SanitizeId(server), login);
}

string SanitizeId(string value)
{
   StringReplace(value, " ", "-");
   StringReplace(value, "/", "-");
   StringReplace(value, "\\", "-");
   StringReplace(value, ":", "-");
   return value;
}

void SendHeartbeat()
{
   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"brokerName\":\"%s\",\"serverName\":\"%s\",\"accountLogin\":%I64d,\"timeUtc\":\"%s\"}",
      JsonEscape(InpProviderKey),
      JsonEscape(g_terminal_id),
      JsonEscape(AccountInfoString(ACCOUNT_COMPANY)),
      JsonEscape(AccountInfoString(ACCOUNT_SERVER)),
      AccountInfoInteger(ACCOUNT_LOGIN),
      IsoUtc(TimeGMT()));

   if(PostJson("/api/market-data/ingest/mt5/heartbeat", json))
      g_last_heartbeat = TimeCurrent();
}

void SendStreamingInstrumentCatalogue()
{
   SendInstrumentBatch(g_symbols, 0, ArraySize(g_symbols));
}

void SendDiscoveredInstrumentCatalogue()
{
   int total = SymbolsTotal(false);
   if(total <= 0)
   {
      Print("Axetos MT5 Bridge: terminal returned no discoverable symbols.");
      return;
   }

   string discovered[];
   ArrayResize(discovered, total);
   int accepted = 0;
   for(int i = 0; i < total; i++)
   {
      string symbol = SymbolName(i, false);
      if(symbol == "")
         continue;
      discovered[accepted++] = symbol;
   }
   ArrayResize(discovered, accepted);

   int batch_size = MathMax(10, MathMin(250, InpDiscoveryBatchSize));
   int sent = 0;
   for(int offset = 0; offset < accepted; offset += batch_size)
   {
      int count = MathMin(batch_size, accepted - offset);
      if(SendInstrumentBatch(discovered, offset, count))
         sent += count;
   }

   PrintFormat("Axetos MT5 Bridge: discovered and submitted %d of %d available symbols.", sent, accepted);
}

bool SendInstrumentBatch(string &symbols[], int offset, int count)
{
   string items = "";
   int upper = MathMin(ArraySize(symbols), offset + count);
   for(int i = offset; i < upper; i++)
   {
      string symbol = symbols[i];
      long digits = SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
      bool visible = (bool)SymbolInfoInteger(symbol, SYMBOL_VISIBLE);
      bool selected = (bool)SymbolInfoInteger(symbol, SYMBOL_SELECT);
      bool custom = (bool)SymbolInfoInteger(symbol, SYMBOL_CUSTOM);
      string description = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
      string path = SymbolInfoString(symbol, SYMBOL_PATH);
      string currency_base = SymbolInfoString(symbol, SYMBOL_CURRENCY_BASE);
      string currency_profit = SymbolInfoString(symbol, SYMBOL_CURRENCY_PROFIT);
      string currency_margin = SymbolInfoString(symbol, SYMBOL_CURRENCY_MARGIN);
      double contract_size = SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
      double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
      double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
      double volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
      long trade_mode = SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
      long calc_mode = SymbolInfoInteger(symbol, SYMBOL_TRADE_CALC_MODE);
      string asset_class = ClassifyAsset(symbol, description, path, currency_base, currency_profit);
      string canonical = CanonicalDiscoveredSymbol(symbol, asset_class, currency_base, currency_profit);

      if(items != "") items += ",";
      items += StringFormat(
         "{\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"digits\":%d,\"point\":%s,\"isVisible\":%s,\"displayName\":\"%s\",\"description\":\"%s\",\"path\":\"%s\",\"assetClass\":\"%s\",\"currencyBase\":\"%s\",\"currencyProfit\":\"%s\",\"currencyMargin\":\"%s\",\"contractSize\":%s,\"volumeMin\":%s,\"volumeMax\":%s,\"volumeStep\":%s,\"tradeMode\":%d,\"calculationMode\":%d,\"isCustom\":%s,\"isSelected\":%s}",
         JsonEscape(symbol), JsonEscape(canonical), (int)digits,
         DoubleToString(point, (int)MathMax(0, digits)), visible ? "true" : "false",
         JsonEscape(description != "" ? description : symbol), JsonEscape(description), JsonEscape(path),
         JsonEscape(asset_class), JsonEscape(currency_base), JsonEscape(currency_profit), JsonEscape(currency_margin),
         JsonNumber(contract_size), JsonNumber(volume_min), JsonNumber(volume_max), JsonNumber(volume_step),
         (int)trade_mode, (int)calc_mode, custom ? "true" : "false", selected ? "true" : "false");
   }

   if(items == "")
      return true;

   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"timeUtc\":\"%s\",\"instruments\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), IsoUtc(TimeGMT()), items);
   return PostJson("/api/market-data/ingest/mt5/instruments", json);
}

string ClassifyAsset(string symbol, string description, string path, string currency_base, string currency_profit)
{
   string normalized_path = path;
   StringToUpper(normalized_path);

   // MT5's broker catalogue path is the strongest classification signal. OANDA's
   // PRO tree mixes FX, metals, energy, indices and commodities under one root.
   if(StringFind(normalized_path, "EQUITIES_CFD\\") == 0)
      return "EquityCFD";
   if(StringFind(normalized_path, "ETF_CFD\\") == 0)
      return "EtfCFD";
   if(StringFind(normalized_path, "CRYPTO\\") == 0)
      return "Crypto";
   if(StringFind(normalized_path, "FOREX\\") == 0 || StringFind(normalized_path, "PRO\\FX\\") == 0)
      return "Forex";
   if(StringFind(normalized_path, "PRO\\NOBLE\\") == 0 || StringFind(normalized_path, "METAL") >= 0)
      return "Metal";
   if(StringFind(normalized_path, "PRO\\ENERGY\\") == 0)
      return "Energy";
   if(StringFind(normalized_path, "PRO\\INDICES\\") == 0)
      return "Index";
   if(StringFind(normalized_path, "PRO\\COMMODITIES\\") == 0)
      return "Commodity";
   if(StringFind(normalized_path, "BOND") >= 0 || StringFind(normalized_path, "TREASUR") >= 0)
      return "Bond";
   if(StringFind(normalized_path, "FUTURE") >= 0)
      return "Future";

   string haystack = symbol + " " + description + " " + path;
   StringToUpper(haystack);
   if(StringFind(haystack, "GOLD") >= 0 || StringFind(haystack, "SILVER") >= 0 ||
      StringFind(haystack, "XAU") >= 0 || StringFind(haystack, "XAG") >= 0 ||
      StringFind(haystack, "PLATINUM") >= 0 || StringFind(haystack, "PALLADIUM") >= 0)
      return "Metal";
   if(StringFind(haystack, "ENERGY") >= 0 || StringFind(haystack, "OIL") >= 0 ||
      StringFind(haystack, "BRENT") >= 0 || StringFind(haystack, "WTI") >= 0 ||
      StringFind(haystack, "NATGAS") >= 0 || StringFind(haystack, "NATURAL GAS") >= 0)
      return "Energy";
   if(StringFind(haystack, "INDEX") >= 0 || StringFind(haystack, "INDICES") >= 0)
      return "Index";
   if(StringFind(haystack, "CRYPTO") >= 0 || StringFind(haystack, "BITCOIN") >= 0 || StringFind(haystack, "ETHEREUM") >= 0)
      return "Crypto";
   if(StringFind(haystack, "ETF") >= 0)
      return "EtfCFD";
   if(StringFind(haystack, "EQUITY") >= 0 || StringFind(haystack, "STOCK") >= 0 || StringFind(haystack, "SHARE") >= 0)
      return "EquityCFD";
   if(IsCurrencyCode(currency_base) && IsCurrencyCode(currency_profit))
      return "Forex";
   return "CFD";
}

bool IsCurrencyCode(string value)
{
   if(StringLen(value) != 3)
      return false;
   string upper = value;
   StringToUpper(upper);
   string known = "USD,EUR,GBP,JPY,CHF,CAD,AUD,NZD,SEK,NOK,DKK,PLN,CZK,HUF,TRY,ZAR,MXN,SGD,HKD,CNH,CNY,ILS,AED,SAR,THB,INR,IDR,MYR,PHP,TWD,BRL,CLP,COP,ARS";
   return StringFind("," + known + ",", "," + upper + ",") >= 0;
}

string CanonicalDiscoveredSymbol(string provider_symbol, string asset_class, string currency_base, string currency_profit)
{
   string base = currency_base;
   string profit = currency_profit;
   StringToUpper(base);
   StringToUpper(profit);
   // Crypto CFDs frequently report the account settlement currency (USD) as both
   // currency base and profit. Derive the traded crypto/quote pair from the broker
   // symbol instead: BTCUSD -> BTC/USD, ETHUSD -> ETH/USD.
   if(asset_class == "Crypto")
      return CanonicalCryptoSymbol(provider_symbol);

   if(asset_class == "Metal")
   {
      string metal_symbol = provider_symbol;
      StringToUpper(metal_symbol);

      // Preserve the actual quote currency for metal crosses. IC Markets exposes
      // symbols such as XAGUSD, XAGAUD and XAGEUR; collapsing all of them to
      // XAG/USD causes the wrong live feed to overwrite the USD silver stream.
      string clean_metal = metal_symbol;
      int suffix_dot = StringFind(clean_metal, ".");
      if(suffix_dot > 0)
         clean_metal = StringSubstr(clean_metal, 0, suffix_dot);
      StringReplace(clean_metal, "_", "");
      StringReplace(clean_metal, "-", "");

      string metal_base = "";
      if(StringFind(clean_metal, "GOLD") == 0 || StringFind(clean_metal, "XAU") == 0)
         metal_base = "XAU";
      else if(StringFind(clean_metal, "SILVER") == 0 || StringFind(clean_metal, "XAG") == 0)
         metal_base = "XAG";

      if(metal_base != "")
      {
         string quote = profit;
         if(StringLen(clean_metal) >= 6)
         {
            string suffix_quote = StringSubstr(clean_metal, StringLen(clean_metal) - 3, 3);
            if(IsCurrencyCode(suffix_quote))
               quote = suffix_quote;
         }
         if(quote == "" || !IsCurrencyCode(quote))
            quote = "USD";
         return metal_base + "/" + quote;
      }
   }

   if(asset_class == "Forex" && base != "" && profit != "")
      return base + "/" + profit;

   string canonical = provider_symbol;
   StringToUpper(canonical);

   // Equity/ETF symbols carry useful market identity after the final dot. Keep it,
   // but remove the broker's _CFD marker: AAPL_CFD.US -> AAPL.US.
   if(asset_class == "EquityCFD" || asset_class == "EtfCFD")
   {
      StringReplace(canonical, "_CFD", "");
      return canonical;
   }

   // For broker suffixes such as .pro, retain the meaningful instrument name only.
   int dot = StringFind(canonical, ".");
   if(dot > 0)
      canonical = StringSubstr(canonical, 0, dot);
   return canonical;
}


string CanonicalCryptoSymbol(string provider_symbol)
{
   string canonical = provider_symbol;
   StringToUpper(canonical);

   int dot = StringFind(canonical, ".");
   if(dot > 0)
      canonical = StringSubstr(canonical, 0, dot);
   StringReplace(canonical, "_CFD", "");

   string quote_codes[] = {"USDT", "USDC", "USD", "EUR", "GBP", "JPY"};
   int count = ArraySize(quote_codes);
   for(int i = 0; i < count; i++)
   {
      string quote = quote_codes[i];
      int quote_length = StringLen(quote);
      int symbol_length = StringLen(canonical);
      if(symbol_length <= quote_length)
         continue;

      int offset = symbol_length - quote_length;
      if(StringSubstr(canonical, offset, quote_length) == quote)
         return StringSubstr(canonical, 0, offset) + "/" + quote;
   }

   return canonical;
}

string JsonNumber(double value)
{
   if(!MathIsValidNumber(value))
      return "0";
   return DoubleToString(value, 8);
}

void SendCurrentTicks()
{
   string items = "";
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      MqlTick tick;
      string symbol = g_symbols[i];
      if(!SymbolInfoTick(symbol, tick) || tick.bid <= 0 || tick.ask <= 0)
         continue;

      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      if(items != "") items += ",";
      items += StringFormat(
         "{\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"timeUtc\":\"%s\",\"bid\":%s,\"ask\":%s,\"last\":%s,\"volume\":%I64u}",
         JsonEscape(symbol), JsonEscape(CanonicalSymbol(symbol)), IsoUtc(BrokerTimeToUtc((datetime)tick.time)),
         DoubleToString(tick.bid, digits), DoubleToString(tick.ask, digits),
         DoubleToString(tick.last > 0 ? tick.last : (tick.bid + tick.ask) / 2.0, digits), tick.volume);
   }

   if(items == "")
      return;

   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"ticks\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), items);
   PostJson("/api/market-data/ingest/mt5/ticks", json);
}

void SendCompletedMinuteBars()
{
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      datetime current_open = iTime(g_symbols[i], PERIOD_M1, 0);
      if(current_open <= 0 || current_open == g_last_m1_bar[i])
         continue;

      // Send exactly the newly completed minute. The server deduplicates as a second
      // safety net when more than one bridge instance is accidentally attached.
      int copied = 0;
      if(SendCandles(g_symbols[i], PERIOD_M1, "1m", 1, 1, copied))
         g_last_m1_bar[i] = current_open;
   }
}

int BackfillTargetBars(string interval)
{
   if(interval == "1d")
      return MathMax(10, InpBackfillBarsD1);
   if(interval == "1h")
      return MathMax(10, InpBackfillBarsH1);
   if(interval == "15m")
      return MathMax(10, InpBackfillBarsM15);
   if(interval == "1m")
      return MathMax(10, InpBackfillBarsM1);
   return 500;
}

void ResetBackfillProgress()
{
   g_backfill_attempt_count = 0;
   g_backfill_stable_count = 0;
   g_backfill_last_count = -1;
   g_backfill_last_oldest = 0;
   g_backfill_next_attempt = 0;
}

bool IsSeriesSynchronized(string symbol, ENUM_TIMEFRAMES timeframe)
{
   long synchronized = 0;
   if(!SeriesInfoInteger(symbol, timeframe, SERIES_SYNCHRONIZED, synchronized))
      return false;
   return synchronized != 0;
}

void AdvanceBackfillJob()
{
   ResetBackfillProgress();

   // Finish the current provider timeframe for every enabled instrument before
   // moving to the next: 1d -> 1h -> 15m -> 1m.
   g_backfill_symbol_index++;
   if(g_backfill_symbol_index >= ArraySize(g_symbols))
   {
      g_backfill_symbol_index = 0;
      g_backfill_interval_index++;
      if(g_backfill_interval_index >= ArraySize(g_timeframes))
      {
         g_backfill_interval_index = 0;
         g_backfill_enabled = false;
         Print("Axetos MT5 Bridge: progressive historical backfill completed.");
      }
   }
}

void SendNextBackfillJob()
{
   if(ArraySize(g_symbols) == 0 || !g_backfill_enabled)
      return;

   datetime now = TimeCurrent();
   if(g_backfill_next_attempt > 0 && now < g_backfill_next_attempt)
      return;

   string symbol = g_symbols[g_backfill_symbol_index];
   ENUM_TIMEFRAMES timeframe = g_timeframes[g_backfill_interval_index];
   string interval = g_intervals[g_backfill_interval_index];
   int target = BackfillTargetBars(interval);
   int copied = 0;
   datetime oldest = 0;
   bool sent = SendCandlesWithCoverage(symbol, timeframe, interval, target, 1, copied, oldest);
   g_backfill_attempt_count++;

   bool progressed = false;
   if(sent && copied > 0)
   {
      progressed = (g_backfill_last_count < 0 || copied > g_backfill_last_count ||
                    (oldest > 0 && (g_backfill_last_oldest == 0 || oldest < g_backfill_last_oldest)));

      if(progressed)
      {
         PrintFormat("Axetos MT5 Bridge: progressive backfill %s %s expanded to %d bars; oldest=%s.",
                     symbol, interval, copied, TimeToString(oldest, TIME_DATE|TIME_MINUTES));
         g_backfill_stable_count = 0;
      }
      else
      {
         g_backfill_stable_count++;
         PrintFormat("Axetos MT5 Bridge: progressive backfill %s %s unchanged at %d bars (%d/%d stable checks).",
                     symbol, interval, copied, g_backfill_stable_count, MathMax(1, InpBackfillStableChecks));
      }

      g_backfill_last_count = copied;
      if(oldest > 0)
         g_backfill_last_oldest = oldest;
   }
   else
   {
      g_backfill_stable_count++;
      PrintFormat("Axetos MT5 Bridge: progressive backfill %s %s returned no bars (%d/%d stable checks).",
                  symbol, interval, g_backfill_stable_count, MathMax(1, InpBackfillStableChecks));
   }

   bool target_reached = sent && copied >= target;
   bool provider_limit_reached = g_backfill_stable_count >= MathMax(1, InpBackfillStableChecks) &&
                                 IsSeriesSynchronized(symbol, timeframe);
   bool attempt_limit_reached = g_backfill_attempt_count >= MathMax(1, InpBackfillMaxAttempts);

   if(target_reached || provider_limit_reached || attempt_limit_reached)
   {
      string reason = target_reached ? "target reached" :
                      (provider_limit_reached ? "provider limit reached" : "attempt limit reached");
      PrintFormat("Axetos MT5 Bridge: historical backfill %s %s completed with %d bars; oldest=%s; %s.",
                  symbol, interval, MathMax(0, copied),
                  oldest > 0 ? TimeToString(oldest, TIME_DATE|TIME_MINUTES) : "unknown", reason);
      AdvanceBackfillJob();
      return;
   }

   // Give MT5 time to request and append older bars before polling this exact
   // symbol/timeframe again. Live one-second ticks continue independently.
   g_backfill_next_attempt = now + MathMax(1, InpBackfillRetrySeconds);
}

bool SendCandles(string symbol, ENUM_TIMEFRAMES timeframe, string interval, int count, int start_pos, int &copied_out)
{
   datetime oldest = 0;
   return SendCandlesWithCoverage(symbol, timeframe, interval, count, start_pos, copied_out, oldest);
}

bool SendCandlesWithCoverage(string symbol, ENUM_TIMEFRAMES timeframe, string interval, int count, int start_pos,
                             int &copied_out, datetime &oldest_out)
{
   copied_out = 0;
   oldest_out = 0;
   MqlRates rates[];
   ArraySetAsSeries(rates, true);
   ResetLastError();
   int copied = CopyRates(symbol, timeframe, start_pos, count, rates);
   copied_out = copied;
   if(copied <= 0)
   {
      PrintFormat("Axetos MT5 Bridge: CopyRates failed for %s %s (%d).", symbol, interval, GetLastError());
      ResetLastError();
      return false;
   }

   oldest_out = rates[copied - 1].time;
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   string items = "";
   for(int i = copied - 1; i >= 0; i--)
   {
      if(items != "") items += ",";
      items += StringFormat(
         "{\"timeUtc\":\"%s\",\"open\":%s,\"high\":%s,\"low\":%s,\"close\":%s,\"tickVolume\":%I64d}",
         IsoUtc(CandleTimeToUtc(rates[i].time, interval)),
         DoubleToString(rates[i].open, digits),
         DoubleToString(rates[i].high, digits),
         DoubleToString(rates[i].low, digits),
         DoubleToString(rates[i].close, digits),
         rates[i].tick_volume);
   }

   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"interval\":\"%s\",\"candles\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), JsonEscape(symbol),
      JsonEscape(CanonicalSymbol(symbol)), interval, items);
   return PostJson("/api/market-data/ingest/mt5/candles", json);
}


bool SendCandlesForDateRange(string symbol, ENUM_TIMEFRAMES timeframe, string interval,
                             string start_date, string end_date, int &copied_out)
{
   copied_out = 0;

   string normalized_start = start_date;
   string normalized_end = end_date;
   StringReplace(normalized_start, "-", ".");
   StringReplace(normalized_end, "-", ".");

   datetime from_time = StringToTime(normalized_start + " 00:00:00");
   datetime to_time = StringToTime(normalized_end + " 23:59:59");
   if(from_time <= 0 || to_time <= 0 || to_time < from_time)
   {
      PrintFormat("Axetos MT5 Bridge: invalid targeted repair range %s through %s for %s %s.",
                  start_date, end_date, symbol, interval);
      return false;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   ResetLastError();
   int copied = CopyRates(symbol, timeframe, from_time, to_time, rates);
   int copy_error = GetLastError();
   copied_out = (copied > 0 ? copied : 0);
   if(copied <= 0)
   {
      // CopyRates can legitimately return no bars with error 0 when the provider
      // has no candle for the bounded range (for example a crypto-history hole
      // or an exchange closure). Report that as a completed source attempt so
      // the server preserves the evidence and tries the next configured source
      // instead of reissuing the same request every polling cycle.
      if(copy_error == 0)
      {
         PrintFormat("Axetos MT5 Bridge: targeted repair %s %s %s through %s completed with no bars.",
                     symbol, interval, start_date, end_date);
         ResetLastError();
         return true;
      }

      PrintFormat("Axetos MT5 Bridge: targeted CopyRates failed for %s %s, %s through %s (%d).",
                  symbol, interval, start_date, end_date, copy_error);
      ResetLastError();
      return false;
   }

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   string items = "";
   for(int i = 0; i < copied; i++)
   {
      if(items != "") items += ",";
      items += StringFormat(
         "{\"timeUtc\":\"%s\",\"open\":%s,\"high\":%s,\"low\":%s,\"close\":%s,\"tickVolume\":%I64d}",
         IsoUtc(CandleTimeToUtc(rates[i].time, interval)),
         DoubleToString(rates[i].open, digits),
         DoubleToString(rates[i].high, digits),
         DoubleToString(rates[i].low, digits),
         DoubleToString(rates[i].close, digits),
         rates[i].tick_volume);
   }

   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"interval\":\"%s\",\"candles\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), JsonEscape(symbol),
      JsonEscape(CanonicalSymbol(symbol)), interval, items);
   return PostJson("/api/market-data/ingest/mt5/candles", json);
}


void RefreshRepairRequest()
{
   string path = "/api/market-data/mt5/repair-request.txt?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id;
   string response = "";
   if(!GetText(path, response))
      return;

   StringTrimLeft(response);
   StringTrimRight(response);
   if(response == "")
      return;

   string parts[];
   int part_count = StringSplit(response, '|', parts);
   if(part_count != 5)
   {
      // Old two-field requests caused the EA to upload an entire historical
      // block repeatedly. The server now issues bounded five-field requests;
      // ignore stale legacy requests instead of replaying full history.
      PrintFormat("Axetos MT5 Bridge: ignored legacy/unbounded repair request '%s'.", response);
      return;
   }

   string symbol = parts[0];
   string interval = parts[1];
   string start_date = parts[2];
   string end_date = parts[3];
   string request_id = parts[4];
   StringTrimLeft(symbol); StringTrimRight(symbol);
   StringTrimLeft(interval); StringTrimRight(interval);
   StringTrimLeft(start_date); StringTrimRight(start_date);
   StringTrimLeft(end_date); StringTrimRight(end_date);
   StringTrimLeft(request_id); StringTrimRight(request_id);

   if(interval == "1wk" || interval == "1mo")
      interval = "1d";

   ENUM_TIMEFRAMES timeframe = PERIOD_CURRENT;
   if(interval == "1m") timeframe = PERIOD_M1;
   else if(interval == "15m") timeframe = PERIOD_M15;
   else if(interval == "1h") timeframe = PERIOD_H1;
   else if(interval == "1d") timeframe = PERIOD_D1;
   else return;

   string resolved = ResolveProviderSymbol(symbol);
   bool completed = false;
   int copied = 0;
   if(resolved != "" && SymbolSelect(resolved, true))
      completed = SendCandlesForDateRange(resolved, timeframe, interval, start_date, end_date, copied);

   string result_path = "/api/market-data/mt5/repair-result?providerKey=" + InpProviderKey +
                        "&terminalInstanceId=" + g_terminal_id +
                        "&providerSymbol=" + symbol +
                        "&interval=" + interval +
                        "&completed=" + (completed ? "true" : "false") +
                        "&requestId=" + request_id;
   PostJson(result_path, "{}");

   if(completed)
      PrintFormat("Axetos MT5 Bridge: targeted repair %s %s %s through %s submitted %d bar(s); request=%s.",
                  resolved, interval, start_date, end_date, copied, request_id);
   else
      PrintFormat("Axetos MT5 Bridge: targeted repair %s %s %s through %s failed; request=%s.",
                  symbol, interval, start_date, end_date, request_id);
}

void RefreshServerSelection()
{
   string path = "/api/market-data/mt5/enabled-symbols.txt?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id;
   string response = "";
   if(!GetText(path, response))
      return;

   StringTrimLeft(response);
   StringTrimRight(response);
   g_last_selection_refresh = TimeCurrent();
   if(response == "")
   {
      Print("Axetos MT5 Bridge: server selection is empty; retaining the current stream to avoid an accidental outage.");
      return;
   }

   string requested[];
   int count = StringSplit(response, ',', requested);
   if(count <= 0)
      return;

   string resolved[];
   ArrayResize(resolved, count);
   int accepted = 0;
   for(int i = 0; i < count; i++)
   {
      string candidate = requested[i];
      StringTrimLeft(candidate);
      StringTrimRight(candidate);
      if(candidate == "")
         continue;

      string provider_symbol = ResolveProviderSymbol(candidate);
      if(provider_symbol == "" || !SymbolSelect(provider_symbol, true))
         continue;
      resolved[accepted++] = provider_symbol;
   }
   ArrayResize(resolved, accepted);
   if(accepted <= 0)
      return;

   string old_key = JoinedSymbols(g_symbols);
   string new_key = JoinedSymbols(resolved);
   if(old_key == new_key)
      return;

   ArrayResize(g_symbols, accepted);
   ArrayResize(g_last_m1_bar, accepted);
   for(int i = 0; i < accepted; i++)
   {
      g_symbols[i] = resolved[i];
      g_last_m1_bar[i] = iTime(resolved[i], PERIOD_M1, 0);
   }

   g_backfill_symbol_index = 0;
   g_backfill_interval_index = 0;
   ResetBackfillProgress();
   g_backfill_enabled = InpSendHistoricalBars;
   PrintFormat("Axetos MT5 Bridge: server selection applied; streaming-symbols=%d.", accepted);
}

string JoinedSymbols(string &symbols[])
{
   string value = "";
   for(int i = 0; i < ArraySize(symbols); i++)
   {
      if(value != "") value += ",";
      value += symbols[i];
   }
   return value;
}

bool GetText(string path, string &response)
{
   string url = InpServerUrl + path;
   string headers = "Accept: text/plain\r\n";
   if(InpBridgeToken != "")
      headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";

   char data[];
   char result[];
   string result_headers;
   ArrayResize(data, 0);
   ResetLastError();
   int status = WebRequest("GET", url, headers, InpRequestTimeoutMs, data, result, result_headers);
   response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   if(status >= 200 && status < 300)
      return true;

   PrintFormat("Axetos MT5 Bridge: GET %s failed. HTTP=%d error=%d response=%s",
               path, status, GetLastError(), response);
   return false;
}

bool PostJson(string path, string payload)
{
   string url = InpServerUrl + path;
   string headers = "Content-Type: application/json\r\n";
   if(InpBridgeToken != "")
      headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";

   char data[];
   char result[];
   string result_headers;
   int length = StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(length > 0)
      ArrayResize(data, length - 1);

   ResetLastError();
   int status = WebRequest("POST", url, headers, InpRequestTimeoutMs, data, result, result_headers);
   if(status >= 200 && status < 300)
      return true;

   string response = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   PrintFormat("Axetos MT5 Bridge: POST %s failed. HTTP=%d error=%d response=%s",
               path, status, GetLastError(), response);
   return false;
}

string CanonicalSymbol(string provider_symbol)
{
   string description = SymbolInfoString(provider_symbol, SYMBOL_DESCRIPTION);
   string path = SymbolInfoString(provider_symbol, SYMBOL_PATH);
   string currency_base = SymbolInfoString(provider_symbol, SYMBOL_CURRENCY_BASE);
   string currency_profit = SymbolInfoString(provider_symbol, SYMBOL_CURRENCY_PROFIT);
   string asset_class = ClassifyAsset(provider_symbol, description, path, currency_base, currency_profit);
   return CanonicalDiscoveredSymbol(provider_symbol, asset_class, currency_base, currency_profit);
}

datetime BrokerTimeToUtc(datetime broker_time)
{
   // MqlTick.time and MqlRates.time are expressed in the broker trade-server
   // timezone. Appending a literal Z without conversion makes ICMarkets/Oanda
   // candles appear two or three hours in the future. Round the current offset
   // to 15-minute boundaries to avoid one-second sampling jitter.
   long raw_offset = (long)TimeTradeServer() - (long)TimeGMT();
   long rounded_offset = (long)MathRound((double)raw_offset / 900.0) * 900;
   if(MathAbs((double)rounded_offset) > 14.0 * 60.0 * 60.0)
      rounded_offset = 0;
   return (datetime)((long)broker_time - rounded_offset);
}

datetime CandleTimeToUtc(datetime broker_time, string interval)
{
   // Daily candles are canonical trading dates in AxetosOS. Preserve their
   // date key at 00:00 rather than shifting them to the previous UTC evening.
   if(interval == "1d")
      return broker_time;
   return BrokerTimeToUtc(broker_time);
}

string IsoUtc(datetime value)
{
   MqlDateTime parts;
   TimeToStruct(value, parts);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
                       parts.year, parts.mon, parts.day, parts.hour, parts.min, parts.sec);
}

string JsonEscape(string value)
{
   StringReplace(value, "\\", "\\\\");
   StringReplace(value, "\"", "\\\"");
   StringReplace(value, "\r", "\\r");
   StringReplace(value, "\n", "\\n");
   return value;
}
