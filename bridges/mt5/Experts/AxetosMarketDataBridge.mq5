#property copyright "AxetosOS"
#property version   "2.00"
#property strict
#property description "Thin MT5 command adapter for Axetos Market Data Server."

input string InpProviderKey = "Oanda.MT5";
input string InpServerUrl = "http://127.0.0.1:8000";
input string InpBridgeToken = "";
input bool InpDiscoverAllSymbols = true;
input int InpDiscoveryBatchSize = 75;
input int InpHeartbeatSeconds = 1;
input int InpSelectionRefreshSeconds = 15;
input int InpControlTimeoutMs = 1000;
input int InpLiveTimeoutMs = 1500;
input int InpRequestTimeoutMs = 5000;

string g_symbols[];
datetime g_last_heartbeat = 0;
datetime g_last_selection_refresh = 0;
string g_terminal_id = "";
int g_http_channel_failures[5];
datetime g_http_channel_last_log[5];
datetime g_tick_retry_after = 0;
bool g_tick_congested = false;
int g_tick_suppressed_batches = 0;
enum AXETOS_HTTP_CHANNEL { HTTP_CONTROL=0, HTTP_HEARTBEAT=1, HTTP_QUOTES=2, HTTP_CANDLES=3, HTTP_CATALOG=4 };

int OnInit()
{
   ArrayResize(g_symbols, 0);
   g_terminal_id = BuildTerminalId();
   EventSetTimer(1);
   string response = "";
   if(GetText("/api/live", response))
      PrintFormat("Axetos MT5 Bridge v2.00: transport self-test passed; server=%s", InpServerUrl);
   SendHeartbeat();
   if(InpDiscoverAllSymbols) SendDiscoveredInstrumentCatalogue();
   RefreshServerSelection();
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) { EventKillTimer(); }

void OnTimer()
{
   datetime now = TimeCurrent();
   if(g_last_heartbeat == 0 || now - g_last_heartbeat >= InpHeartbeatSeconds) SendHeartbeat();
   if(g_last_selection_refresh == 0 || now - g_last_selection_refresh >= InpSelectionRefreshSeconds) RefreshServerSelection();
   SendCurrentTicks();
   ExecuteServerCommand();
}

bool ExecuteServerCommand()
{
   string path = "/api/market-data/mt5/repair-request.txt?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id;
   string response = "";
   if(!GetText(path, response)) return false;
   StringTrimLeft(response); StringTrimRight(response);
   if(response == "") return false;
   string parts[];
   int count = StringSplit(response, '|', parts);
   if(count != 6) return false;
   string command = parts[0], provider_symbol = parts[1], interval = parts[2];
   string start_time = parts[3], end_time = parts[4], request_id = parts[5];
   string symbol = ResolveProviderSymbol(provider_symbol);
   ENUM_TIMEFRAMES timeframe = IntervalTimeframe(interval);
   if(symbol == "" || timeframe == PERIOD_CURRENT || !SymbolSelect(symbol, true))
   {
      ReportCommandResult(provider_symbol, interval, request_id, false, 0, 0, 0, 4301);
      return true;
   }
   if(command == "DISCOVER" || command == "AVAILABILITY")
   {
      datetime earliest=0, latest=0; int bars=0, error_code=0;
      bool ok = ProbeHistoryRange(symbol, timeframe, interval, start_time, end_time,
                                  earliest, latest, bars, error_code);
      string result = "/api/market-data/mt5/history-availability?providerKey=" + InpProviderKey +
                      "&requestId=" + request_id + "&candleCount=" + IntegerToString(bars);
      if(ok && bars > 0)
      {
         result += "&earliestUtc=" + IsoUtc(CandleTimeToUtc(earliest, interval));
         result += "&latestUtc=" + IsoUtc(CandleTimeToUtc(latest, interval));
      }
      string ignored=""; PostJsonText(result, "{}", ignored);
      return true;
   }
   if(command != "BACKFILL" && command != "FETCH") return true;
   int copied=0, stored=0, skipped=0, error_code=0;
   bool completed = SendCandlesForDateRange(symbol, timeframe, interval, start_time, end_time,
                                             copied, stored, skipped, error_code, request_id);
   ReportCommandResult(provider_symbol, interval, request_id, completed, copied, stored, skipped, error_code);
   return true;
}

void ReportCommandResult(string provider_symbol, string interval, string request_id, bool completed,
                         int received, int stored, int skipped, int error_code)
{
   bool unavailable = (!completed && error_code == 4401);
   string path = "/api/market-data/mt5/repair-result?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id +
                 "&providerSymbol=" + provider_symbol + "&interval=" + interval +
                 "&completed=" + (completed ? "true" : "false") +
                 "&barsReceived=" + IntegerToString(received) +
                 "&barsInserted=" + IntegerToString(stored) +
                 "&unavailable=" + (unavailable ? "true" : "false") +
                 "&errorCode=" + IntegerToString(error_code) +
                 "&requestId=" + request_id;
   string response="";
   PostJsonText(path, "{}", response);
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
      if(items != "") items += ",";
      items += StringFormat(
         "{\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"digits\":%d,\"point\":%s,\"isVisible\":%s,\"isSelected\":%s,\"displayName\":\"%s\",\"description\":\"%s\",\"path\":\"%s\"}",
         JsonEscape(symbol), JsonEscape(symbol),
         (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS),
         JsonNumber(SymbolInfoDouble(symbol, SYMBOL_POINT)),
         SymbolInfoInteger(symbol, SYMBOL_VISIBLE) ? "true" : "false",
         SymbolInfoInteger(symbol, SYMBOL_SELECT) ? "true" : "false",
         JsonEscape(symbol), JsonEscape(SymbolInfoString(symbol, SYMBOL_DESCRIPTION)),
         JsonEscape(SymbolInfoString(symbol, SYMBOL_PATH)));
   }
   string json = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"timeUtc\":\"%s\",\"instruments\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), IsoUtc(TimeGMT()), items);
   return PostJson("/api/market-data/ingest/mt5/instruments", json);
}

string JsonNumber(double value)
{
   if(!MathIsValidNumber(value)) return "0";
   return DoubleToString(value, 10);
}

void SendCurrentTicks()
{
   if(g_tick_retry_after > 0 && TimeLocal() < g_tick_retry_after)
   {
      g_tick_suppressed_batches++;
      return;
   }

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

bool SendCandlesForDateRange(string symbol, ENUM_TIMEFRAMES timeframe, string interval,
                             string start_date, string end_date, int &copied_out, int &stored_out,
                             int &skipped_out, int &error_out, string request_id)
{
   copied_out = 0;
   stored_out = 0;
   skipped_out = 0;
   error_out = 0;

   string normalized_start = start_date;
   string normalized_end = end_date;
   StringReplace(normalized_start, "T", " ");
   StringReplace(normalized_end, "T", " ");
   int plus_pos = StringFind(normalized_start, "+"); if(plus_pos > 0) normalized_start = StringSubstr(normalized_start, 0, plus_pos);
   plus_pos = StringFind(normalized_end, "+"); if(plus_pos > 0) normalized_end = StringSubstr(normalized_end, 0, plus_pos);
   StringReplace(normalized_start, "-", ".");
   StringReplace(normalized_end, "-", ".");

   datetime from_time = StringToTime(normalized_start);
   datetime to_time = StringToTime(normalized_end);
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
   error_out = copy_error;
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

   // Keep each HTTP request small enough to complete comfortably inside the
   // bridge timeout. The full provider operation remains in flight until every
   // chunk has received a valid server storage acknowledgement.
   const int upload_chunk_size = 100;
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   int chunk_count = (copied + upload_chunk_size - 1) / upload_chunk_size;
   for(int chunk_index = 0; chunk_index < chunk_count; chunk_index++)
   {
      // Keep provider liveness independent from long bulk uploads.
      datetime heartbeat_now = TimeCurrent();
      if(g_last_heartbeat == 0 || heartbeat_now - g_last_heartbeat >= InpHeartbeatSeconds)
         SendHeartbeat();

      int chunk_start = chunk_index * upload_chunk_size;
      int chunk_end = MathMin(copied, chunk_start + upload_chunk_size);
      string items = "";
      for(int i = chunk_start; i < chunk_end; i++)
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
         "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"interval\":\"%s\",\"requestId\":\"%s\",\"chunkIndex\":%d,\"chunkCount\":%d,\"candles\":[%s]}",
         JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), JsonEscape(symbol),
         JsonEscape(CanonicalSymbol(symbol)), interval, JsonEscape(request_id),
         chunk_index, chunk_count, items);
      string storage_response = "";
      PrintFormat("Axetos MT5 Bridge: uploading backfill chunk %d/%d for %s %s; bars=%d; request=%s.",
                  chunk_index + 1, chunk_count, symbol, interval, chunk_end - chunk_start, request_id);
      if(!PostJsonText("/api/market-data/ingest/mt5/candles", json, storage_response))
      {
         PrintFormat("Axetos MT5 Bridge: backfill chunk %d/%d was not acknowledged; request=%s remains in flight.",
                     chunk_index + 1, chunk_count, request_id);
         return false;
      }

      int chunk_stored = JsonIntegerField(storage_response, "stored", -1);
      int chunk_skipped = JsonIntegerField(storage_response, "skipped", -1);
      if(chunk_stored < 0 || chunk_skipped < 0 || chunk_stored + chunk_skipped != chunk_end - chunk_start)
      {
         PrintFormat("Axetos MT5 Bridge: invalid candle-storage acknowledgement for chunk %d/%d of %s %s: %s",
                     chunk_index + 1, chunk_count, symbol, interval, storage_response);
         return false;
      }
      stored_out += chunk_stored;
      skipped_out += chunk_skipped;
      PrintFormat("Axetos MT5 Bridge: chunk %d/%d acknowledged for %s %s; stored=%d, skipped=%d.",
                  chunk_index + 1, chunk_count, symbol, interval, chunk_stored, chunk_skipped);
   }

   PrintFormat("Axetos MT5 Bridge: server stored %d and skipped %d of %d bars for %s %s.",
               stored_out, skipped_out, copied_out, symbol, interval);
   return true;
}


bool FindEarliestRetrievableM1(string symbol, datetime &earliest_out)
{
   earliest_out = 0;
   long advertised = 0;
   if(!SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SERVER_FIRSTDATE, advertised) || advertised <= 0)
      return false;

   datetime probe_start = (datetime)advertised;
   datetime now_time = TimeCurrent();
   const int probe_days = 3;
   const int max_probes = 4096;
   for(int probe = 0; probe < max_probes && probe_start <= now_time; probe++)
   {
      datetime probe_end = probe_start + probe_days * 86400 - 60;
      if(probe_end > now_time) probe_end = now_time;
      MqlRates rates[];
      ArraySetAsSeries(rates, false);
      ResetLastError();
      int copied = CopyRates(symbol, PERIOD_M1, probe_start, probe_end, rates);
      int copy_error = GetLastError();
      if(copied > 0)
      {
         earliest_out = rates[0].time;
         ResetLastError();
         return true;
      }

      // 4401 means the broker has no history in this range. Advance the probe
      // instead of advertising an unusable SERIES_SERVER_FIRSTDATE boundary.
      if(copy_error != 0 && copy_error != 4401)
      {
         PrintFormat("Axetos MT5 Bridge: availability probe failed for %s, %s through %s (%d).",
                     symbol, TimeToString(probe_start, TIME_DATE|TIME_MINUTES),
                     TimeToString(probe_end, TIME_DATE|TIME_MINUTES), copy_error);
      }
      probe_start = probe_end + 60;
      ResetLastError();
   }
   return false;
}



ENUM_TIMEFRAMES IntervalTimeframe(string interval)
{
   if(interval == "1m") return PERIOD_M1;
   if(interval == "1h") return PERIOD_H1;
   if(interval == "1d") return PERIOD_D1;
   return PERIOD_CURRENT;
}

bool ProbeHistoryRange(string symbol, ENUM_TIMEFRAMES timeframe, string interval,
                       string start_date, string end_date,
                       datetime &earliest_out, datetime &latest_out, int &count_out, int &error_out)
{
   earliest_out = 0; latest_out = 0; count_out = 0; error_out = 0;
   string normalized_start = start_date;
   string normalized_end = end_date;
   StringReplace(normalized_start, "T", " ");
   StringReplace(normalized_end, "T", " ");
   int plus_pos = StringFind(normalized_start, "+"); if(plus_pos > 0) normalized_start = StringSubstr(normalized_start, 0, plus_pos);
   plus_pos = StringFind(normalized_end, "+"); if(plus_pos > 0) normalized_end = StringSubstr(normalized_end, 0, plus_pos);
   StringReplace(normalized_start, "-", ".");
   StringReplace(normalized_end, "-", ".");
   datetime from_time = StringToTime(normalized_start);
   datetime to_time = StringToTime(normalized_end);
   if(from_time <= 0 || to_time <= 0 || to_time < from_time) return false;

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   ResetLastError();
   int copied = CopyRates(symbol, timeframe, from_time, to_time, rates);
   error_out = GetLastError();
   if(copied <= 0)
   {
      ResetLastError();
      return true; // confirmed zero availability is a valid probe result
   }
   count_out = copied;
   earliest_out = rates[0].time;
   latest_out = rates[copied - 1].time;
   ResetLastError();
   return true;
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
      // Empty means the server has intentionally selected no symbols. Stop all
      // streaming rather than retaining stale bridge-local subscriptions.
      for(int i = 0; i < ArraySize(g_symbols); i++)
         SymbolSelect(g_symbols[i], false);
      ArrayResize(g_symbols, 0);
      Print("Axetos MT5 Bridge: server selection is empty; streaming stopped.");
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

   // Deselect anything the server removed before applying the exact new set.
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      bool keep = false;
      for(int j = 0; j < accepted; j++)
      {
         if(g_symbols[i] == resolved[j])
         {
            keep = true;
            break;
         }
      }
      if(!keep)
         SymbolSelect(g_symbols[i], false);
   }

   ArrayResize(g_symbols, accepted);
   for(int i = 0; i < accepted; i++)
   {
      g_symbols[i] = resolved[i];
   }

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

int HttpChannelForPath(string path)
{
   if(path == "/api/market-data/ingest/mt5/heartbeat")
      return HTTP_HEARTBEAT;
   if(StringFind(path, "/api/market-data/mt5/repair-request.txt") == 0 ||
      StringFind(path, "/api/market-data/mt5/history-") == 0)
      return HTTP_CONTROL;
   if(path == "/api/market-data/ingest/mt5/ticks")
      return HTTP_QUOTES;
   if(path == "/api/market-data/ingest/mt5/candles")
      return HTTP_CANDLES;
   return HTTP_CATALOG;
}

string HttpChannelName(int channel)
{
   if(channel == HTTP_CONTROL) return "control";
   if(channel == HTTP_HEARTBEAT) return "heartbeat";
   if(channel == HTTP_QUOTES) return "quotes";
   if(channel == HTTP_CANDLES) return "candles";
   return "catalog";
}

int HttpTimeoutForPath(string path)
{
   int channel = HttpChannelForPath(path);
   if(channel == HTTP_CONTROL || channel == HTTP_HEARTBEAT)
      return MathMax(250, InpControlTimeoutMs);
   if(channel == HTTP_QUOTES || channel == HTTP_CATALOG)
      return MathMax(500, InpLiveTimeoutMs);
   return MathMax(1000, InpRequestTimeoutMs);
}

void RecordHttpSuccess(string path)
{
   int channel = HttpChannelForPath(path);
   if(g_http_channel_failures[channel] > 0)
      PrintFormat("Axetos MT5 Bridge: %s channel restored after %d failed attempt(s).",
                  HttpChannelName(channel), g_http_channel_failures[channel]);
   g_http_channel_failures[channel] = 0;
   g_http_channel_last_log[channel] = 0;
}

void RecordHttpApplicationFailure(string method, string path, int status, int error_code, string response)
{
   // An HTTP response proves transport availability. Rejecting one channel must not
   // alter scheduling or suppress any other bridge responsibility.
   RecordHttpSuccess(path);
   PrintFormat("Axetos MT5 Bridge: %s %s rejected. HTTP=%d error=%d response=%s; continuing other requests.",
               method, path, status, error_code, response);
}

void RecordHttpFailure(string method, string path, int status, int error_code, string response)
{
   int channel = HttpChannelForPath(path);
   g_http_channel_failures[channel]++;

   // Failures are channel-local and never create a shared retry gate. Throttle only
   // the Journal message; heartbeat, quotes, completed M1, catalogue and repair
   // polling continue on their own schedules.
   datetime now = TimeLocal();
   if(g_http_channel_last_log[channel] == 0 || now - g_http_channel_last_log[channel] >= 30)
   {
      PrintFormat("Axetos MT5 Bridge: %s channel request failed: %s %s HTTP=%d error=%d response=%s; other channels continue.",
                  HttpChannelName(channel), method, path, status, error_code, response);
      g_http_channel_last_log[channel] = now;
   }
}

bool IsTickBackpressure(string path, int status, string response)
{
   if(path != "/api/market-data/ingest/mt5/ticks")
      return false;
   if(status == 429)
      return true;
   return status == 503 && (StringFind(response, "queue") >= 0 || StringFind(response, "saturated") >= 0);
}

void RecordTickBackpressure()
{
   g_tick_retry_after = TimeLocal() + 5;
   if(!g_tick_congested)
      Print("Axetos MT5 Bridge: live ingestion congested; pausing tick submissions for 5s and retaining only fresh snapshots.");
   g_tick_congested = true;
}

void RecordTickSuccess()
{
   if(g_tick_congested)
      PrintFormat("Axetos MT5 Bridge: live ingestion resumed; %d stale tick batch(es) suppressed.", g_tick_suppressed_batches);
   g_tick_congested = false;
   g_tick_retry_after = 0;
   g_tick_suppressed_batches = 0;
}

bool GetText(string path, string &response)
{
   response = "";
   string url = InpServerUrl + path;
   string headers = "Accept: text/plain\r\n";
   if(InpBridgeToken != "")
      headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";

   char data[];
   char result[];
   string result_headers;
   ArrayResize(data, 0);
   ArrayResize(result, 0);
   ResetLastError();

   int status;
   if(InpBridgeToken == "")
      status = WebRequest("GET", url, NULL, NULL, HttpTimeoutForPath(path), data, 0, result, result_headers);
   else
      status = WebRequest("GET", url, headers, HttpTimeoutForPath(path), data, result, result_headers);

   int request_error = GetLastError();
   if(ArraySize(result) > 0)
      response = CharArrayToString(result, 0, ArraySize(result), CP_UTF8);

   if(status >= 200 && status < 300)
   {
      RecordHttpSuccess(path);
      return true;
   }

   if(status < 0 || status >= 500)
      RecordHttpFailure("GET", path, status, request_error, response);
   else
      RecordHttpApplicationFailure("GET", path, status, request_error, response);
   return false;
}

int JsonIntegerField(string json, string field, int fallback)
{
   string needle = "\"" + field + "\":";
   int pos = StringFind(json, needle);
   if(pos < 0) return fallback;
   pos += StringLen(needle);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == 32) pos++;
   int end = pos;
   if(end < StringLen(json) && StringGetCharacter(json, end) == 45) end++;
   while(end < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, end);
      if(ch < 48 || ch > 57) break;
      end++;
   }
   if(end <= pos) return fallback;
   return (int)StringToInteger(StringSubstr(json, pos, end - pos));
}

bool PostJsonText(string path, string payload, string &response)
{
   string url = InpServerUrl + path;
   string headers = "Content-Type: application/json\r\nAccept: text/plain, application/json\r\n";
   if(InpBridgeToken != "")
      headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";

   char data[];
   char result[];
   string result_headers;
   int length = StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(length > 0)
      ArrayResize(data, length - 1);
   ArrayResize(result, 0);

   ResetLastError();
   int status = WebRequest("POST", url, headers, HttpTimeoutForPath(path), data, result, result_headers);
   int request_error = GetLastError();

   response = "";
   if(ArraySize(result) > 0)
      response = CharArrayToString(result, 0, ArraySize(result), CP_UTF8);

   if(status >= 200 && status < 300)
   {
      RecordHttpSuccess(path);
      if(path == "/api/market-data/ingest/mt5/ticks")
         RecordTickSuccess();
      return true;
   }

   if(IsTickBackpressure(path, status, response))
      RecordTickBackpressure();
   else if(status < 0 || status >= 500)
      RecordHttpFailure("POST", path, status, request_error, response);
   else
      RecordHttpApplicationFailure("POST", path, status, request_error, response);
   return false;
}

bool PostJson(string path, string payload)
{
   string url = InpServerUrl + path;
   string headers = "Content-Type: application/json\r\nAccept: application/json\r\n";
   if(InpBridgeToken != "")
      headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";

   char data[];
   char result[];
   string result_headers;
   int length = StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(length > 0)
      ArrayResize(data, length - 1);
   ArrayResize(result, 0);

   ResetLastError();
   int status = WebRequest("POST", url, headers, HttpTimeoutForPath(path), data, result, result_headers);
   int request_error = GetLastError();

   string response = "";
   if(ArraySize(result) > 0)
      response = CharArrayToString(result, 0, ArraySize(result), CP_UTF8);

   if(status >= 200 && status < 300)
   {
      RecordHttpSuccess(path);
      if(path == "/api/market-data/ingest/mt5/ticks")
         RecordTickSuccess();
      return true;
   }

   if(IsTickBackpressure(path, status, response))
      RecordTickBackpressure();
   else if(status < 0 || status >= 500)
      RecordHttpFailure("POST", path, status, request_error, response);
   else
      RecordHttpApplicationFailure("POST", path, status, request_error, response);
   return false;
}

string CanonicalSymbol(string provider_symbol) { return provider_symbol; }

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
