#property copyright "AxetosOS"
#property version   "3.06"
#property strict
#property description "Passive MT5 adapter controlled entirely by Axetos Market Data Server."

input string InpProviderKey = "Oanda.MT5";
input string InpServerUrl = "http://127.0.0.1:8000";
input string InpBridgeToken = "";
input int InpHeartbeatSeconds = 1;
input int InpSelectionRefreshSeconds = 15;
input int InpControlTimeoutMs = 1500;
input int InpUploadTimeoutMs = 5000;
input int InpUploadChunkSize = 100;
input bool InpLogCommands = true;

string g_terminal_id = "";
string g_tick_symbols[];
datetime g_last_heartbeat = 0;
datetime g_last_selection = 0;

int OnInit()
{
   g_terminal_id = BuildTerminalId();
   ArrayResize(g_tick_symbols, 0);
   EventSetTimer(1);
   SendHeartbeat();
   RefreshTickSymbols();
   PrintFormat("Axetos MT5 Bridge v3.06 started; provider=%s server=%s", InpProviderKey, InpServerUrl);
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
   if(g_last_selection == 0 || now - g_last_selection >= InpSelectionRefreshSeconds)
      RefreshTickSymbols();
   SendTicks();
   PollCommand();
}

void SendHeartbeat()
{
   string payload = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"brokerName\":\"%s\",\"serverName\":\"%s\",\"accountLogin\":%I64d,\"timeUtc\":\"%s\"}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id),
      JsonEscape(AccountInfoString(ACCOUNT_COMPANY)), JsonEscape(AccountInfoString(ACCOUNT_SERVER)),
      AccountInfoInteger(ACCOUNT_LOGIN), IsoUtc(TimeGMT()));
   string response = "";
   if(HttpPost("/api/market-data/ingest/mt5/heartbeat", payload, InpControlTimeoutMs, response))
      g_last_heartbeat = TimeCurrent();
}

void RefreshTickSymbols()
{
   string path = "/api/market-data/mt5/enabled-symbols.txt?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id;
   string response = "";
   if(!HttpGet(path, InpControlTimeoutMs, response))
      return;
   StringTrimLeft(response);
   StringTrimRight(response);
   g_last_selection = TimeCurrent();

   string requested[];
   int count = response == "" ? 0 : StringSplit(response, ',', requested);
   string resolved[];
   ArrayResize(resolved, MathMax(0, count));
   int accepted = 0;
   for(int i = 0; i < count; i++)
   {
      StringTrimLeft(requested[i]);
      StringTrimRight(requested[i]);
      string symbol = ResolveSymbol(requested[i]);
      if(symbol != "" && SymbolSelect(symbol, true))
         resolved[accepted++] = symbol;
   }
   ArrayResize(resolved, accepted);

   for(int i = 0; i < ArraySize(g_tick_symbols); i++)
   {
      bool keep = false;
      for(int j = 0; j < accepted; j++)
         if(g_tick_symbols[i] == resolved[j]) { keep = true; break; }
      if(!keep) SymbolSelect(g_tick_symbols[i], false);
   }

   ArrayResize(g_tick_symbols, accepted);
   for(int i = 0; i < accepted; i++) g_tick_symbols[i] = resolved[i];
}

void SendTicks()
{
   string items = "";
   for(int i = 0; i < ArraySize(g_tick_symbols); i++)
   {
      MqlTick tick;
      string symbol = g_tick_symbols[i];
      if(!SymbolInfoTick(symbol, tick) || tick.bid <= 0 || tick.ask <= 0)
         continue;
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      if(items != "") items += ",";
      items += StringFormat(
         "{\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"timeUtc\":\"%s\",\"bid\":%s,\"ask\":%s,\"last\":%s,\"volume\":%I64u}",
         JsonEscape(symbol), JsonEscape(symbol), IsoUtc((datetime)tick.time),
         DoubleToString(tick.bid, digits), DoubleToString(tick.ask, digits),
         DoubleToString(tick.last > 0 ? tick.last : (tick.bid + tick.ask) / 2.0, digits), tick.volume);
   }
   if(items == "") return;

   string payload = StringFormat(
      "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"ticks\":[%s]}",
      JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), items);
   string response = "";
   HttpPost("/api/market-data/ingest/mt5/ticks", payload, InpControlTimeoutMs, response);
}

void PollCommand()
{
   string path = "/api/market-data/mt5/repair-request.txt?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id;
   string command_text = "";
   if(!HttpGet(path, InpControlTimeoutMs, command_text)) return;
   StringTrimLeft(command_text);
   StringTrimRight(command_text);
   if(command_text == "") return;

   string fields[];
   if(StringSplit(command_text, '|', fields) != 6)
   {
      PrintFormat("Axetos MT5 Bridge: invalid server command: %s", command_text);
      return;
   }
   if(InpLogCommands)
      PrintFormat("Axetos MT5 Bridge: command received action=%s symbol=%s timeframe=%s from=%s to=%s request=%s",
                  fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
   ExecuteCommand(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
}

void ExecuteCommand(string action, string provider_symbol, string interval,
                    string from_text, string to_text, string request_id)
{
   if(action == "TIME")
   {
      datetime terminal_time = TimeTradeServer();
      if(terminal_time <= 0) terminal_time = TimeCurrent();
      ReportTerminalTime(request_id, terminal_time);
      return;
   }

   string symbol = ResolveSymbol(provider_symbol);
   ENUM_TIMEFRAMES timeframe = ParseTimeframe(interval);
   datetime from_time = ParseUtc(from_text);
   datetime to_time = ParseUtc(to_text);
   if(symbol == "" || timeframe == PERIOD_CURRENT || from_time <= 0 || to_time < from_time || !SymbolSelect(symbol, true))
   {
      ReportResult(provider_symbol, interval, request_id, false, 0, 0, 0, 4301);
      return;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   if(InpLogCommands)
      PrintFormat("Axetos MT5 Bridge: CopyRates started symbol=%s timeframe=%s request=%s", symbol, interval, request_id);
   ResetLastError();
   int copied = CopyRates(symbol, timeframe, from_time, to_time, rates);
   int error_code = GetLastError();
   if(InpLogCommands)
      PrintFormat("Axetos MT5 Bridge: CopyRates returned bars=%d error=%d request=%s", copied, error_code, request_id);
   ResetLastError();

   if(action == "DISCOVER" || action == "AVAILABILITY")
   {
      ReportAvailability(request_id, interval, rates, copied);
      return;
   }

   if(action != "FETCH" && action != "BACKFILL")
   {
      ReportResult(provider_symbol, interval, request_id, false, 0, 0, 0, 4000);
      return;
   }

   if(copied <= 0)
   {
      bool completed = (error_code == 0);
      ReportResult(provider_symbol, interval, request_id, completed, 0, 0, 0, error_code);
      return;
   }

   int stored = 0;
   int skipped = 0;
   bool uploaded = UploadCandles(symbol, interval, request_id, rates, copied, stored, skipped);
   ReportResult(provider_symbol, interval, request_id, uploaded, copied, stored, skipped,
                uploaded ? 0 : (error_code == 0 ? 5203 : error_code));
}

bool UploadCandles(string symbol, string interval, string request_id,
                   MqlRates &rates[], int copied, int &stored_out, int &skipped_out)
{
   stored_out = 0;
   skipped_out = 0;
   int chunk_size = MathMax(10, MathMin(500, InpUploadChunkSize));
   int chunk_count = (copied + chunk_size - 1) / chunk_size;
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   for(int chunk_index = 0; chunk_index < chunk_count; chunk_index++)
   {
      if(g_last_heartbeat == 0 || TimeCurrent() - g_last_heartbeat >= InpHeartbeatSeconds)
         SendHeartbeat();

      int first = chunk_index * chunk_size;
      int last = MathMin(copied, first + chunk_size);
      string items = "";
      for(int i = first; i < last; i++)
      {
         if(items != "") items += ",";
         items += StringFormat(
            "{\"timeUtc\":\"%s\",\"open\":%s,\"high\":%s,\"low\":%s,\"close\":%s,\"tickVolume\":%I64d}",
            IsoUtc(rates[i].time),
            DoubleToString(rates[i].open, digits), DoubleToString(rates[i].high, digits),
            DoubleToString(rates[i].low, digits), DoubleToString(rates[i].close, digits), rates[i].tick_volume);
      }

      string payload = StringFormat(
         "{\"providerKey\":\"%s\",\"terminalInstanceId\":\"%s\",\"providerSymbol\":\"%s\",\"canonicalInstrument\":\"%s\",\"interval\":\"%s\",\"requestId\":\"%s\",\"chunkIndex\":%d,\"chunkCount\":%d,\"candles\":[%s]}",
         JsonEscape(InpProviderKey), JsonEscape(g_terminal_id), JsonEscape(symbol), JsonEscape(symbol),
         JsonEscape(interval), JsonEscape(request_id), chunk_index + 1, chunk_count, items);
      if(InpLogCommands && (chunk_index == 0 || chunk_index == chunk_count - 1))
         PrintFormat("Axetos MT5 Bridge: upload chunk %d/%d bars=%d request=%s",
                     chunk_index + 1, chunk_count, last - first, request_id);
      string response = "";
      if(!HttpPost("/api/market-data/ingest/mt5/candles", payload, InpUploadTimeoutMs, response))
         return false;

      int stored = JsonInt(response, "stored", -1);
      int skipped = JsonInt(response, "skipped", -1);
      if(stored < 0 || skipped < 0 || stored + skipped != last - first)
         return false;
      stored_out += stored;
      skipped_out += skipped;
   }
   return true;
}

void ReportTerminalTime(string request_id, datetime terminal_time)
{
   string path = "/api/market-data/mt5/terminal-time?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id +
                 "&requestId=" + request_id +
                 "&terminalTime=" + IsoUtc(terminal_time);
   string response = "";
   bool acknowledged = HttpPost(path, "{}", InpControlTimeoutMs, response);
   if(InpLogCommands)
      PrintFormat("Axetos MT5 Bridge: terminal time %s time=%s request=%s",
                  acknowledged ? "acknowledged" : "not acknowledged", IsoUtc(terminal_time), request_id);
}

void ReportAvailability(string request_id, string interval, MqlRates &rates[], int copied)
{
   string path = "/api/market-data/mt5/history-availability?providerKey=" + InpProviderKey +
                 "&requestId=" + request_id + "&candleCount=" + IntegerToString(MathMax(0, copied));
   if(copied > 0)
   {
      path += "&earliestUtc=" + IsoUtc(rates[0].time);
      path += "&latestUtc=" + IsoUtc(rates[copied - 1].time);
   }
   string response = "";
   HttpPost(path, "{}", InpControlTimeoutMs, response);
}

void ReportResult(string provider_symbol, string interval, string request_id, bool completed,
                  int received, int stored, int skipped, int error_code)
{
   string path = "/api/market-data/mt5/repair-result?providerKey=" + InpProviderKey +
                 "&terminalInstanceId=" + g_terminal_id +
                 "&providerSymbol=" + provider_symbol + "&interval=" + interval +
                 "&completed=" + (completed ? "true" : "false") +
                 "&barsReceived=" + IntegerToString(received) +
                 "&barsInserted=" + IntegerToString(stored) +
                 "&unavailable=" + ((!completed && error_code == 4401) ? "true" : "false") +
                 "&errorCode=" + IntegerToString(error_code) + "&requestId=" + request_id;
   string response = "";
   bool acknowledged = HttpPost(path, "{}", InpControlTimeoutMs, response);
   if(InpLogCommands)
      PrintFormat("Axetos MT5 Bridge: result %s received=%d stored=%d skipped=%d request=%s",
                  acknowledged ? "acknowledged" : "not acknowledged", received, stored, skipped, request_id);
}

string ResolveSymbol(string configured)
{
   if(configured == "") return "";
   if(SymbolInfoInteger(configured, SYMBOL_EXIST)) return configured;
   string wanted = NormalizeSymbol(configured);
   int total = SymbolsTotal(false);
   for(int i = 0; i < total; i++)
   {
      string candidate = SymbolName(i, false);
      if(NormalizeSymbol(candidate) == wanted) return candidate;
   }
   return "";
}

string NormalizeSymbol(string value)
{
   string result = "";
   for(int i = 0; i < StringLen(value); i++)
   {
      ushort c = StringGetCharacter(value, i);
      if((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))
         result += StringSubstr(value, i, 1);
      else
         break;
   }
   StringToUpper(result);
   return result;
}

ENUM_TIMEFRAMES ParseTimeframe(string interval)
{
   if(interval == "1m") return PERIOD_M1;
   if(interval == "1h") return PERIOD_H1;
   if(interval == "1d") return PERIOD_D1;
   return PERIOD_CURRENT;
}

datetime ParseUtc(string value)
{
   string normalized = value;
   StringReplace(normalized, "T", " ");
   int z = StringFind(normalized, "Z");
   if(z > 0) normalized = StringSubstr(normalized, 0, z);
   int plus = StringFind(normalized, "+");
   if(plus > 0) normalized = StringSubstr(normalized, 0, plus);
   StringReplace(normalized, "-", ".");

   // MQL datetime values are absolute Unix timestamps. CopyRates accepts the
   // same absolute timestamp even though MT5 renders chart labels using the
   // broker's display clock. Never add the broker offset here: doing so shifts
   // the requested market window. The server command and database remain UTC.
   return StringToTime(normalized);
}

string BuildTerminalId()
{
   string value = StringFormat("%s-%s-%I64d", AccountInfoString(ACCOUNT_COMPANY),
                               AccountInfoString(ACCOUNT_SERVER), AccountInfoInteger(ACCOUNT_LOGIN));
   StringReplace(value, " ", "-");
   StringReplace(value, "/", "-");
   StringReplace(value, "\\", "-");
   StringReplace(value, ":", "-");
   return value;
}

string IsoUtc(datetime value)
{
   MqlDateTime p;
   TimeToStruct(value, p);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ", p.year, p.mon, p.day, p.hour, p.min, p.sec);
}

bool HttpGet(string path, int timeout_ms, string &response)
{
   string url = InpServerUrl + path;
   string headers = "Accept: text/plain\r\n";
   if(InpBridgeToken != "") headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";
   char data[];
   char result[];
   string result_headers;
   ArrayResize(data, 0);
   ArrayResize(result, 0);
   ResetLastError();
   int status = InpBridgeToken == ""
      ? WebRequest("GET", url, NULL, NULL, timeout_ms, data, 0, result, result_headers)
      : WebRequest("GET", url, headers, timeout_ms, data, result, result_headers);
   int error_code = GetLastError();
   response = ArraySize(result) > 0 ? CharArrayToString(result, 0, ArraySize(result), CP_UTF8) : "";
   if(status >= 200 && status < 300) return true;
   PrintFormat("Axetos MT5 Bridge: GET failed path=%s HTTP=%d error=%d", path, status, error_code);
   return false;
}

bool HttpPost(string path, string payload, int timeout_ms, string &response)
{
   string url = InpServerUrl + path;
   string headers = "Content-Type: application/json\r\nAccept: text/plain, application/json\r\n";
   if(InpBridgeToken != "") headers += "Authorization: Bearer " + InpBridgeToken + "\r\n";
   char data[];
   char result[];
   string result_headers;
   int length = StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   if(length > 0) ArrayResize(data, length - 1);
   ArrayResize(result, 0);
   ResetLastError();
   int status = WebRequest("POST", url, headers, timeout_ms, data, result, result_headers);
   int error_code = GetLastError();
   response = ArraySize(result) > 0 ? CharArrayToString(result, 0, ArraySize(result), CP_UTF8) : "";
   if(status >= 200 && status < 300) return true;
   PrintFormat("Axetos MT5 Bridge: POST failed path=%s HTTP=%d error=%d response=%s", path, status, error_code, response);
   return false;
}

int JsonInt(string json, string field, int fallback)
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
      ushort c = StringGetCharacter(json, end);
      if(c < 48 || c > 57) break;
      end++;
   }
   return end > pos ? (int)StringToInteger(StringSubstr(json, pos, end - pos)) : fallback;
}

string JsonEscape(string value)
{
   StringReplace(value, "\\", "\\\\");
   StringReplace(value, "\"", "\\\"");
   StringReplace(value, "\r", "\\r");
   StringReplace(value, "\n", "\\n");
   return value;
}
