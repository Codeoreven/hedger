# ================= IMPORTS =================
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import datetime
import time
import threading
import csv
import json
import os
import sys
from urllib.parse import parse_qs, unquote, urlparse
from dotenv import load_dotenv

# ================= CONFIG =================
load_dotenv()

client_id = os.getenv("CLIENT_ID")
secret_key = os.getenv("SECRET_KEY")
redirect_uri = "https://trade.fyers.in/api-login/redirect-uri/index.html"
FYERS_STATE = os.getenv("FYERS_STATE", "sample_state")
FYERS_NONCE = os.getenv("FYERS_NONCE", "sample_nonce")

ENTRY_PRICE = 250
STOPLOSS = 225
TARGET = 300
QTY = 50

TOKEN_FILE = "tokens.json"
TRADES_FILE = "trades.csv"

trades = []
ltp_data = {}

# ================= AUTH =================
def authenticate():
    if not client_id or not secret_key:
        raise RuntimeError(
            "Missing FYERS credentials. Ensure environment variables CLIENT_ID and SECRET_KEY are set "
            "(and that your server process actually loads them)."
        )

    def _write_tokens(payload: dict) -> None:
        with open(TOKEN_FILE, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

    def _extract_access_token(payload: dict) -> str:
        token = payload.get("access_token")
        if token:
            return token
        err_status = payload.get("s")
        err_code = payload.get("code")
        err_message = payload.get("message")
        raise RuntimeError(
            "Token generation did not return access_token. "
            f"status={err_status!r}, code={err_code!r}, message={err_message!r}. "
            f"Response keys={sorted(list(payload.keys()))}. "
            f"Full response saved to {TOKEN_FILE!r}."
        )

    def _normalize_auth_code(raw_input: str) -> str:
        value = (raw_input or "").strip()
        if not value:
            return value

        # If user pastes full redirect URL, extract auth_code query param.
        if "://" in value:
            parsed = urlparse(value)
            query = parse_qs(parsed.query)
            code_from_query = query.get("auth_code", [""])[0].strip()
            if code_from_query:
                return unquote(code_from_query)

        # Handle "auth_code=xxxx" pastes directly.
        if "auth_code=" in value:
            query = parse_qs(value)
            code_from_query = query.get("auth_code", [""])[0].strip()
            if code_from_query:
                return unquote(code_from_query)

        return unquote(value)

    def _generate_from_auth_code() -> str:
        # FYERS authcode generation URL depends on app config. redirect_uri must match exactly
        # what is configured in the FYERS app dashboard.
        session_kwargs = dict(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        # Some FYERS examples include state/nonce; pass them if supported by installed SDK.
        # (If unsupported, SessionModel will raise TypeError and we retry without them.)
        try:
            session = fyersModel.SessionModel(
                **session_kwargs,
                state=FYERS_STATE,
                nonce=FYERS_NONCE,
            )
        except TypeError:
            session = fyersModel.SessionModel(**session_kwargs)

        auth_url = session.generate_authcode()
        print("Open this URL:\n", auth_url)

        # On servers (systemd/cron/docker), stdin is often non-interactive.
        if not sys.stdin.isatty():
            raise RuntimeError(
                "Cannot prompt for auth code (non-interactive). "
                "Set a valid refresh_token in tokens.json or run once interactively to generate it."
            )

        auth_code_input = input("Enter auth code (or full redirect URL): ").strip()
        auth_code = _normalize_auth_code(auth_code_input)
        if not auth_code:
            raise RuntimeError("Empty auth code received. Please paste a valid auth code/redirect URL.")
        session.set_token(auth_code)
        response = session.generate_token() or {}
        _write_tokens(response)
        if not response.get("access_token"):
            print("FYERS token error:", response.get("code"), response.get("message"))
        return _extract_access_token(response)

    def _refresh_from_refresh_token(refresh_token: str) -> str:
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="refresh_token",
        )
        session.set_token(refresh_token)
        response = session.generate_token() or {}
        _write_tokens(response)
        if not response.get("access_token"):
            print("FYERS refresh error:", response.get("code"), response.get("message"))
        return _extract_access_token(response)

    # Try using saved token
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)

        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")

        if access_token:
            fyers_test = fyersModel.FyersModel(
                client_id=client_id,
                is_async=False,
                token=access_token,
                log_path="",
            )

            profile = fyers_test.get_profile()

            # ✅ check if token works
            if profile.get("s") == "ok":
                print("Using valid saved token")
                return access_token
            else:
                print("Saved token invalid/expired.")

        if refresh_token:
            print("Refreshing access token using refresh_token...")
            try:
                return _refresh_from_refresh_token(refresh_token)
            except Exception as e:
                print("Refresh failed:", str(e))

    # Generate new token
    print("Generating new token using auth code flow...")
    return _generate_from_auth_code()

# ================= TIME =================
def wait_for_market_open():
    while True:
        if datetime.datetime.now().time() >= datetime.time(9, 16):
            print("Market open - starting strategy")
            break
        time.sleep(5)

def is_market_close():
    return datetime.datetime.now().time() >= datetime.time(15, 0)

# ================= CSV LOGGER =================
def log_trade(trade):
    file_exists = os.path.isfile(TRADES_FILE)

    with open(TRADES_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "symbol","buy_price","qty","stoploss","target",
            "entry_time","exit_time","exit_price","pnl"
        ])

        if not file_exists:
            writer.writeheader()

        writer.writerow(trade)

# ================= TRADE ENGINE =================
def create_trade(symbol):
    trade = {
        "symbol": symbol,
        "buy_price": None,
        "qty": QTY,
        "stoploss": STOPLOSS,
        "target": TARGET,
        "entry_time": None,
        "exit_time": None,
        "exit_price": None,
        "pnl": 0,
        "status": "PENDING"
    }
    trades.append(trade)
    print(f"Waiting BUY {symbol} @ {ENTRY_PRICE}")

def open_trade(trade, price):
    trade["buy_price"] = price
    trade["entry_time"] = datetime.datetime.now()
    trade["status"] = "OPEN"
    print(f"BUY {trade['symbol']} @ {price}")

def close_trade(trade, price):
    trade["exit_price"] = price
    trade["exit_time"] = datetime.datetime.now()
    trade["status"] = "CLOSED"
    trade["pnl"] = (price - trade["buy_price"]) * trade["qty"]

    log_trade(trade)
    print(f"CLOSE {trade['symbol']} @ {price} | PnL: {trade['pnl']}")

# ================= WEBSOCKET =================
def on_message(message):
    if 'symbol' not in message:
        return

    symbol = message['symbol']
    ltp = message['ltp']
    ltp_data[symbol] = ltp

    for trade in trades:
        if trade["symbol"] != symbol:
            continue

        # ENTRY
        if trade["status"] == "PENDING":
            if ltp >= ENTRY_PRICE:
                open_trade(trade, ltp)

        # EXIT
        elif trade["status"] == "OPEN":
            if ltp <= STOPLOSS:
                close_trade(trade, ltp)
            elif ltp >= TARGET:
                close_trade(trade, ltp)

def start_websocket(symbols, access_token):

    def on_open():
        print("WebSocket Connected")
        fyers_socket.subscribe(symbols=symbols, data_type="symbolData")

    global fyers_socket
    fyers_socket = data_ws.FyersDataSocket(
        access_token=access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=on_open,
        on_message=on_message,
        on_error=lambda e: print("WS Error:", e),
        on_close=lambda e: print("WS Closed")
    )

    fyers_socket.connect()

# ================= MARKET CLOSE =================
def monitor_market_close():
    while True:
        if is_market_close():
            print("Force closing all trades at 15:00")

            for trade in trades:
                if trade["status"] == "OPEN":
                    ltp = ltp_data.get(trade["symbol"], trade["buy_price"])
                    close_trade(trade, ltp)

            break

        time.sleep(5)

# ================= OPTION CHAIN =================
def get_filtered_symbols(fyers):
    data = {
        "symbol": "NSE:NIFTY50-INDEX",
        "strikecount": 7,
        "timestamp": ""
    }

    option_chain_data = fyers.optionchain(data=data)

    print("Option Chain Data:", option_chain_data)

    symbols = []
    for opt in option_chain_data['data']['optionsChain']: 
        if opt.get('option_type') and 225 <= opt.get('ltp', 0) <= 250: 
            symbols.append({ 
                'symbol': opt['symbol'], 
                'strike': opt['strike_price'], 
                'type': opt['option_type'], 
                'ltp': opt['ltp'] 
            })

    return symbols

# ================= MAIN =================
def main():

    access_token = authenticate()

    fyers = fyersModel.FyersModel(
        client_id=client_id,
        is_async=False,
        token=access_token,
        log_path=""
    )

    profile = fyers.get_profile()
    print("Welcome:", profile['data']['name'])

    wait_for_market_open()

    symbols = get_filtered_symbols(fyers)
    print("Selected Symbols:", symbols)

    for sym in symbols:
        create_trade(sym)

    # Start threads
    threading.Thread(target=start_websocket, args=(symbols, access_token)).start()
    threading.Thread(target=monitor_market_close).start()

# ================= RUN =================
if __name__ == "__main__":
    main()