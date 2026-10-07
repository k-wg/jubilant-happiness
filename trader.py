#!/usr/bin/env python3
# Binance Convert trading bot driven by snippets.csv.
#
# Flow:
#   1. Boot: define token_pair (e.g. ZECUSDT), main_token (ZEC),
#      stablecoin_pair (USDT). Query wallet balances.
#   2. If we hold main_token  -> SELL mode.
#      Watch "levels" column for 50, 75 or 93.
#      On hit: convert main_token -> USDT.
#   3. If we do NOT hold main_token (only USDT or other tokens) -> BUY mode.
#      Watch "levels" column for 9.
#      On hit: if USDT balance > 0.2 -> convert USDT -> main_token, enter SELL mode.
#      If USDT <= 0.2 -> arm a 5-minute COOLDOWN (non-blocking), ignore any 9
#      during the cooldown, and require a FRESH 9 after the cooldown expires.
#
# Every wallet query is appended to wallet_history.csv.
# Current holdings are always shown on the terminal with USDT valuation.
#
# On any critical error (buy/sell conversion, wallet query, etc.):
#   * email kwgatheru@gmail.com
#   * telegram the same message
#
# Visuals: rich for panels / rules / colors / spinners;
#          tabulate for the data tables (rounded_outline).

import argparse
import csv
import hashlib
import hmac
import json
import os
import signal
import smtplib
import ssl
import sys
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("Missing dependency: requests")
    print("Install with:  pip install requests rich tabulate")
    sys.exit(1)

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn,
)
from rich.theme import Theme
from rich.align import Align

from tabulate import tabulate


# ==================== CONFIGURATION ====================
CONFIG = {
    # ----- Trading pair -----
    "token_pair":        "ZECUSDT",   # e.g. ZECUSDT
    "main_token":        "ZEC",       # base asset
    "stablecoin_pair":   "USDT",      # quote asset

    # ----- Inputs -----
    "snippets_csv":       "snippets.csv",
    "wallet_history_csv": "wallet_history.csv",
    "state_json":         "convert_bot_state.json",

    # ----- Binance API -----
    "binance_api_key":     os.environ.get("BINANCE_API_KEY", ""),
    "binance_api_secret":  os.environ.get("BINANCE_API_SECRET", ""),
    "binance_base":        "https://api.binance.com",
    "binance_base":        "https://api.binance.com",

    # Binance endpoints
    "convert_quote_path":  "/sapi/v1/convert/getQuote",
    "convert_accept_path": "/sapi/v1/convert/acceptQuote",
    "account_path":        "/api/v3/account",
    "ticker_price_path":   "/api/v3/ticker/price",

    # ----- Trading behaviour -----
    "buy_level":          9,                 # buy when this appears
    "sell_levels":        [50, 75, 93],      # sell when any of these appear
    "min_usdt_to_buy":    0.2,               # min USDT to attempt a buy
    "buy_cooldown_seconds": 300,             # 5 min cooldown after low-USDT
    "poll_seconds":       1.0,               # how often to poll snippets.csv
    "heartbeat_seconds":  300.0,             # heartbeat interval

    # ----- Convert parameters -----
    "convert_slippage":    "0.005",          # 0.5% slippage
    "convert_quote_valid_seconds": 30,       # validTime

    # ----- Notifications -----
    "notify_email":       "",
    "smtp_host":          os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port":          int(os.environ.get("SMTP_PORT", "587")),
    "smtp_user":          os.environ.get("SMTP_USER", ""),
    "smtp_pass":          os.environ.get("SMTP_PASS", ""),

    "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":   os.environ.get("TELEGRAM_CHAT_ID", ""),

    # ----- Display -----
    "display_tz_offset_hours": 3,
}

DISPLAY_TZ = timezone(timedelta(hours=CONFIG["display_tz_offset_hours"]))

WALLET_HISTORY_COLUMNS = [
    "ts_utc",
    "ts_eat",
    "main_token",
    "main_balance",
    "main_usdt_value",
    "usdt_balance",
    "other_assets_json",
    "note",
]

keep_running = True


# ==================== RICH CONSOLE ====================
THEME = Theme({
    "tag.boot":      "bold cyan",
    "tag.watch":     "bold blue",
    "tag.buy":       "bold green",
    "tag.sell":      "bold red",
    "tag.hb":        "bold bright_black",
    "tag.ok":        "bold green",
    "tag.warn":      "bold yellow",
    "tag.err":       "bold red",
    "tag.signal":    "bold red on grey11",
    "tag.notify":    "bold magenta",
    "close.up":      "bold green",
    "close.down":    "bold red",
    "close.flat":    "white",
    "time":          "cyan",
})

console = Console(theme=THEME, highlight=False)

PHASE_TAG = {
    "boot":      "[tag.boot]\\[boot][/tag.boot]",
    "watch":     "[tag.watch]\\[watch][/tag.watch]",
    "buy":       "[tag.buy]\\[buy][/tag.buy]",
    "sell":      "[tag.sell]\\[sell][/tag.sell]",
    "hb":        "[tag.hb]\\[heartbeat][/tag.hb]",
    "ok":        "[tag.ok]\\[ok][/tag.ok]",
    "warn":      "[tag.warn]\\[warn][/tag.warn]",
    "err":       "[tag.err]\\[err][/tag.err]",
    "signal":    "[tag.signal]\\[signal][/tag.signal]",
    "notify":    "[tag.notify]\\[notify][/tag.notify]",
}


def log(tag: str, msg: str, *args):
    prefix = PHASE_TAG.get(tag, f"[{tag}]")
    text = msg.format(*args) if args else msg
    console.print(f"{prefix} {text}")


def signal_handler(signum, frame):
    global keep_running
    console.print()
    log("signal", "shutting down…")
    keep_running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ==================== TIME HELPERS ====================
def now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_iso(ms, tz=timezone.utc) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=tz) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


def ms_to_iso_eat(ms) -> str:
    return ms_to_iso(ms, DISPLAY_TZ)


def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def iso_now_eat() -> str:
    return datetime.now(DISPLAY_TZ).isoformat(timespec="seconds")


# ==================== NOTIFICATIONS ====================
def notify(subject: str, body: str) -> None:
    """
    Send a notification via email AND telegram. Never raises; logs
    failures.
    """
    # ---- Email ----
    try:
        if CONFIG["smtp_user"] and CONFIG["smtp_pass"]:
            msg = EmailMessage()
            msg["Subject"] = f"[convert-bot] {subject}"
            msg["From"] = CONFIG["smtp_user"]
            msg["To"] = CONFIG["notify_email"]
            msg.set_content(
                f"{body}\n\n"
                f"---\n"
                f"token_pair     = {CONFIG['token_pair']}\n"
                f"main_token     = {CONFIG['main_token']}\n"
                f"stablecoin     = {CONFIG['stablecoin_pair']}\n"
                f"time (utc)     = {iso_now_utc()}\n"
                f"time (eat)     = {iso_now_eat()}\n"
            )
            ctx = ssl.create_default_context()
            with smtplib.SMTP(CONFIG["smtp_host"], CONFIG["smtp_port"],
                              timeout=20) as s:
                s.starttls(context=ctx)
                s.login(CONFIG["smtp_user"], CONFIG["smtp_pass"])
                s.send_message(msg)
            log("notify", f"email sent → {CONFIG['notify_email']}")
        else:
            log("warn", "email skipped (SMTP_USER / SMTP_PASS not set)")
    except Exception as e:
        log("err", f"email send failed: {e}")

    # ---- Telegram ----
    try:
        if CONFIG["telegram_bot_token"] and CONFIG["telegram_chat_id"]:
            url = (f"https://api.telegram.org/bot"
                   f"{CONFIG['telegram_bot_token']}/sendMessage")
            payload = {
                "chat_id": CONFIG["telegram_chat_id"],
                "text": (f"[convert-bot] {subject}\n\n{body}\n\n"
                         f"pair={CONFIG['token_pair']} "
                         f"main={CONFIG['main_token']} "
                         f"quote={CONFIG['stablecoin_pair']}\n"
                         f"utc={iso_now_utc()}"),
                "disable_web_page_preview": True,
            }
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200 and r.json().get("ok"):
                log("notify", "telegram sent")
            else:
                log("warn", f"telegram non-OK: {r.status_code} {r.text[:200]}")
        else:
            log("warn", "telegram skipped (TELEGRAM_BOT_TOKEN / CHAT_ID not set)")
    except Exception as e:
        log("err", f"telegram send failed: {e}")


# ==================== BINANCE REST (signed) ====================
class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, base: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base = base.rstrip("/")

    def _sign(self, params: Dict) -> str:
        query = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret.encode(), query.encode(),
                       hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    def _headers(self) -> Dict:
        return {"X-MBX-APIKEY": self.api_key}

    def signed_get(self, path: str, params: Optional[Dict] = None,
                   timeout: int = 20) -> Dict:
        params = dict(params or {})
        params["timestamp"] = now_ms()
        params["recvWindow"] = 60000
        url = f"{self.base}{path}?{self._sign(params)}"
        r = requests.get(url, headers=self._headers(), timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(
                f"Binance GET {path} {r.status_code}: {r.text[:400]}")
        return r.json()

    def signed_post(self, path: str, params: Optional[Dict] = None,
                    timeout: int = 20) -> Dict:
        params = dict(params or {})
        params["timestamp"] = now_ms()
        params["recvWindow"] = 60000
        url = f"{self.base}{path}?{self._sign(params)}"
        r = requests.post(url, headers=self._headers(), timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(
                f"Binance POST {path} {r.status_code}: {r.text[:400]}")
        return r.json()

    def public_get(self, path: str, params: Optional[Dict] = None,
                   timeout: int = 20) -> Dict:
        url = f"{self.base}{path}"
        r = requests.get(url, params=params or {}, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(
                f"Binance GET {path} {r.status_code}: {r.text[:400]}")
        return r.json()

    # ---- account ----
    def account(self) -> Dict:
        return self.signed_get(CONFIG["account_path"],
                               {"omitZeroBalances": "true"})

    def price(self, symbol: str) -> float:
        data = self.public_get(CONFIG["ticker_price_path"], {"symbol": symbol})
        return float(data["price"])

    # ---- convert ----
    def convert_quote(self, from_asset: str, to_asset: str, amount: float):
        params = {
            "fromAsset": from_asset.upper(),
            "toAsset":   to_asset.upper(),
            "fromAmount": f"{amount:.8f}".rstrip("0").rstrip("."),
            "validTime": str(CONFIG["convert_quote_valid_seconds"]),
            "slippage":  CONFIG["convert_slippage"],
        }
        return self.signed_post(CONFIG["convert_quote_path"], params)

    def convert_accept(self, quote_id: str):
        return self.signed_post(CONFIG["convert_accept_path"],
                                {"quoteId": quote_id})


# ==================== WALLET ====================
def get_balances(client: BinanceClient) -> Dict[str, float]:
    """
    Returns {asset: free_balance}. Raises on failure.
    """
    acct = client.account()
    out: Dict[str, float] = {}
    for b in acct.get("balances", []):
        try:
            free = float(b.get("free", 0.0))
        except (TypeError, ValueError):
            free = 0.0
        if free > 0:
            out[b["asset"].upper()] = free
    return out


def ensure_wallet_header(path: Path):
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(WALLET_HISTORY_COLUMNS)


def append_wallet_history(path: Path, main_token: str, main_balance: float,
                          main_usdt_value: Optional[float],
                          usdt_balance: float,
                          other_assets: Dict[str, float],
                          note: str = ""):
    ensure_wallet_header(path)
    row = [
        iso_now_utc(),
        iso_now_eat(),
        main_token,
        f"{main_balance:.8f}",
        "" if main_usdt_value is None else f"{main_usdt_value:.8f}",
        f"{usdt_balance:.8f}",
        json.dumps(other_assets, ensure_ascii=False, sort_keys=True),
        note,
    ]
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(row)


# ==================== SNIPPETS CSV READER ====================
def read_snippets_tail(path: Path, n: int = 5) -> List[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    dq = deque(maxlen=n)
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            dq.append(row)
    return list(dq)


def parse_levels(levels_field: str) -> List[int]:
    if not levels_field:
        return []
    out = []
    for tok in str(levels_field).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            continue
    return out


# ==================== STATE ====================
def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, data: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    os.replace(tmp, path)


# ==================== DISPLAY ====================
def render_wallet_panel(main_token: str, main_balance: float,
                        main_price: Optional[float],
                        usdt_balance: float,
                        other_assets: Dict[str, float],
                        mode: str,
                        cooldown_remaining: int = 0) -> Panel:
    main_usdt = None
    if main_balance > 0 and main_price is not None:
        main_usdt = main_balance * main_price

    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="right", style="bold bright_black")
    grid.add_column(style="white")

    mode_txt = f"[bold yellow]{mode}[/bold yellow]"
    if cooldown_remaining > 0:
        mode_txt += f"  [bright_black](cooldown {cooldown_remaining}s)[/bright_black]"
    grid.add_row("mode", mode_txt)

    if main_usdt is not None:
        grid.add_row(
            main_token,
            f"[bold cyan]{main_balance:.8f}[/bold cyan]  "
            f"[green]≈ {main_usdt:,.4f} {CONFIG['stablecoin_pair']}[/green]  "
            f"[bright_black](@ {main_price:,.4f})[/bright_black]",
        )
    else:
        grid.add_row(
            main_token,
            f"[bold cyan]{main_balance:.8f}[/bold cyan]  "
            f"[bright_black](no price yet)[/bright_black]",
        )

    grid.add_row(CONFIG["stablecoin_pair"],
                 f"[bold green]{usdt_balance:,.8f}[/bold green]")

    if other_assets:
        pretty = ", ".join(
            f"{a}={v:.6f}" for a, v in sorted(other_assets.items())
        )
        grid.add_row("other", f"[bright_black]{pretty}[/bright_black]")

    return Panel(
        Align.left(grid),
        title=f"[bold white on blue] WALLET — {CONFIG['token_pair']} [/bold white on blue]",
        border_style="bright_blue",
        padding=(1, 2),
    )


def render_snippet_tail(rows: List[dict]) -> str:
    if not rows:
        return "(no data)"
    table_rows = []
    for r in rows:
        table_rows.append([
            r.get("open_time_utc", ""),
            r.get("close", ""),
            r.get("rsi", ""),
            r.get("rsi_ma50", ""),
            r.get("plotted", ""),
            r.get("levels", ""),
        ])
    return tabulate(
        table_rows,
        headers=["time (utc)", "close", "rsi", "rsi_ma50", "plotted", "levels"],
        tablefmt="rounded_outline",
        colalign=("left", "right", "right", "right", "left", "left"),
        disable_numparse=True,
    )


def print_snippet_panel(rows: List[dict], title: str):
    console.print(Panel(
        render_snippet_tail(rows),
        title=f"[bold white]{title}[/bold white]",
        border_style="bright_magenta",
        padding=(0, 1),
    ))


# ==================== BANNER ====================
def print_banner():
    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="right", style="bold bright_black")
    grid.add_column(style="white")

    grid.add_row("token_pair",  f"[yellow]{CONFIG['token_pair']}[/yellow]")
    grid.add_row("main_token",  f"[yellow]{CONFIG['main_token']}[/yellow]")
    grid.add_row("stablecoin",  f"[yellow]{CONFIG['stablecoin_pair']}[/yellow]")
    grid.add_row("snippets",    f"[cyan]{CONFIG['snippets_csv']}[/cyan]")
    grid.add_row("wallet csv",  f"[cyan]{CONFIG['wallet_history_csv']}[/cyan]")
    grid.add_row("buy level",   f"[green]{CONFIG['buy_level']}[/green]")
    grid.add_row("sell levels", f"[red]{CONFIG['sell_levels']}[/red]")
    grid.add_row("min USDT",    f"[green]{CONFIG['min_usdt_to_buy']}[/green]")
    grid.add_row("cooldown",    f"[green]{CONFIG['buy_cooldown_seconds']}s[/green]")
    grid.add_row("poll",        f"[green]{CONFIG['poll_seconds']}s[/green]")
    grid.add_row("heartbeat",
                 f"[bright_black]{CONFIG['heartbeat_seconds']:.0f}s[/bright_black]")
    grid.add_row("api key",
                 f"[bright_black]{'set' if CONFIG['binance_api_key'] else 'MISSING'}[/bright_black]")
    grid.add_row("smtp",
                 f"[bright_black]{'set' if CONFIG['smtp_user'] else 'not set'}[/bright_black]")
    grid.add_row("telegram",
                 f"[bright_black]{'set' if CONFIG['telegram_bot_token'] else 'not set'}[/bright_black]")

    console.print(Panel(
        Align.left(grid),
        title="[bold white on blue] BINANCE CONVERT TRADING BOT [/bold white on blue]",
        subtitle="[italic bright_black]driven by snippets.csv · rich + tabulate[/italic bright_black]",
        border_style="bright_blue",
        padding=(1, 2),
    ))


# ==================== TRADING CORE ====================
class TradingBot:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.mode: str = "INIT"              # INIT / BUY / SELL
        self.last_seen_row_ms: int = 0
        self.last_wallet_query_ts: float = 0.0
        self.last_heartbeat_ts: float = 0.0
        self.last_wallet_snapshot: Dict = {}
        self.last_error_ts: float = 0.0
        self.cooldown_until_ts: float = 0.0  # non-blocking cooldown gate

    # ---------- wallet helpers ----------
    def refresh_wallet(self, note: str = ""
                       ) -> Tuple[float, float, Dict[str, float], Optional[float]]:
        """
        Query balances, fetch price, append to wallet_history.csv,
        cache snapshot. Returns (main_balance, usdt_balance, other, price).
        Raises on Binance failure (caller decides what to do).
        """
        balances = get_balances(self.client)

        main_bal = balances.get(CONFIG["main_token"], 0.0)
        usdt_bal = balances.get(CONFIG["stablecoin_pair"], 0.0)
        others = {a: v for a, v in balances.items()
                  if a not in (CONFIG["main_token"], CONFIG["stablecoin_pair"])}

        price = None
        try:
            price = self.client.price(CONFIG["token_pair"])
        except Exception as e:
            log("warn", f"price fetch failed: {e}")

        main_usdt = None
        if price is not None and main_bal > 0:
            main_usdt = main_bal * price

        append_wallet_history(
            Path(CONFIG["wallet_history_csv"]),
            CONFIG["main_token"], main_bal, main_usdt, usdt_bal, others, note,
        )

        self.last_wallet_query_ts = time.time()
        self.last_wallet_snapshot = {
            "main_balance": main_bal,
            "main_usdt_value": main_usdt,
            "usdt_balance": usdt_bal,
            "other_assets": others,
            "price": price,
        }
        return main_bal, usdt_bal, others, price

    def print_wallet(self):
        snap = self.last_wallet_snapshot or {}
        remaining = max(0, int(self.cooldown_until_ts - time.time()))
        console.print(render_wallet_panel(
            CONFIG["main_token"],
            snap.get("main_balance", 0.0),
            snap.get("price"),
            snap.get("usdt_balance", 0.0),
            snap.get("other_assets", {}),
            self.mode,
            cooldown_remaining=remaining,
        ))

    # ---------- trading ----------
    def do_buy(self, usdt_balance: float) -> bool:
        amount = float(f"{usdt_balance:.8f}")
        log("buy", f"requesting convert quote: "
                   f"[green]{amount:.8f} {CONFIG['stablecoin_pair']}[/green] "
                   f"→ [cyan]{CONFIG['main_token']}[/cyan]")
        try:
            quote = self.client.convert_quote(
                CONFIG["stablecoin_pair"], CONFIG["main_token"], amount)
        except Exception as e:
            raise RuntimeError(f"BUY convert_quote failed: {e}") from e

        quote_id = quote.get("quoteId")
        if not quote_id:
            raise RuntimeError(f"BUY quote missing quoteId: {quote}")

        log("buy", f"quote id=[magenta]{quote_id}[/magenta] "
                   f"ratio={quote.get('ratio')} "
                   f"toAmount={quote.get('toAmount')}")

        try:
            res = self.client.convert_accept(quote_id)
        except Exception as e:
            raise RuntimeError(f"BUY convert_accept failed: {e}") from e

        status = str(res.get("orderStatus", "")).upper()
        if status not in ("SUCCESS", "PROCESS"):
            raise RuntimeError(f"BUY accept returned non-success: {res}")

        log("ok", f"BUY success: {status} id={res.get('orderId')}")
        return True

    def do_sell(self) -> bool:
        balances = get_balances(self.client)
        main_bal = balances.get(CONFIG["main_token"], 0.0)
        if main_bal <= 0:
            log("warn", f"SELL requested but {CONFIG['main_token']} balance "
                        f"is {main_bal}; nothing to do.")
            return False

        amount = float(f"{main_bal:.8f}")
        log("sell", f"requesting convert quote: "
                    f"[cyan]{amount:.8f} {CONFIG['main_token']}[/cyan] "
                    f"→ [green]{CONFIG['stablecoin_pair']}[/green]")
        try:
            quote = self.client.convert_quote(
                CONFIG["main_token"], CONFIG["stablecoin_pair"], amount)
        except Exception as e:
            raise RuntimeError(f"SELL convert_quote failed: {e}") from e

        quote_id = quote.get("quoteId")
        if not quote_id:
            raise RuntimeError(f"SELL quote missing quoteId: {quote}")

        log("sell", f"quote id=[magenta]{quote_id}[/magenta] "
                    f"ratio={quote.get('ratio')} "
                    f"toAmount={quote.get('toAmount')}")

        try:
            res = self.client.convert_accept(quote_id)
        except Exception as e:
            raise RuntimeError(f"SELL convert_accept failed: {e}") from e

        status = str(res.get("orderStatus", "")).upper()
        if status not in ("SUCCESS", "PROCESS"):
            raise RuntimeError(f"SELL accept returned non-success: {res}")

        log("ok", f"SELL success: {status} id={res.get('orderId')}")
        return True

    # ---------- mode selection ----------
    def decide_initial_mode(self):
        log("boot", "querying wallet to decide starting mode…")
        try:
            main_bal, usdt_bal, others, price = self.refresh_wallet("boot")
        except Exception as e:
            msg = f"boot wallet query failed: {e}"
            log("err", msg)
            notify("BOOT wallet query failed", msg)
            self.mode = "BUY"  # default; will retry on next signal
            return

        self.print_wallet()

        if main_bal > 0:
            self.mode = "SELL"
            log("boot", f"holding [cyan]{main_bal:.8f} "
                        f"{CONFIG['main_token']}[/cyan] → "
                        f"[red]SELL[/red] mode")
        else:
            self.mode = "BUY"
            log("boot", f"no [cyan]{CONFIG['main_token']}[/cyan] → "
                        f"[green]BUY[/green] mode")

    # ---------- per-row handling ----------
    def handle_new_row(self, row: dict):
        levels = parse_levels(row.get("levels", ""))
        plotted = row.get("plotted", "")
        t = row.get("open_time_utc", "")

        if self.mode == "BUY":
            self._handle_buy_mode(row, levels, plotted, t)
        elif self.mode == "SELL":
            self._handle_sell_mode(row, levels, plotted, t)

    def _handle_buy_mode(self, row, levels, plotted, t):
        if CONFIG["buy_level"] not in levels:
            return

        # ---- Cooldown gate ----
        # If we recently found USDT <= 0.2, we ignore any 9 that arrives
        # before the cooldown expires. This forces the bot to wait for a
        # FRESH 9 AFTER the cooldown, not reuse the signal that triggered
        # the cooldown.
        now = time.time()
        if now < self.cooldown_until_ts:
            remaining = int(self.cooldown_until_ts - now)
            log("buy", f"9 seen @ {t} but in cooldown for "
                       f"{remaining}s more; ignoring (will re-arm for a "
                       f"fresh 9).")
            return

        log("signal", f"BUY signal @ {t} plotted={plotted} levels={levels}")

        try:
            main_bal, usdt_bal, others, price = self.refresh_wallet("pre-buy")
        except Exception as e:
            msg = f"pre-buy wallet query failed: {e}"
            log("err", msg)
            notify("PRE-BUY wallet query failed", msg)
            return
        self.print_wallet()

        if main_bal > 0:
            log("warn", f"already holding {main_bal:.8f} "
                        f"{CONFIG['main_token']}; switching to SELL mode.")
            self.mode = "SELL"
            return

        if usdt_bal <= CONFIG["min_usdt_to_buy"]:
            # Arm the non-blocking cooldown. The polling loop keeps
            # running; any 9 within the window is discarded. A 9 after
            # the window is a fresh signal and will be acted on.
            self.cooldown_until_ts = now + CONFIG["buy_cooldown_seconds"]
            log("warn", f"USDT balance {usdt_bal:.8f} <= "
                        f"{CONFIG['min_usdt_to_buy']}; cooling down for "
                        f"{CONFIG['buy_cooldown_seconds']}s and will re-arm "
                        f"for a fresh {CONFIG['buy_level']}.")
            return

        # Execute buy.
        try:
            self.do_buy(usdt_bal)
        except Exception as e:
            msg = f"BUY conversion failed: {e}"
            log("err", msg)
            notify("BUY conversion FAILED", msg)
            return

        try:
            self.refresh_wallet("post-buy")
        except Exception as e:
            log("warn", f"post-buy wallet query failed: {e}")
        self.print_wallet()

        self.mode = "SELL"
        log("sell", "entered SELL mode after successful buy")

    def _handle_sell_mode(self, row, levels, plotted, t):
        hit = [lv for lv in CONFIG["sell_levels"] if lv in levels]
        if not hit:
            return
        log("signal", f"SELL signal @ {t} levels={levels} (hit {hit})")

        try:
            main_bal, usdt_bal, others, price = self.refresh_wallet("pre-sell")
        except Exception as e:
            msg = f"pre-sell wallet query failed: {e}"
            log("err", msg)
            notify("PRE-SELL wallet query failed", msg)
            return
        self.print_wallet()

        if main_bal <= 0:
            log("warn", f"no {CONFIG['main_token']} to sell; "
                        f"switching to BUY.")
            self.mode = "BUY"
            return

        try:
            self.do_sell()
        except Exception as e:
            msg = f"SELL conversion failed: {e}"
            log("err", msg)
            notify("SELL conversion FAILED", msg)
            return

        try:
            self.refresh_wallet("post-sell")
        except Exception as e:
            log("warn", f"post-sell wallet query failed: {e}")
        self.print_wallet()

        self.mode = "BUY"
        log("buy", "entered BUY mode after successful sell")

    # ---------- heartbeat ----------
    def maybe_heartbeat(self):
        now = time.time()
        if now - self.last_heartbeat_ts < CONFIG["heartbeat_seconds"]:
            return
        self.last_heartbeat_ts = now
        snap = self.last_wallet_snapshot or {}
        cd = max(0, int(self.cooldown_until_ts - now))
        console.print(Rule(style="bright_black"))
        log("hb", f"{iso_now_eat()}  mode=[yellow]{self.mode}[/yellow]  "
                  f"cooldown=[bright_black]{cd}s[/bright_black]  "
                  f"{CONFIG['main_token']}="
                  f"{snap.get('main_balance', 0.0):.8f}  "
                  f"{CONFIG['stablecoin_pair']}="
                  f"{snap.get('usdt_balance', 0.0):.8f}")
        console.print(Rule(style="bright_black"))


# ==================== MAIN ====================
def main():
    global keep_running

    p = argparse.ArgumentParser()
    p.add_argument("--snippets", default=CONFIG["snippets_csv"])
    p.add_argument("--poll", type=float, default=CONFIG["poll_seconds"])
    args = p.parse_args()

    CONFIG["snippets_csv"] = args.snippets
    CONFIG["poll_seconds"] = args.poll

    console.print()
    print_banner()
    console.print(Rule(style="bright_black"))

    if not CONFIG["binance_api_key"] or not CONFIG["binance_api_secret"]:
        log("err", "BINANCE_API_KEY / BINANCE_API_SECRET not set in env.")
        log("err", "Export them and retry, e.g.:")
        console.print("  [cyan]export BINANCE_API_KEY=...[/cyan]")
        console.print("  [cyan]export BINANCE_API_SECRET=...[/cyan]")
        sys.exit(2)

    ensure_wallet_header(Path(CONFIG["wallet_history_csv"]))

    client = BinanceClient(
        CONFIG["binance_api_key"],
        CONFIG["binance_api_secret"],
        CONFIG["binance_base"],
    )
    bot = TradingBot(client)

    # Restore cooldown from state if present.
    state_path = Path(CONFIG["state_json"])
    st = load_state(state_path)
    if "cooldown_until_ts" in st:
        try:
            bot.cooldown_until_ts = float(st["cooldown_until_ts"])
            remaining = int(bot.cooldown_until_ts - time.time())
            if remaining > 0:
                log("boot", f"restored cooldown: {remaining}s remaining")
        except (TypeError, ValueError):
            pass

    # Decide starting mode from wallet.
    bot.decide_initial_mode()

    snippets_path = Path(CONFIG["snippets_csv"])

    # Seed last_seen_row_ms from the current tail so we don't re-fire
    # on historical rows.
    tail = read_snippets_tail(snippets_path, 10)
    if tail:
        print_snippet_panel(tail, "snippets.csv — tail at boot")
        last_row = tail[-1]
        try:
            bot.last_seen_row_ms = int(last_row.get("open_time_ms") or 0)
        except (TypeError, ValueError):
            bot.last_seen_row_ms = 0
        log("boot", f"seeding last_seen_row_ms="
                    f"[cyan]{ms_to_iso_eat(bot.last_seen_row_ms)}[/cyan]")
    else:
        log("warn", "snippets.csv not ready yet; will poll until it appears.")

    console.print(Rule(style="bright_black"))
    log("watch", f"polling [cyan]{snippets_path}[/cyan] every "
                 f"[green]{CONFIG['poll_seconds']}s; Ctrl+C to stop")
    console.print(Rule(style="bright_black"))

    last_heartbeat = time.time()

    while keep_running:
        time.sleep(CONFIG["poll_seconds"])
        if not keep_running:
            break

        try:
            rows = read_snippets_tail(snippets_path, 20)
            if not rows:
                continue

            # Find rows newer than last_seen_row_ms.
            new_rows = []
            for r in rows:
                try:
                    t_ms = int(r.get("open_time_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if t_ms > bot.last_seen_row_ms:
                    new_rows.append((t_ms, r))

            if new_rows:
                new_rows.sort(key=lambda x: x[0])
                print_snippet_panel(rows[-10:], "snippets.csv — tail")
                for t_ms, r in new_rows:
                    bot.handle_new_row(r)
                    bot.last_seen_row_ms = max(bot.last_seen_row_ms, t_ms)

                # Persist cooldown after handling.
                try:
                    save_state(state_path, {
                        "cooldown_until_ts": bot.cooldown_until_ts,
                        "saved_at_utc": iso_now_utc(),
                    })
                except Exception as e:
                    log("warn", f"state save failed: {e}")

            # Periodic heartbeat.
            if time.time() - last_heartbeat > CONFIG["heartbeat_seconds"]:
                last_heartbeat = time.time()
                bot.maybe_heartbeat()

        except Exception as e:
            log("err", f"main loop error: {e}")
            now = time.time()
            if now - bot.last_error_ts > 300:
                bot.last_error_ts = now
                notify("main loop error", str(e))
            time.sleep(2.0)

    log("ok", "done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print()
        console.print("[bold red]interrupted.[/bold red]")
