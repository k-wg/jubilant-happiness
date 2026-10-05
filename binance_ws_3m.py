#!/usr/bin/env python3
# Binance WebSocket 3-minute kline collector (persistent).
#
# Features:
#   - EAT (+03:00) timestamps in CSV and console.
#   - REST backfill of >=300 historical bars on startup.
#   - Live WebSocket streaming of closed bars.
#   - Integrity thread (single thread, two cadences):
#       * every 10 min : tail check on the last 50 rows.
#       * every 180 min: full check on the entire CSV.
#       * on inconsistency: REST-refresh the last 500 closed bars,
#         atomically rewrite the CSV, resume.
#   - Safe to restart (skips bars already written).
#   - Auto-reconnects with exponential backoff.
#
# Visuals: rich for panels / rules / colors / spinners;
#          tabulate for the data tables (rounded_outline).

import csv
import json
import os
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import websocket  # websocket-client
except ImportError:
    print("Missing dependency: websocket-client")
    print("Install with:  pip install websocket-client requests rich tabulate")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("Missing dependency: requests")
    print("Install with:  pip install websocket-client requests rich tabulate")
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
    "ws_base":  "wss://stream.binance.com:9443/stream?streams=",
    "rest_base": "https://api.binance.com",

    "symbol":   "ZECUSDT",
    "interval": "3m",

    "csv_closed": "binance_3m_closed.csv",

    "reconnect_min": 2.0,
    "reconnect_max": 60.0,
    "heartbeat_seconds": 300.0,

    # Timing
    "poll_sleep_seconds": 1.0,
    "tail_check_seconds": 600.0,     # 10 min
    "full_check_seconds": 10800.0,   # 180 min
    "integrity_tick_seconds": 60.0,  # thread wake-up cadence

    # How many tail rows the cheap check inspects.
    "integrity_tail_rows": 50,

    # On inconsistency, how many fresh bars to fetch from REST.
    "refresh_bars": 500,

    # Startup backfill size (minimum).
    "backfill_bars": 6000,

    # REST page size (Binance max is 1000).
    "rest_limit": 1000,

    "verbose": False,

    # Display timezone offset (EAT = +03:00).
    "display_tz_offset_hours": 3,
}

CSV_COLUMNS = [
    "open_time_utc",       # NOTE: values are EAT (+03:00), name kept for compat
    "open_time_ms",
    "close_time_utc",      # NOTE: values are EAT (+03:00)
    "close_time_ms",
    "symbol",
    "interval",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "is_closed",
]

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

DISPLAY_TZ = timezone(timedelta(hours=CONFIG["display_tz_offset_hours"]))

keep_running = True


# ==================== RICH CONSOLE ====================
THEME = Theme({
    "tag.boot":      "bold cyan",
    "tag.open":      "bold green",
    "tag.ws":        "bold blue",
    "tag.closed":    "bold white",
    "tag.hb":        "bold bright_black",
    "tag.integrity": "bold magenta",
    "tag.full":      "bold bright_magenta",
    "tag.ok":        "bold green",
    "tag.warn":      "bold yellow",
    "tag.err":       "bold red",
    "tag.signal":    "bold red on grey11",
    "close.up":      "bold green",
    "close.down":    "bold red",
    "close.flat":    "white",
    "time":          "cyan",
    "num":           "white",
})

console = Console(theme=THEME, highlight=False)

PHASE_TAG = {
    "boot":      "[tag.boot]\\[boot][/tag.boot]",
    "open":      "[tag.open]\\[open][/tag.open]",
    "ws":        "[tag.ws]\\[ws][/tag.ws]",
    "closed":    "[tag.closed]\\[closed][/tag.closed]",
    "hb":        "[tag.hb]\\[heartbeat][/tag.hb]",
    "integrity": "[tag.integrity]\\[integrity][/tag.integrity]",
    "full":      "[tag.full]\\[full][/tag.full]",
    "ok":        "[tag.ok]\\[ok][/tag.ok]",
    "warn":      "[tag.warn]\\[warn][/tag.warn]",
    "err":       "[tag.err]\\[err][/tag.err]",
    "signal":    "[tag.signal]\\[signal][/tag.signal]",
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
def ms_to_iso_eat(ms) -> str:
    """Format a ms-since-epoch as an ISO string in EAT (+03:00)."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=DISPLAY_TZ) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


def now_ms() -> int:
    return int(time.time() * 1000)


# ==================== COLOR HELPERS ====================
def close_style(prev: Optional[float], cur: float) -> str:
    if prev is None:
        return "close.flat"
    if cur > prev:
        return "close.up"
    if cur < prev:
        return "close.down"
    return "close.flat"


def close_text(prev: Optional[float], cur: float) -> Text:
    return Text(f"{cur:>12.4f}", style=close_style(prev, cur))


# ==================== BASIC I/O ====================
def ensure_header(path: Path):
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_COLUMNS)


def read_last_open_time_ms(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    last = 0
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.reader(fh)
        try:
            header = next(r)
        except StopIteration:
            return 0
        try:
            idx = header.index("open_time_ms")
        except ValueError:
            return 0
        for row in r:
            if len(row) <= idx:
                continue
            try:
                v = int(row[idx])
            except (ValueError, TypeError):
                continue
            if v > last:
                last = v
    return last


def read_csv_tail(path: Path, n: int):
    """Read last n rows of the OHLC CSV into parallel lists."""
    if not path.exists() or path.stat().st_size == 0:
        return [], [], []
    dq_utc: deque = deque(maxlen=n)
    dq_ms: deque = deque(maxlen=n)
    dq_close: deque = deque(maxlen=n)
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
                c = float(row["close"])
            except (KeyError, ValueError, TypeError):
                continue
            dq_utc.append(row.get("open_time_utc", ""))
            dq_ms.append(t_ms)
            dq_close.append(c)
    return list(dq_utc), list(dq_ms), list(dq_close)


def read_csv_full(path: Path):
    """Read the entire OHLC CSV into parallel lists."""
    if not path.exists() or path.stat().st_size == 0:
        return [], [], []
    utc: List[str] = []
    ms: List[int] = []
    close: List[float] = []
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
                c = float(row["close"])
            except (KeyError, ValueError, TypeError):
                continue
            utc.append(row.get("open_time_utc", ""))
            ms.append(t_ms)
            close.append(c)
    return utc, ms, close


# ==================== REST HELPERS ====================
def rest_klines(symbol, interval, start_ms=None, end_ms=None, limit=1000):
    """
    Fetch klines from Binance REST. Returns a list of dicts in the CSV
    column shape. Drops the in-progress candle.
    """
    url = f"{CONFIG['rest_base']}/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)

    for attempt in range(5):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            raw = r.json()
            break
        except Exception as e:
            wait = min(2 ** attempt, 30)
            log("warn", f"REST error {e}; retry in {wait}s")
            time.sleep(wait)
    else:
        raise RuntimeError("REST klines failed after retries")

    out = []
    now_ms_local = now_ms()
    for k in raw:
        open_ms = int(k[0])
        close_ms = int(k[6])
        if close_ms >= now_ms_local:
            continue  # skip the in-progress bar
        out.append({
            "open_time_ms": open_ms,
            "close_time_ms": close_ms,
            "open_time_utc": ms_to_iso_eat(open_ms),
            "close_time_utc": ms_to_iso_eat(close_ms),
            "symbol": symbol.upper(),
            "interval": interval,
            "open": k[1],
            "high": k[2],
            "low": k[3],
            "close": k[4],
            "volume": k[5],
            "quote_volume": k[7],
            "trades": k[8],
            "taker_buy_base": k[9],
            "taker_buy_quote": k[10],
            "is_closed": True,
        })
    return out


def rest_fetch_last_n(symbol, interval, n):
    """
    Fetch the last n closed bars from REST. Uses endTime=now and pages
    backwards until we have n bars or REST stops returning data.
    Returns a list in chronological order (oldest first).
    """
    step = INTERVAL_MS[interval]
    out_rev: List[dict] = []
    end = now_ms()
    page = CONFIG["rest_limit"]
    guard = 0
    while len(out_rev) < n and guard < 20:
        guard += 1
        want = min(page, n - len(out_rev))
        batch = rest_klines(symbol, interval,
                            start_ms=end - step * want * 2,
                            end_ms=end,
                            limit=page)
        if not batch:
            break
        # Keep only strictly older than our current oldest, and dedup.
        if out_rev:
            cutoff = out_rev[-1]["open_time_ms"]
            batch = [b for b in batch if b["open_time_ms"] < cutoff]
        # Reverse to oldest-first, extend our reversed accumulator.
        for b in reversed(batch):
            out_rev.append(b)
            if len(out_rev) >= n:
                break
        # Move endTime back past the oldest bar we just got.
        end = out_rev[-1]["open_time_ms"] - 1
        time.sleep(0.2)
    out_rev.reverse()
    return out_rev[-n:] if len(out_rev) > n else out_rev


# ==================== MESSAGE PARSING ====================
def parse_kline(msg):
    try:
        if msg.get("e") != "kline":
            return None
        k = msg["k"]
        return {
            "open_time_ms": k["t"],
            "close_time_ms": k["T"],
            "open_time_utc": ms_to_iso_eat(k["t"]),
            "close_time_utc": ms_to_iso_eat(k["T"]),
            "symbol": k["s"],
            "interval": k["i"],
            "open": k["o"],
            "high": k["h"],
            "low": k["l"],
            "close": k["c"],
            "volume": k["v"],
            "quote_volume": k["q"],
            "trades": k["n"],
            "taker_buy_base": k.get("V", ""),
            "taker_buy_quote": k.get("Q", ""),
            "is_closed": bool(k["x"]),
        }
    except Exception as e:
        log("err", f"parse error: {e}")
        return None


# ==================== SHARED STATE ====================
class Shared:
    def __init__(self):
        self.lock = threading.RLock()
        self.last_open_time_ms: int = 0
        self.bars_written: int = 0
        self.last_heartbeat_ts: float = 0.0
        # Last integrity scan result, for the banner/table.
        self.last_scan_kind: str = "-"
        self.last_scan_ok: Optional[bool] = None
        self.last_scan_ts: float = 0.0
        self.last_scan_detail: str = ""


shared = Shared()


# ==================== CSV WRITE (atomic + append) ====================
def append_row_locked(path: Path, row: dict):
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow([row.get(c, "") for c in CSV_COLUMNS])


def write_csv_full_locked(path: Path, rows: List[dict]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_COLUMNS)
        for row in rows:
            w.writerow([row.get(c, "") for c in CSV_COLUMNS])
    os.replace(tmp, path)


# ==================== TABLES (tabulate) ====================
def render_tail_table(utc, ms, closes, n: int = 10) -> str:
    if not closes:
        return "(no data)"
    start = max(0, len(closes) - n)

    def c_close(prev, cur):
        if prev is None:
            return f"\x1b[37m{cur:>12.4f}\x1b[0m"
        if cur > prev:
            return f"\x1b[1;32m{cur:>12.4f}\x1b[0m"
        if cur < prev:
            return f"\x1b[1;31m{cur:>12.4f}\x1b[0m"
        return f"\x1b[37m{cur:>12.4f}\x1b[0m"

    def c_time(s):
        return f"\x1b[36m{s}\x1b[0m"

    rows = []
    prev = closes[start - 1] if start > 0 else None
    for i in range(start, len(closes)):
        rows.append([
            c_time(utc[i]),
            str(ms[i]),
            c_close(prev, closes[i]),
        ])
        prev = closes[i]

    return tabulate(
        rows,
        headers=["open_time_eat", "open_time_ms", "close"],
        tablefmt="rounded_outline",
        colalign=("left", "right", "right"),
        disable_numparse=True,
    )


def render_scan_table() -> str:
    kind = shared.last_scan_kind
    ok = shared.last_scan_ok
    detail = shared.last_scan_detail
    if ok is None:
        ok_txt = "\x1b[2mn/a\x1b[0m"
    elif ok:
        ok_txt = "\x1b[1;32mOK\x1b[0m"
    else:
        ok_txt = "\x1b[1;31mRECOMPUTED\x1b[0m"
    when = (ms_to_iso_eat(int(shared.last_scan_ts * 1000))
            if shared.last_scan_ts else "-")
    rows = [[
        f"\x1b[35m{kind}\x1b[0m",
        ok_txt,
        f"\x1b[36m{when}\x1b[0m",
        detail,
    ]]
    return tabulate(
        rows,
        headers=["scan", "result", "when_eat", "detail"],
        tablefmt="rounded_outline",
        colalign=("left", "left", "left", "left"),
        disable_numparse=True,
    )


def print_tail_panel(utc, ms, closes, title: str):
    body = render_tail_table(utc, ms, closes, n=10)
    console.print(Panel(
        body,
        title=f"[bold white]{title}[/bold white]",
        border_style="bright_blue",
        padding=(0, 1),
    ))


def print_scan_panel(title: str = "last integrity scan"):
    body = render_scan_table()
    console.print(Panel(
        body,
        title=f"[bold white]{title}[/bold white]",
        border_style="bright_magenta",
        padding=(0, 1),
    ))


# ==================== BANNER ====================
def print_banner(csv_path: Path):
    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="right", style="bold bright_black")
    grid.add_column(style="white")

    grid.add_row("symbol",   f"[yellow]{CONFIG['symbol']}[/yellow]")
    grid.add_row("interval", f"[yellow]{CONFIG['interval']}[/yellow]")
    grid.add_row("csv",      f"[cyan]{csv_path}[/cyan]")
    grid.add_row("timezone", f"[green]EAT (+03:00)[/green]")
    grid.add_row("tail check",
                 f"[bright_magenta]{CONFIG['tail_check_seconds'] / 60:.0f} min[/bright_magenta] "
                 f"[bright_black](last {CONFIG['integrity_tail_rows']} rows)[/bright_black]")
    grid.add_row("full check",
                 f"[bright_magenta]{CONFIG['full_check_seconds'] / 60:.0f} min[/bright_magenta] "
                 f"[bright_black](entire CSV)[/bright_black]")
    grid.add_row("refresh bars", f"[magenta]{CONFIG['refresh_bars']}[/magenta]")
    grid.add_row("heartbeat", f"[bright_black]{CONFIG['heartbeat_seconds']:.0f}s[/bright_black]")

    console.print(Panel(
        Align.left(grid),
        title="[bold white on blue] BINANCE 3m WS COLLECTOR [/bold white on blue]",
        subtitle="[italic bright_black]EAT timestamps · rich + tabulate[/italic bright_black]",
        border_style="bright_blue",
        padding=(1, 2),
    ))


# ==================== INTEGRITY CHECKS ====================
def check_sequence(times_ms: List[int], interval_ms: int) -> Optional[str]:
    if len(times_ms) < 2:
        return None
    for i in range(1, len(times_ms)):
        delta = times_ms[i] - times_ms[i - 1]
        if delta <= 0:
            return (f"non-increasing at index {i}: "
                    f"{times_ms[i-1]} -> {times_ms[i]}")
        if delta != interval_ms:
            return (f"gap at index {i}: expected +{interval_ms}ms, "
                    f"got +{delta}ms ({ms_to_iso_eat(times_ms[i-1])} -> "
                    f"{ms_to_iso_eat(times_ms[i])})")
    return None


def integrity_check_tail(csv_path: Path, interval_ms: int) -> bool:
    """
    Cheap, bounded check: last N rows only, plus the shared state.
    Returns True if consistent, False if a refresh was performed.
    """
    n = CONFIG["integrity_tail_rows"]
    with shared.lock:
        utc, ms, closes = read_csv_tail(csv_path, n)
        if not ms:
            shared.last_scan_kind = "tail"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = "empty CSV"
            log("integrity", "tail empty; nothing to verify.")
            return True

        reasons = []
        seq_reason = check_sequence(ms, interval_ms)
        if seq_reason:
            reasons.append(f"tail sequence: {seq_reason}")

        max_in = ms[-1]
        if shared.last_open_time_ms != max_in:
            reasons.append(
                f"shared.last_open_time_ms={ms_to_iso_eat(shared.last_open_time_ms)} "
                f"!= newest row={ms_to_iso_eat(max_in)}"
            )

        if not reasons:
            shared.last_scan_kind = "tail"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = f"last {len(ms)} rows OK"
            log("ok", f"tail OK — last [bold]{len(ms)}[/bold] rows, "
                      f"last=[cyan]{ms_to_iso_eat(max_in)}[/cyan]")
            return True

        console.print(Rule(style="yellow"))
        log("warn", "[bold]TAIL INCONSISTENCY[/bold]:")
        for r in reasons:
            log("warn", f"  • {r}")

        shared.last_scan_kind = "tail"
        shared.last_scan_ok = False
        shared.last_scan_ts = time.time()
        shared.last_scan_detail = "; ".join(reasons)[:200]

        do_refresh(csv_path, "tail")
        return False


def integrity_check_full(csv_path: Path, interval_ms: int) -> bool:
    """
    Expensive, complete check of the whole CSV.
    Returns True if consistent, False if a refresh was performed.
    """
    with shared.lock:
        utc, ms, closes = read_csv_full(csv_path)
        if not ms:
            shared.last_scan_kind = "full"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = "empty CSV"
            log("full", "full scan: empty CSV.")
            return True

        reasons = []
        seq_reason = check_sequence(ms, interval_ms)
        if seq_reason:
            reasons.append(f"sequence: {seq_reason}")

        max_in = ms[-1]
        if shared.last_open_time_ms != max_in:
            reasons.append(
                f"shared.last_open_time_ms={ms_to_iso_eat(shared.last_open_time_ms)} "
                f"!= newest row={ms_to_iso_eat(max_in)}"
            )

        if not reasons:
            shared.last_scan_kind = "full"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = f"{len(ms)} rows OK"
            log("ok", f"FULL scan OK — [bold]{len(ms)}[/bold] rows, "
                      f"last=[cyan]{ms_to_iso_eat(max_in)}[/cyan]")
            return True

        console.print(Rule(style="yellow"))
        log("warn", "[bold]FULL-SCAN INCONSISTENCY[/bold]:")
        for r in reasons:
            log("warn", f"  • {r}")

        shared.last_scan_kind = "full"
        shared.last_scan_ok = False
        shared.last_scan_ts = time.time()
        shared.last_scan_detail = "; ".join(reasons)[:200]

        do_refresh(csv_path, "full")
        return False


# ==================== REFRESH FROM REST ====================
def do_refresh(csv_path: Path, reason: str):
    """
    Fetch the last `refresh_bars` closed bars from REST and atomically
    rewrite the CSV. Called on inconsistency.
    """
    n = CONFIG["refresh_bars"]
    log("warn", f"refreshing last [bold]{n}[/bold] bars from REST "
                f"([bright_black]{reason}[/bright_black]) …")

    with Progress(
        SpinnerColumn(style="bright_magenta"),
        TextColumn("[bold bright_magenta]REST refresh[/bold bright_magenta]"),
        BarColumn(bar_width=None, complete_style="bright_magenta",
                  finished_style="green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console, transient=True,
    ) as prog:
        task = prog.add_task("refresh", total=n)
        try:
            bars = rest_fetch_last_n(CONFIG["symbol"], CONFIG["interval"], n)
        except Exception as e:
            log("err", f"REST refresh failed: {e}")
            return
        prog.update(task, advance=len(bars))

    if not bars:
        log("err", "REST returned no bars; leaving CSV unchanged.")
        return

    # Dedup and sort by open_time_ms, then write atomically.
    seen = set()
    unique = []
    for b in sorted(bars, key=lambda x: x["open_time_ms"]):
        if b["open_time_ms"] in seen:
            continue
        seen.add(b["open_time_ms"])
        unique.append(b)

    write_csv_full_locked(csv_path, unique)

    last_ms = unique[-1]["open_time_ms"]
    shared.last_open_time_ms = last_ms
    shared.bars_written = len(unique)

    log("ok", f"refresh done: wrote [bold]{len(unique)}[/bold] rows, "
              f"last=[cyan]{ms_to_iso_eat(last_ms)}[/cyan]")

    # Show the new tail.
    utc, ms, closes = read_csv_tail(csv_path, 10)
    if ms:
        print_tail_panel(utc, ms, closes, "tail (after REST refresh)")
    console.print(Rule(style="bright_black"))


# ==================== STARTUP BACKFILL ====================
def startup_backfill(csv_path: Path):
    """
    Ensure at least CONFIG['backfill_bars'] closed bars exist.
    If the file exists, just top up the tail.
    """
    step = INTERVAL_MS[CONFIG["interval"]]
    ensure_header(csv_path)
    shared.last_open_time_ms = read_last_open_time_ms(csv_path)

    now_ms_local = now_ms()
    last_closed_open = (now_ms_local // step) * step - step

    if shared.last_open_time_ms == 0:
        start_ms = last_closed_open - step * (CONFIG["backfill_bars"] - 1)
        log("boot", f"fresh start: fetching [bold]{CONFIG['backfill_bars']}[/bold] "
                    f"bars from [cyan]{ms_to_iso_eat(start_ms)}[/cyan]")
    elif shared.last_open_time_ms < last_closed_open:
        start_ms = shared.last_open_time_ms + step
        log("boot", f"tail gap: fetching from "
                    f"[cyan]{ms_to_iso_eat(start_ms)}[/cyan]")
    else:
        log("boot", "CSV already up to date.")
        return

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]REST backfill[/bold cyan]"),
        BarColumn(bar_width=None, complete_style="cyan",
                  finished_style="green"),
        TimeColumn_placeholder := TextColumn(
            "[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console, transient=True,
    ) as prog:
        task = prog.add_task("backfill", total=None)
        cursor = start_ms
        written = 0
        while cursor <= last_closed_open and keep_running:
            batch = rest_klines(CONFIG["symbol"], CONFIG["interval"],
                                start_ms=cursor,
                                end_ms=min(cursor + step * CONFIG["rest_limit"],
                                           last_closed_open + step),
                                limit=CONFIG["rest_limit"])
            if not batch:
                cursor += step * CONFIG["rest_limit"]
                continue
            new_rows = [b for b in batch
                        if b["open_time_ms"] > shared.last_open_time_ms]
            for b in new_rows:
                append_row_locked(csv_path, b)
                shared.last_open_time_ms = max(
                    shared.last_open_time_ms, b["open_time_ms"])
                written += 1
                prog.update(task, advance=1)
            last = batch[-1]["open_time_ms"]
            cursor = last + step
            if len(batch) < CONFIG["rest_limit"] and cursor > last_closed_open:
                break
            time.sleep(0.2)

    log("ok", f"backfill wrote [bold]{written}[/bold] bars; "
              f"last=[cyan]{ms_to_iso_eat(shared.last_open_time_ms)}[/cyan]")

    utc, ms, closes = read_csv_tail(csv_path, 10)
    if ms:
        print_tail_panel(utc, ms, closes, "tail (after startup backfill)")


# ==================== WEBSOCKET CALLBACKS ====================
def stream_url(symbol, interval):
    return f"{CONFIG['ws_base']}{symbol.lower()}@kline_{interval}"


def on_open(ws):
    log("open", f"{ws.url}")
    log("open", f"last committed open_time_ms = "
                f"[cyan]{ms_to_iso_eat(shared.last_open_time_ms)}[/cyan]")
    with shared.lock:
        shared.last_heartbeat_ts = time.time()


def on_message(ws, message):
    try:
        outer = json.loads(message)
        data = outer.get("data", outer)
        row = parse_kline(data)
        if row is None:
            return

        if row["is_closed"]:
            with shared.lock:
                if row["open_time_ms"] > shared.last_open_time_ms:
                    append_row_locked(Path(CONFIG["csv_closed"]), row)
                    shared.last_open_time_ms = row["open_time_ms"]
                    shared.bars_written += 1
                    # Colorized per-bar print.
                    line = Text()
                    line.append("  ● ", style="bright_black")
                    line.append(row["open_time_utc"], style="time")
                    line.append("  close=")
                    line.append_text(close_text(None, float(row["close"])))
                    line.append("  V=", style="bright_black")
                    line.append(f"{float(row['volume']):.4f}", style="num")
                    console.print(line)

        # Heartbeat.
        now = time.time()
        with shared.lock:
            last_hb = shared.last_heartbeat_ts
            if now - last_hb >= CONFIG["heartbeat_seconds"]:
                shared.last_heartbeat_ts = now
                should_log = True
            else:
                should_log = False
        if should_log:
            console.print(Rule(style="bright_black"))
            log("hb", f"{ms_to_iso_eat(now_ms())}  "
                      f"last_bar=[cyan]{ms_to_iso_eat(shared.last_open_time_ms)}[/cyan]  "
                      f"bars_written=[magenta]{shared.bars_written}[/magenta]")
            console.print(Rule(style="bright_black"))

        if CONFIG["verbose"]:
            log("ws", f"tick {row['open_time_utc']} "
                      f"close={row['close']} closed={row['is_closed']}")

    except Exception as e:
        log("err", f"message error: {e}")


def on_error(ws, error):
    log("err", f"{error}")


def on_close(ws, status_code, msg):
    log("warn", f"closed: code={status_code} msg={msg}")


# ==================== INTEGRITY THREAD (two cadences) ====================
def integrity_thread(csv_path: Path):
    interval_ms = INTERVAL_MS[CONFIG["interval"]]
    tick = CONFIG["integrity_tick_seconds"]
    tail_every = CONFIG["tail_check_seconds"]
    full_every = CONFIG["full_check_seconds"]

    next_tail = time.time() + tail_every
    next_full = time.time() + full_every

    while keep_running:
        slept = 0.0
        while keep_running and slept < tick:
            time.sleep(min(1.0, tick - slept))
            slept += 1.0
        if not keep_running:
            break

        now = time.time()

        # Full takes precedence.
        if now >= next_full:
            try:
                integrity_check_full(csv_path, interval_ms)
            except Exception as e:
                log("err", f"full scan error: {e}")
            next_full = time.time() + full_every
            next_tail = time.time() + tail_every
            continue

        if now >= next_tail:
            try:
                integrity_check_tail(csv_path, interval_ms)
            except Exception as e:
                log("err", f"tail scan error: {e}")
            next_tail = time.time() + tail_every


# ==================== MAIN LOOP ====================
def run():
    csv_path = Path(CONFIG["csv_closed"])

    console.print()
    print_banner(csv_path)
    console.print(Rule(style="bright_black"))

    # 1. Startup REST backfill.
    try:
        startup_backfill(csv_path)
    except Exception as e:
        log("err", f"startup backfill failed: {e}")

    console.print(Rule(style="bright_black"))

    # 2. Integrity thread.
    it = threading.Thread(target=integrity_thread, args=(csv_path,),
                          daemon=True, name="integrity")
    it.start()
    log("integrity", f"thread started; tail every "
                     f"[bright_magenta]{CONFIG['tail_check_seconds'] / 60:.0f} min[/bright_magenta], "
                     f"full every "
                     f"[bright_magenta]{CONFIG['full_check_seconds'] / 60:.0f} min[/bright_magenta]")

    console.print(Rule(style="bright_black"))

    # 3. Live WebSocket loop.
    url = stream_url(CONFIG["symbol"], CONFIG["interval"])
    backoff = CONFIG["reconnect_min"]

    while keep_running:
        log("ws", f"connecting {url}")
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            ws.run_forever(ping_interval=180, ping_timeout=60)
        except Exception as e:
            log("err", f"run_forever: {e}")

        if not keep_running:
            break

        log("ws", f"reconnecting in {backoff:.1f}s …")
        time.sleep(backoff)
        backoff = min(backoff * 1.5, CONFIG["reconnect_max"])

    log("ok", "done.")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        console.print()
        console.print("[bold red]interrupted.[/bold red]")