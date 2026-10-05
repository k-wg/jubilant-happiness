#!/usr/bin/env python3
# Pine Script Snippets -> Python faithful port.
#
# SOURCE OF TRUTH: the RUNNABLE Pine code ONLY. Comments in pinescript.txt
# are IGNORED entirely. Where the comment text disagrees with the code
# (e.g. "Step 3: Wait for rsi_ma50 < 35" while the code reads
# `if rsi_ma50 < 100`), we use the CODE.
#
# Null-safety: in Pine, any comparison involving `na` yields `na`, which is
# falsy in boolean contexts. Python raises TypeError instead. Every numeric
# comparison in the snippet evaluator therefore goes through `_gt/_lt/_gte/
# _lte`, which return False if either operand is None. This matches Pine's
# warm-up behaviour exactly (nothing fires until indicators are ready).
#
# Reads the OHLC CSV (binance_3m_closed.csv by default), recomputes ALL
# indicators exactly as in pinescript.txt, runs ALL snippets as state
# machines matching Pine's var-persistence semantics, and writes a
# snippets.csv with one row per bar.
#
# The output CSV keeps only the aggregated snippet columns:
#   plotted, levels, level_9, level_35, level_50, level_75, level_93
# (placed right after rsi_ma50) alongside the full indicator set
# (all SMAs + Auto-Fib + BB + LR + points + col_up/col_dn). Per-snippet
# trigger/signal columns are NOT written to the CSV (they'd balloon the
# file); the underlying state is still persisted in the state JSON so
# warm-up and cross-bar behaviour are identical to TradingView.
#
# Visuals: rich for panels / rules / colors / spinners;
#          tabulate for the data tables (rounded_outline).

import argparse
import csv
import json
import math
import os
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
DEFAULTS = {
    "input_csv": "binance_3m_closed.csv",
    "output_csv": "snippets.csv",
    "state_json": "snippets_state.json",

    # Base indicator parameters (verbatim from pinescript.txt runnable code).
    "rsi_length": 7,
    "rsi_sma_length": 50,
    "ob": 65,
    "os": 35,

    "ma_fast": 350,     # long350
    "ma_slow": 500,     # long500
    "bb_length": 200,
    "bb_mult": 2.0,
    "lr_length": 250,
    "lr_upper": 1.5,
    "lr_lower": -1.5,
    "lr_norm_len": 100,
    "inv_lookback": 5,

    # Pine SMA block (runnable code).
    "short_002": 2,
    "short_007": 7,
    "short_14":  14,
    "short_50":  50,
    "long_100":  100,
    "long_200":  200,
    "long_350":  350,
    "long_500":  500,

    # Auto-Fib.
    "fiblength": 5500,

    # Timing / IO.
    "poll": 1.0,
    "heartbeat_seconds": 300.0,
    "interval_ms": 180_000,
    "display_tz_offset_hours": 3,
}

DISPLAY_TZ = timezone(timedelta(hours=DEFAULTS["display_tz_offset_hours"]))

# Each snippet's *uncommented* plotchar() call, if any.
# (level, emoji, plotted?).
#
# Every commented-out plotchar is NOT included here. Its trigger is still
# computed and written to the state file, but it does NOT feed "plotted" /
# "levels" / "level_*".
SNIPPET_SPECS = {
    "snip1":   (None, "✨", False),
    "snip6":   (None, "💥", False),
    "snip7":   (None, "⚡️", False),
    "snip9":   (None, "🚀", False),
    "snip15":  (None, "⚡️", False),
    "snip16":  (9,    "⚡️", True),    # plotchar(snip16_trigger2 ? 9 : na, ...)
    "snip34":  (None, "🚀", False),
    "snip47":  (35,   "💥", True),    # plotchar(snip47_trigger_path1 ? 35 : na, ...)
    "snip61":  (None, "❌", False),
    "snip63":  (None, "🔰", False),
    "snip64":  (50,   "🚨", True),    # plotchar(snip64_trigger ? 50 : na, ...)
    "snip66":  (None, "⚓", False),
    "snip67":  (None, "📌", False),
    "snip68":  (None, "🎤", False),
    "snip69":  (None, "☢️", False),
    "snip70":  (None, "📛", False),
    "snip72":  (9,    "🎯", True),    # plotchar(snip72_trigger ? 9 : na, ...)
    "snip73":  (None, "💡", False),
    "snip74":  (None, "🔔", False),
    "snip76":  (None, "🌟", False),
    "snip78":  (None, "💀", False),
    "snip79":  (None, "🔶", False),
    "snip82":  (93,   "⚠️", True),    # plotchar(snip82_trigger ? 93 : na, ...)
    "snip83":  (None, "🔱", False),
    "snip84":  (None, "🔻", False),
    "snip85":  (None, "☠️", False),
    "snip86":  (None, "👽", False),
    "snip88":  (None, "☠️", False),
    "snip89":  (None, "👽", False),
    "snip90":  (None, "👽", False),
    "snip91":  (None, "☠️", False),
    "snip94":  (9,    "🌙", True),    # plotchar(snip94_trigger ? 9 : na, ...)
    "snip95":  (None, "⛔", False),
    "snip97":  (None, "🏆", False),
    "snip99":  (93,   "☣️", True),    # plotchar(snip99_trigger ? 93 : na, "☣️", ...)
    "snip100": (93,   "🚧", True),    # plotchar(snip100_trigger ? 93 : na, "🚧", ...)
    "snip102": (9,    "🎶", True),    # plotchar(snip102_trigger ? 9 : na, "🎶", ...)
    "snip104": (None, "🔱", False),
    "snip105": (None, "🔱", False),
    "snip107": (35,   "🌟", True),    # plotchar(snip107_trigger ? 35 : na, ...)
    "snip108": (9,    "🌟", True),    # plotchar(snip108_trigger ? 9 : na, ...)
    "snip109": (None, "🍁", False),
    "snip111": (9,    "✨", True),    # plotchar(snip111_trigger ? 9 : na, ...)
    "snip112": (9,    "🚀", True),    # plotchar(snip112_trigger ? 9 : na, ...)
    "snip114": (9,    "💧", True),    # plotchar(snip114_trigger ? 9 : na, ...)
    "snip119": (9,    "📌", True),    # plotchar(snip119_trigger ? 9 : na, ...)
    "snip120": (93,   "📣", True),    # plotchar(snip120_trigger ? 93 : na, ...)
    "snip121": (9,    "💡", True),    # plotchar(snip121_trigger ? 9 : na, ...)
    "snip123": (None, "👺", False),
    "snip125": (None, "👿", False),
    "snip126": (35,   "👻", True),    # plotchar(snip126_trigger_93 ? 35 : na, "👻", ...)
    "snip127": (35,   "🌹", True),    # plotchar(snip127_trigger ? 35 : na, "🌹", ...)
    "snip128": (9,    "🐮", True),    # plotchar(snip128_trigger ? 9 : na, "🐮", ...)
    "snip129": (35,   "🦁", True),    # plotchar(snip129_trigger ? 35 : na, "🦁", ...)
    "snip130": (None, "🍁", False),
    "snip131": (75,   "👽", True),    # plotchar(snip131_trigger ? 75 : na, "👽", ...)
    "snip132": (75,   "☠️", True),    # plotchar(snip132_trigger ? 75 : na, "☠️", ...)
    "snip133": (93,   "☠️", True),    # plotchar(snip133_trigger ? 93 : na, "☠️", ...)
    "snip134": (93,   "👽", True),    # plotchar(snip134_trigger ? 93 : na, "👽", ...)
    "snip135": (75,   "👾", True),    # plotchar(snip135_trigger ? 75 : na, "👾", ...)
    "snip136": (75,   "🔱", True),    # plotchar(snip136_trigger ? 75 : na, "🔱", ...)
    "snip137": (93,   "🔱", True),    # plotchar(snip137_trigger ? 93 : na, "🔱", ...)
    "snip138": (93,   "🏆", True),    # plotchar(snip138_trigger ? 93 : na, "🏆", ...)
    "snip139": (93,   "👿", True),    # plotchar(snip139_trigger ? 93 : na, "👿", ...)
    "snip140": (93,   "👺", True),    # plotchar(snip140_trigger ? 93 : na, "👺", ...)
    "snip141": (93,   "🍁", True),    # plotchar(snip141_trigger ? 93 : na, "🍁", ...)
    "snip142": (75,   "🔔", True),    # plotchar(snip142_trigger ? 75 : na, "🔔", ...)
    "snip143": (9,    "🔔", True),    # plotchar(snip143_trigger ? 9 : na, "🔔", ...)
}

ALL_LEVELS = sorted({9, 35, 50, 75, 93})

_SNIPPET_NAMES_SORTED = sorted(SNIPPET_SPECS.keys(), key=lambda s: int(s[4:]))


# ==================== SLIM OUTPUT SCHEMA ====================
# Layout: identity -> oscillators -> snippet aggregations -> the rest.
OUTPUT_COLUMNS = [
    # identity
    "open_time_utc",
    "open_time_ms",
    "close",
    "high",
    "low",

    # base oscillators
    "rsi",
    "rsi_ma50",

    # snippet aggregations (moved to the LEFT, right after rsi_ma50)
    "plotted",
    "levels",
    *[f"level_{lv}" for lv in ALL_LEVELS],

    # ma_inv.py block
    "ma_350",
    "ma_500",
    "bbbasis",
    "bbdev",
    "bbupper",
    "bblower",
    "lr",
    "cond1",
    "cond2",
    "points",
    "col_up",
    "col_dn",
    "col_up_active",
    "col_dn_active",

    # pinescript.txt SMA block
    "short_002",
    "short_007",
    "short_14",
    "short_50",
    "long_100",
    "long_200",
    "long_350",
    "long_500",

    # pinescript.txt Auto-Fib block
    "fiblength",
    "maxr",
    "minr",
    "ranr",
    "fibo_1",
    "fibo_0_764",
    "fibo_0_618",
    "fibo_0_5",
    "fibo_0_382",
    "fibo_0_236",
    "fibo_0",
]

keep_running = True


# ==================== RICH CONSOLE ====================
THEME = Theme({
    "tag.boot":      "bold cyan",
    "tag.watch":     "bold blue",
    "tag.integrity": "bold magenta",
    "tag.full":      "bold bright_magenta",
    "tag.hb":        "bold bright_black",
    "tag.ok":        "bold green",
    "tag.warn":      "bold yellow",
    "tag.err":       "bold red",
    "tag.signal":    "bold red on grey11",
    "close.up":      "bold green",
    "close.down":    "bold red",
    "close.flat":    "white",
    "time":          "cyan",
})

console = Console(theme=THEME, highlight=False)

PHASE_TAG = {
    "boot":      "[tag.boot]\\[boot][/tag.boot]",
    "watch":     "[tag.watch]\\[watch][/tag.watch]",
    "integrity": "[tag.integrity]\\[integrity][/tag.integrity]",
    "full":      "[tag.full]\\[full][/tag.full]",
    "hb":        "[tag.hb]\\[heartbeat][/tag.hb]",
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
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=DISPLAY_TZ) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


def ms_to_iso_utc(ms) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


# ==================== CSV I/O ====================
def read_ohlc_csv(path: Path):
    times_utc: List[str] = []
    times_ms: List[int] = []
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    if not path.exists():
        return times_utc, times_ms, opens, highs, lows, closes
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
                c = float(row["close"])
                h = float(row.get("high", c))
                l = float(row.get("low", c))
                o = float(row.get("open", c))
            except (KeyError, ValueError, TypeError):
                continue
            times_utc.append(row.get("open_time_utc", ""))
            times_ms.append(t_ms)
            opens.append(o)
            highs.append(h)
            lows.append(l)
            closes.append(c)
    return times_utc, times_ms, opens, highs, lows, closes


def ensure_output_header(path: Path):
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(OUTPUT_COLUMNS)


def write_output_csv(path: Path, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(OUTPUT_COLUMNS)
        for row in rows:
            w.writerow(row)
    os.replace(tmp, path)


def append_output_rows(path: Path, rows):
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for row in rows:
            w.writerow(row)


# ==================== PINE RSI ====================
class PineRsiState:
    """ta.rsi + ta.sma(rsi, N)."""
    def __init__(self, rsi_length: int, sma_length: int):
        self.rsi_length = rsi_length
        self.sma_length = sma_length
        self.u_warmup: List[float] = []
        self.d_warmup: List[float] = []
        self.prev_rma_u: Optional[float] = None
        self.prev_rma_d: Optional[float] = None
        self.rsi_window: List[float] = []
        self.rsi_window_sum: float = 0.0
        self.last_close: Optional[float] = None
        self.bars_fed: int = 0

    def seed_rma(self, warmup: List[float]) -> Optional[float]:
        if len(warmup) < self.rsi_length:
            return None
        return sum(warmup) / self.rsi_length

    def step(self, close: float):
        if self.last_close is None:
            chg = None
        else:
            chg = close - self.last_close
        self.last_close = close
        self.bars_fed += 1

        if chg is None:
            u = 0.0
            d = 0.0
        else:
            u = chg if chg > 0 else 0.0
            d = -chg if chg < 0 else 0.0

        if self.prev_rma_u is None:
            self.u_warmup.append(u)
            if len(self.u_warmup) >= self.rsi_length:
                self.prev_rma_u = self.seed_rma(self.u_warmup)
        else:
            alpha = 1.0 / self.rsi_length
            self.prev_rma_u = alpha * u + (1.0 - alpha) * self.prev_rma_u

        if self.prev_rma_d is None:
            self.d_warmup.append(d)
            if len(self.d_warmup) >= self.rsi_length:
                self.prev_rma_d = self.seed_rma(self.d_warmup)
        else:
            alpha = 1.0 / self.rsi_length
            self.prev_rma_d = alpha * d + (1.0 - alpha) * self.prev_rma_d

        if self.prev_rma_u is None or self.prev_rma_d is None:
            rsi = None
        elif self.prev_rma_d == 0:
            rsi = 100.0
        elif self.prev_rma_u == 0:
            rsi = 0.0
        else:
            rsi = 100.0 - 100.0 / (1.0 + self.prev_rma_u / self.prev_rma_d)

        rsi_ma = None
        if rsi is not None:
            self.rsi_window.append(rsi)
            self.rsi_window_sum += rsi
            if len(self.rsi_window) > self.sma_length:
                old = self.rsi_window.pop(0)
                self.rsi_window_sum -= old
            if len(self.rsi_window) == self.sma_length:
                rsi_ma = self.rsi_window_sum / self.sma_length

        return rsi, rsi_ma

    def to_dict(self):
        return {
            "rsi_length": self.rsi_length,
            "sma_length": self.sma_length,
            "u_warmup": self.u_warmup,
            "d_warmup": self.d_warmup,
            "prev_rma_u": self.prev_rma_u,
            "prev_rma_d": self.prev_rma_d,
            "rsi_window": self.rsi_window,
            "rsi_window_sum": self.rsi_window_sum,
            "last_close": self.last_close,
            "bars_fed": self.bars_fed,
        }

    @classmethod
    def from_dict(cls, d):
        s = cls(d["rsi_length"], d["sma_length"])
        s.u_warmup = list(d.get("u_warmup") or [])
        s.d_warmup = list(d.get("d_warmup") or [])
        s.prev_rma_u = d.get("prev_rma_u")
        s.prev_rma_d = d.get("prev_rma_d")
        s.rsi_window = list(d.get("rsi_window") or [])
        s.rsi_window_sum = float(d.get("rsi_window_sum") or 0.0)
        s.last_close = d.get("last_close")
        s.bars_fed = int(d.get("bars_fed") or 0)
        return s


# ==================== SNIPPET STATE ====================
class SnippetState:
    """
    All var-persistent state. Attribute names mirror Pine exactly.
    Per-bar triggers are recomputed each bar (reset to False at bar top).
    """
    def __init__(self):
        self.snippet1_condition = False
        self.snippet6_condition = False
        self.snippet7_condition = False
        self.snippet9_condition = False

        # snip15 / 16
        self.snip15_armed = False
        self.snip15_trigger = False
        self.snip16_armed = False
        self.snip16_trigger = False
        self.snip16_waiting_rsi = False
        self.snip16_rsi_high = False
        self.snip16_trigger2 = False

        # snip34
        self.snip34_state = 0
        self.snip34_trigger = False

        # snip47
        self.snip47_armed = False
        self.snip47_captured_bbupper = None
        self.snip47_captured_bblower = None
        self.snip47_trigger_path1 = False
        self.snip47_trigger_path2 = False

        # snip61
        self.snip61_armed = False
        self.snip61_trigger1 = False
        self.snip61_trigger2 = False
        self.snip61_trigger_phone = False
        self.snip61_trigger_mobile = False

        # snip63
        self.snip63_state = 0
        self.snip63_trigger = False

        # snip64
        self.snip64_armed = False
        self.snip64_trigger = False

        # snip66
        self.snip66_trigger_anchor = False
        self.snip66_trigger_hook = False

        # snip67
        self.snip67_state = 0
        self.snip67_trigger = False

        # snip68
        self.snip68_trigger_mic = False
        self.snip68_trigger_comet = False

        # snip69
        self.snip69_state = 0
        self.snip69_trigger = False

        # snip70
        self.snip70_state = 0
        self.snip70_trigger = False

        # snip72
        self.snip72_state = 0
        self.snip72_trigger = False

        # snip73
        self.snip73_state = 0
        self.snip73_trigger = False

        # snip74
        self.snip74_trigger = False

        # snip76
        self.snip76_state = 0
        self.snip76_trigger = False

        # snip78
        self.snip78_trigger = False

        # snip79
        self.snip79_state = 0
        self.snip79_trigger = False

        # snip82
        self.snip82_state = 0
        self.snip82_trigger = False

        # snip83
        self.snip83_state = 0
        self.snip83_trigger = False

        # snip84
        self.snip84_state = 0
        self.snip84_trigger = False

        # snip85
        self.snip85_state = 0
        self.snip85_trigger = False

        # snip86
        self.snip86_state = 0
        self.snip86_trigger = False

        # snip88
        self.snip88_state = 0
        self.snip88_trigger = False

        # snip89
        self.snip89_state = 0
        self.snip89_trigger = False

        # snip90
        self.snip90_trigger = False

        # snip91
        self.snip91_trigger = False

        # snip94
        self.snip94_state = 0
        self.snip94_trigger = False

        # snip95
        self.snip95_state = 0
        self.snip95_trigger = False

        # snip97
        self.snip97_state = 0
        self.snip97_trigger = False

        # snip99
        self.snip99_state = 0
        self.snip99_trigger = False

        # snip100
        self.snip100_state = 0
        self.snip100_trigger = False

        # snip102
        self.snip102_state = 0
        self.snip102_trigger = False

        # snip104
        self.snip104_state = 0
        self.snip104_trigger = False

        # snip105
        self.snip105_state = 0
        self.snip105_trigger = False

        # snip107
        self.snip107_state = 0
        self.snip107_trigger = False

        # snip108
        self.snip108_state = 0
        self.snip108_trigger = False

        # snip109
        self.snip109_state = 0
        self.snip109_trigger = False

        # snip111
        self.snip111_state = 0
        self.snip111_trigger = False

        # snip112
        self.snip112_state = 0
        self.snip112_trigger = False

        # snip114
        self.snip114_state = 0
        self.snip114_trigger = False

        # snip119
        self.snip119_state = 0
        self.snip119_trigger = False

        # snip120
        self.snip120_state = 0
        self.snip120_trigger = False

        # snip121
        self.snip121_state = 0
        self.snip121_trigger = False

        # snip123
        self.snip123_state = 0
        self.snip123_trigger = False

        # snip125
        self.snip125_state = 0
        self.snip125_trigger = False

        # snip126
        self.snip126_state = 0
        self.snip126_trigger_93 = False
        self.snip126_trigger_50 = False

        # snip127
        self.snip127_state = 0
        self.snip127_trigger = False

        # snip128
        self.snip128_state = 0
        self.snip128_trigger = False

        # snip129
        self.snip129_state = 0
        self.snip129_trigger = False

        # snip130
        self.snip130_state = 0
        self.snip130_trigger = False

        # snip131
        self.snip131_trigger = False

        # snip132
        self.snip132_trigger = False

        # snip133
        self.snip133_trigger = False

        # snip134
        self.snip134_trigger = False

        # snip135
        self.snip135_state = 0
        self.snip135_trigger = False

        # snip136
        self.snip136_state = 0
        self.snip136_trigger = False

        # snip137
        self.snip137_trigger = False

        # snip138
        self.snip138_state = 0
        self.snip138_trigger = False

        # snip139
        self.snip139_state = 0
        self.snip139_trigger = False

        # snip140
        self.snip140_state = 0
        self.snip140_trigger = False

        # snip141
        self.snip141_state = 0
        self.snip141_trigger = False

        # snip142
        self.snip142_state = 0
        self.snip142_trigger = False

        # snip143
        self.snip143_state = 0
        self.snip143_trigger = False

        # background fill swap (kept, not plotted)
        self.bg_state = 0


# ==================== MERGED INDICATOR STATE ====================
class MergedState:
    """
    Rolling windows for the indicator math. Identical to indicators.py:
    ta.sma, ta.stdev (ddof=0), ta.rsi, ta.highest / ta.lowest, OLS LR.
    """
    def __init__(self, cfg):
        self.ma_fast = cfg["ma_fast"]
        self.ma_slow = cfg["ma_slow"]
        self.bb_length = cfg["bb_length"]
        self.bb_mult = cfg["bb_mult"]
        self.lr_length = cfg["lr_length"]
        self.lr_norm_len = cfg["lr_norm_len"]
        self.bar_index_base = cfg.get("bar_index_base", 0)
        self.inv_lookback = cfg["inv_lookback"]

        self.s002 = cfg["short_002"]
        self.s007 = cfg["short_007"]
        self.s14  = cfg["short_14"]
        self.s50  = cfg["short_50"]
        self.l100 = cfg["long_100"]
        self.l200 = cfg["long_200"]
        self.l350 = cfg["long_350"]
        self.l500 = cfg["long_500"]
        self.fiblength = cfg["fiblength"]

        self.close_window_fast: deque = deque(maxlen=self.ma_fast)
        self.close_window_slow: deque = deque(maxlen=self.ma_slow)
        self.close_window_bb:   deque = deque(maxlen=self.bb_length)
        self.high_window_inv:   deque = deque(maxlen=self.inv_lookback)
        self.low_window_inv:    deque = deque(maxlen=self.inv_lookback)

        self.lr_window: deque = deque(maxlen=self.lr_length)
        self.lr_series_window: deque = deque(maxlen=self.lr_norm_len)
        self.lr_series_sum: float = 0.0
        self.lr_series_sumsq: float = 0.0

        self.last_points: Optional[float] = None
        self.last_lr: Optional[float] = None
        self.last_cond1: bool = False
        self.last_cond2: bool = False
        self._prev_points_for_change: Optional[float] = None

        self.s002_win: deque = deque(maxlen=self.s002)
        self.s007_win: deque = deque(maxlen=self.s007)
        self.s14_win:  deque = deque(maxlen=self.s14)
        self.s50_win:  deque = deque(maxlen=self.s50)
        self.l100_win: deque = deque(maxlen=self.l100)
        self.l200_win: deque = deque(maxlen=self.l200)
        self.l350_win: deque = deque(maxlen=self.l350)
        self.l500_win: deque = deque(maxlen=self.l500)

        self.fib_win: deque = deque(maxlen=self.fiblength)

        self.bar_index: int = 0

    def _ols_slope_intercept_newest_first(self, y_newest_first):
        n = len(y_newest_first)
        if n < 2:
            return float("nan"), float("nan")
        sum_x = 0.0
        sum_y = 0.0
        sum_xy = 0.0
        sum_x_squared = 0.0
        for i in range(n):
            yi = y_newest_first[i]
            sum_x += i
            sum_y += yi
            sum_xy += i * yi
            sum_x_squared += i * i
        denom = n * sum_x_squared - sum_x * sum_x
        if denom == 0:
            return float("nan"), float("nan")
        m = (n * sum_xy - sum_x * sum_y) / denom
        c = (sum_y - m * sum_x) / n
        return m, c

    @staticmethod
    def _pop_mean_std(window: deque, running_sum: float, running_sumsq: float):
        n = len(window)
        if n == 0:
            return float("nan"), float("nan")
        mean = running_sum / n
        var = running_sumsq / n - mean * mean
        if var < 0:
            var = 0.0
        return mean, math.sqrt(var)

    @staticmethod
    def _sma(window: deque) -> Optional[float]:
        if len(window) < window.maxlen:
            return None
        return sum(window) / window.maxlen

    def step(self, high: float, low: float, close: float):
        self.bar_index += 1

        self.close_window_fast.append(close)
        self.close_window_slow.append(close)
        ma_fast = (sum(self.close_window_fast) / self.ma_fast
                   if len(self.close_window_fast) == self.ma_fast else None)
        ma_slow = (sum(self.close_window_slow) / self.ma_slow
                   if len(self.close_window_slow) == self.ma_slow else None)

        self.close_window_bb.append(close)
        if len(self.close_window_bb) == self.bb_length:
            bb_window = list(self.close_window_bb)
            bb_n = self.bb_length
            bb_mean = sum(bb_window) / bb_n
            bb_var = sum((x - bb_mean) ** 2 for x in bb_window) / bb_n
            bb_std = math.sqrt(bb_var)
            bbbasis = bb_mean
            bbdev = self.bb_mult * bb_std
            bbupper = bbbasis + bbdev
            bblower = bbbasis - bbdev
        else:
            bbbasis = bbdev = bbupper = bblower = None

        self.lr_window.append(close)
        lr_raw = None
        if len(self.lr_window) == self.lr_length:
            window_newest_first = list(self.lr_window)[::-1]
            m, c = self._ols_slope_intercept_newest_first(window_newest_first)
            if not (math.isnan(m) or math.isnan(c)):
                bar_index_t = self.bar_index_base + (self.bar_index - 1)
                lr_raw = -(m * bar_index_t + c)

        lr = None
        if lr_raw is not None:
            if len(self.lr_series_window) == self.lr_norm_len:
                old = self.lr_series_window[0]
                self.lr_series_sum -= old
                self.lr_series_sumsq -= old * old
            self.lr_series_window.append(lr_raw)
            self.lr_series_sum += lr_raw
            self.lr_series_sumsq += lr_raw * lr_raw
            if len(self.lr_series_window) == self.lr_norm_len:
                mean, std = self._pop_mean_std(
                    self.lr_series_window,
                    self.lr_series_sum,
                    self.lr_series_sumsq,
                )
                if not math.isnan(mean) and not math.isnan(std) and std != 0:
                    lr = (lr_raw - mean) / std

        cond1 = False
        cond2 = False
        if self.last_lr is not None and lr is not None:
            cond1 = (self.last_lr >= 0.0) and (lr < 0.0)
            cond2 = (self.last_lr <= 0.0) and (lr > 0.0)
        if lr is not None:
            self.last_lr = lr

        self.high_window_inv.append(high)
        self.low_window_inv.append(low)
        rolling_high = None
        rolling_low = None
        if len(self.high_window_inv) == self.inv_lookback:
            rolling_high = max(self.high_window_inv)
            rolling_low = min(self.low_window_inv)

        points = self.last_points
        if cond1 and rolling_high is not None:
            points = rolling_high
        elif cond2 and rolling_low is not None:
            points = rolling_low
        self.last_points = points

        col_up_active = (points is not None) and (lr is not None) and (lr > 0)
        col_dn_active = (points is not None) and (lr is not None) and (lr <= 0)

        col_up = None
        col_dn = None
        if points is not None and self._prev_points_for_change is not None \
                and self._prev_points_for_change == points:
            if col_up_active:
                col_up = points
            if col_dn_active:
                col_dn = points
        self._prev_points_for_change = points

        self.s002_win.append(close)
        self.s007_win.append(close)
        self.s14_win.append(close)
        self.s50_win.append(close)
        self.l100_win.append(close)
        self.l200_win.append(close)
        self.l350_win.append(close)
        self.l500_win.append(close)

        short_002 = self._sma(self.s002_win)
        short_007 = self._sma(self.s007_win)
        short_14  = self._sma(self.s14_win)
        short_50  = self._sma(self.s50_win)
        long_100  = self._sma(self.l100_win)
        long_200  = self._sma(self.l200_win)
        long_350  = self._sma(self.l350_win)
        long_500  = self._sma(self.l500_win)

        self.fib_win.append(close)
        if len(self.fib_win) == self.fiblength:
            maxr = max(self.fib_win)
            minr = min(self.fib_win)
            ranr = maxr - minr
            fibo_1     = maxr
            fibo_0_764 = maxr - 0.236 * ranr
            fibo_0_618 = maxr - 0.382 * ranr
            fibo_0_5   = maxr - 0.50 * ranr
            fibo_0_382 = minr + 0.382 * ranr
            fibo_0_236 = minr + 0.236 * ranr
            fibo_0     = minr
        else:
            maxr = minr = ranr = None
            fibo_1 = fibo_0_764 = fibo_0_618 = fibo_0_5 = None
            fibo_0_382 = fibo_0_236 = fibo_0 = None

        return {
            "ma_350": ma_fast,
            "ma_500": ma_slow,
            "bbbasis": bbbasis,
            "bbdev": bbdev,
            "bbupper": bbupper,
            "bblower": bblower,
            "lr": lr,
            "cond1": cond1,
            "cond2": cond2,
            "points": points,
            "col_up": col_up,
            "col_dn": col_dn,
            "col_up_active": col_up_active,
            "col_dn_active": col_dn_active,
            "short_002": short_002,
            "short_007": short_007,
            "short_14":  short_14,
            "short_50":  short_50,
            "long_100":  long_100,
            "long_200":  long_200,
            "long_350":  long_350,
            "long_500":  long_500,
            "maxr": maxr,
            "minr": minr,
            "ranr": ranr,
            "fibo_1":     fibo_1,
            "fibo_0_764": fibo_0_764,
            "fibo_0_618": fibo_0_618,
            "fibo_0_5":   fibo_0_5,
            "fibo_0_382": fibo_0_382,
            "fibo_0_236": fibo_0_236,
            "fibo_0":     fibo_0,
        }

    def to_dict(self):
        return {
            "ma_fast": self.ma_fast,
            "ma_slow": self.ma_slow,
            "bb_length": self.bb_length,
            "bb_mult": self.bb_mult,
            "lr_length": self.lr_length,
            "lr_norm_len": self.lr_norm_len,
            "bar_index_base": self.bar_index_base,
            "inv_lookback": self.inv_lookback,
            "short_002": self.s002,
            "short_007": self.s007,
            "short_14":  self.s14,
            "short_50":  self.s50,
            "long_100":  self.l100,
            "long_200":  self.l200,
            "long_350":  self.l350,
            "long_500":  self.l500,
            "fiblength": self.fiblength,
            "close_window_fast": list(self.close_window_fast),
            "close_window_slow": list(self.close_window_slow),
            "close_window_bb":   list(self.close_window_bb),
            "high_window_inv":   list(self.high_window_inv),
            "low_window_inv":    list(self.low_window_inv),
            "lr_window":         list(self.lr_window),
            "lr_series_window":  list(self.lr_series_window),
            "lr_series_sum":     self.lr_series_sum,
            "lr_series_sumsq":   self.lr_series_sumsq,
            "last_points":       self.last_points,
            "last_lr":           self.last_lr,
            "last_cond1":        self.last_cond1,
            "last_cond2":        self.last_cond2,
            "prev_points_for_change": self._prev_points_for_change,
            "bar_index":         self.bar_index,
            "s002_win": list(self.s002_win),
            "s007_win": list(self.s007_win),
            "s14_win":  list(self.s14_win),
            "s50_win":  list(self.s50_win),
            "l100_win": list(self.l100_win),
            "l200_win": list(self.l200_win),
            "l350_win": list(self.l350_win),
            "l500_win": list(self.l500_win),
            "fib_win":  list(self.fib_win),
        }

    @classmethod
    def from_dict(cls, d, cfg):
        s = cls(cfg)
        s.close_window_fast = deque(d.get("close_window_fast") or [],
                                   maxlen=s.ma_fast)
        s.close_window_slow = deque(d.get("close_window_slow") or [],
                                   maxlen=s.ma_slow)
        s.close_window_bb   = deque(d.get("close_window_bb") or [],
                                   maxlen=s.bb_length)
        s.high_window_inv   = deque(d.get("high_window_inv") or [],
                                   maxlen=s.inv_lookback)
        s.low_window_inv    = deque(d.get("low_window_inv") or [],
                                   maxlen=s.inv_lookback)
        s.lr_window         = deque(d.get("lr_window") or [],
                                   maxlen=s.lr_length)
        s.lr_series_window  = deque(d.get("lr_series_window") or [],
                                   maxlen=s.lr_norm_len)
        s.lr_series_sum     = float(d.get("lr_series_sum") or 0.0)
        s.lr_series_sumsq   = float(d.get("lr_series_sumsq") or 0.0)
        s.last_points       = d.get("last_points")
        s.last_lr           = d.get("last_lr")
        s.last_cond1        = bool(d.get("last_cond1") or False)
        s.last_cond2        = bool(d.get("last_cond2") or False)
        s._prev_points_for_change = d.get("prev_points_for_change")
        s.bar_index         = int(d.get("bar_index") or 0)
        s.s002_win = deque(d.get("s002_win") or [], maxlen=s.s002)
        s.s007_win = deque(d.get("s007_win") or [], maxlen=s.s007)
        s.s14_win  = deque(d.get("s14_win")  or [], maxlen=s.s14)
        s.s50_win  = deque(d.get("s50_win")  or [], maxlen=s.s50)
        s.l100_win = deque(d.get("l100_win") or [], maxlen=s.l100)
        s.l200_win = deque(d.get("l200_win") or [], maxlen=s.l200)
        s.l350_win = deque(d.get("l350_win") or [], maxlen=s.l350)
        s.l500_win = deque(d.get("l500_win") or [], maxlen=s.l500)
        s.fib_win  = deque(d.get("fib_win")  or [], maxlen=s.fiblength)
        return s


# ==================== SNIPPET EVALUATOR ====================
class SnippetEvaluator:
    """
    Evaluates all snippets for one bar in exact Pine source order.

    EVERY COMPARISON BELOW IS TAKEN FROM THE RUNNABLE CODE, NOT COMMENTS.

    All numeric comparisons go through _gt/_lt/_gte/_lte so that a `None`
    operand (still warming up) yields False — exactly like Pine's `na`.
    """

    def __init__(self, cfg):
        self.cfg = cfg

    # ---- null-safe comparison helpers (Pine na semantics) ----
    @staticmethod
    def _gt(a, b) -> bool:
        return (a is not None) and (b is not None) and (a > b)

    @staticmethod
    def _lt(a, b) -> bool:
        return (a is not None) and (b is not None) and (a < b)

    @staticmethod
    def _gte(a, b) -> bool:
        return (a is not None) and (b is not None) and (a >= b)

    @staticmethod
    def _lte(a, b) -> bool:
        return (a is not None) and (b is not None) and (a <= b)

    def evaluate(self, st: SnippetState, ind: dict) -> dict:
        c = ind["close"]
        h = ind["high"]
        l = ind["low"]
        rsi = ind.get("rsi")
        rsi_ma50 = ind.get("rsi_ma50")
        bbbasis = ind.get("bbbasis")
        bbupper = ind.get("bbupper")
        bblower = ind.get("bblower")
        long350 = ind.get("long_350")
        long500 = ind.get("long_500")
        short_002 = ind.get("short_002")
        short_007 = ind.get("short_007")
        short_14  = ind.get("short_14")
        short_50  = ind.get("short_50")
        long_100  = ind.get("long_100")
        long_200  = ind.get("long_200")
        fibo_0_764 = ind.get("fibo_0_764")
        lr = ind.get("lr")
        points = ind.get("points")
        linear_regression = lr  # Pine: `linear_regression`

        # Reset ALL per-bar triggers (Pine's `:= false`).
        for attr in (
            "snip15_trigger","snip16_trigger","snip16_trigger2",
            "snip34_trigger","snip47_trigger_path1","snip47_trigger_path2",
            "snip61_trigger1","snip61_trigger2","snip61_trigger_phone",
            "snip61_trigger_mobile","snip63_trigger","snip64_trigger",
            "snip66_trigger_anchor","snip66_trigger_hook","snip67_trigger",
            "snip68_trigger_mic","snip68_trigger_comet","snip69_trigger",
            "snip70_trigger","snip72_trigger","snip73_trigger","snip74_trigger",
            "snip76_trigger","snip78_trigger","snip79_trigger","snip82_trigger",
            "snip83_trigger","snip84_trigger","snip85_trigger","snip86_trigger",
            "snip88_trigger","snip89_trigger","snip90_trigger","snip91_trigger",
            "snip94_trigger","snip95_trigger","snip97_trigger","snip99_trigger",
            "snip100_trigger","snip102_trigger","snip104_trigger",
            "snip105_trigger","snip107_trigger","snip108_trigger",
            "snip109_trigger","snip111_trigger","snip112_trigger",
            "snip114_trigger","snip119_trigger","snip120_trigger",
            "snip121_trigger","snip123_trigger","snip125_trigger",
            "snip126_trigger_93","snip126_trigger_50","snip127_trigger",
            "snip128_trigger","snip129_trigger","snip130_trigger",
            "snip131_trigger","snip132_trigger","snip133_trigger",
            "snip134_trigger","snip135_trigger","snip136_trigger",
            "snip137_trigger","snip138_trigger","snip139_trigger",
            "snip140_trigger","snip141_trigger","snip142_trigger",
            "snip143_trigger",
        ):
            setattr(st, attr, False)

        # ---------- base conditions ----------
        st.snippet1_condition = (
            self._gte(rsi, 93)
            and self._gt(bbbasis, long500)
        )
        st.snippet6_condition = (
            self._gt(linear_regression, 0)
            and self._lt(points, bbbasis)
            and self._lt(points, long350)
            and self._lt(points, long500)
            and self._lt(bbbasis, long350)
            and self._gt(long500, bbupper)
            and self._gt(c, long500)
        )
        st.snippet7_condition = (
            self._gt(linear_regression, 0)
            and self._lt(rsi_ma50, 60)
            and self._gt(c, long500)
            and self._gt(bbbasis, long500)
        )
        st.snippet9_condition = (
            self._gt(long350, bbupper)
            and self._gt(long500, bbupper)
            and self._gt(long500, long350)
            and self._lt(bbbasis, bbupper)
            and self._gt(bbbasis, points)
            and self._gt(points, bblower)
            and self._gt(c, long350)
            and self._gt(linear_regression, 0)
        )

        # ---------- snippet 15 ----------
        if not st.snip15_armed and st.snippet7_condition:
            st.snip15_armed = True
            st.snip15_trigger = False
        elif st.snip15_armed and self._lt(long350, long500):
            st.snip15_armed = False
            st.snip15_trigger = True

        # ---------- snippet 16 ----------
        snip15_triggered = st.snip15_trigger
        if not st.snip16_armed and snip15_triggered:
            st.snip16_armed = True
            st.snip16_trigger = False
        elif (st.snip16_armed
              and self._gt(c, bbupper)
              and self._lt(bbbasis, long500)
              and self._lt(long350, long500)
              and self._lt(linear_regression, 0)
              and self._lt(points, long500)
              and self._lt(points, long350)
              and self._lt(points, bbbasis)):
            st.snip16_armed = False
            st.snip16_trigger = True
            st.snip16_waiting_rsi = True
            st.snip16_rsi_high = False
        if st.snip16_waiting_rsi and not st.snip16_rsi_high:
            if self._gt(rsi_ma50, 65):
                st.snip16_rsi_high = True
        elif st.snip16_waiting_rsi and st.snip16_rsi_high:
            if self._lt(rsi_ma50, 50):
                st.snip16_waiting_rsi = False
                st.snip16_rsi_high = False
                st.snip16_trigger2 = True

        # ---------- snippet 34 ----------
        snip9_triggered = st.snippet9_condition
        if st.snip34_state == 0:
            if snip9_triggered:
                st.snip34_state = 1
        elif st.snip34_state == 1:
            if (self._gt(long350, long500)
                    and self._gt(c, long500)
                    and self._gt(long350, bblower)):
                st.snip34_state = 2
        elif st.snip34_state == 2:
            if self._gt(rsi_ma50, 65):
                st.snip34_state = 3
        elif st.snip34_state == 3:
            if self._lt(rsi_ma50, 50):
                st.snip34_state = 4
        elif st.snip34_state == 4:
            if self._gt(rsi_ma50, 63):
                st.snip34_state = 0
                st.snip34_trigger = True

        # ---------- snippet 47 ----------
        snip6_triggered = st.snippet6_condition
        if not st.snip47_armed and snip6_triggered:
            st.snip47_armed = True
            st.snip47_captured_bbupper = bbupper
            st.snip47_captured_bblower = bblower
            st.snip47_trigger_path1 = False
            st.snip47_trigger_path2 = False
        elif (st.snip47_armed
              and st.snip47_captured_bbupper is not None
              and self._gt(long350, st.snip47_captured_bbupper)
              and self._lt(long350, long500)
              and self._gt(linear_regression, 0)
              and self._lt(points, long500)
              and self._gt(bbbasis, long350)
              and self._gt(c, long500)
              and self._gt(c, bbupper)):
            st.snip47_armed = False
            st.snip47_captured_bbupper = None
            st.snip47_captured_bblower = None
            st.snip47_trigger_path1 = True
            st.snip47_trigger_path2 = False
        elif (st.snip47_armed
              and st.snip47_captured_bblower is not None
              and self._lt(long350, st.snip47_captured_bblower)):
            st.snip47_armed = False
            st.snip47_captured_bbupper = None
            st.snip47_captured_bblower = None
            st.snip47_trigger_path1 = False
            st.snip47_trigger_path2 = True

        # ---------- snippet 61 ----------
        snippet61_condition = (
            self._lt(linear_regression, 0)
            and self._gt(points, bbupper)
            and self._gt(long350, bbbasis)
            and self._lt(c, long500)
        )
        if not st.snip61_armed and snippet61_condition:
            st.snip61_armed = True
            st.snip61_trigger1 = True
            if self._gt(bbbasis, long500):
                st.snip61_trigger_phone = True
            elif self._lt(bbbasis, long500):
                st.snip61_trigger_mobile = True
        elif (st.snip61_armed
              and self._lt(linear_regression, 0)
              and self._lt(points, bbupper)):
            st.snip61_armed = False
            st.snip61_trigger2 = True

        # ---------- snippet 63 ----------
        snip61_triggered = st.snip61_trigger1
        if st.snip63_state == 0:
            if snip61_triggered:
                st.snip63_state = 1
        elif st.snip63_state == 1:
            if (self._gt(long350, long500)
                    and self._gt(bbbasis, long350)
                    and self._gt(c, bbupper)):
                st.snip63_state = 2
        elif st.snip63_state == 2:
            if (self._lt(bbbasis, long350) and self._gt(c, long350)):
                st.snip63_state = 0
                st.snip63_trigger = True

        # ---------- snippet 64 ----------
        snip64_step1 = (
            self._gt(short_002, fibo_0_764)
            and self._gt(short_007, fibo_0_764)
            and self._gt(short_14,  fibo_0_764)
            and self._gt(short_50,  fibo_0_764)
            and self._gt(long_100,  fibo_0_764)
            and self._lt(long_200,  fibo_0_764)
            and self._gt(long350,   fibo_0_764)
            and self._gt(long500,   fibo_0_764)
        )
        if not st.snip64_armed and snip64_step1:
            st.snip64_armed = True
            st.snip64_trigger = False
        elif st.snip64_armed and self._gt(long_200, long500):
            st.snip64_armed = False
            st.snip64_trigger = True

        # ---------- snippet 66 ----------
        if st.snip63_trigger and self._gt(long500, bbupper):
            st.snip66_trigger_anchor = True
        elif st.snip63_trigger and self._lt(long500, bbupper):
            st.snip66_trigger_hook = True

        # ---------- snippet 67 ----------
        if st.snip67_state == 0:
            if st.snip66_trigger_anchor:
                st.snip67_state = 1
        elif st.snip67_state == 1:
            if self._lt(c, bblower):
                st.snip67_state = 2
        elif st.snip67_state == 2:
            if self._gt(c, bbupper):
                st.snip67_state = 3
        elif st.snip67_state == 3:
            if self._lt(long350, long500):
                st.snip67_state = 4
        elif st.snip67_state == 4:
            if self._gt(long350, long500):
                st.snip67_state = 0
                st.snip67_trigger = True

        # ---------- snippet 68 ----------
        if st.snip66_trigger_hook and self._gt(bbbasis, long500):
            st.snip68_trigger_mic = True
        elif st.snip66_trigger_hook and self._lt(bbbasis, long500):
            st.snip68_trigger_comet = True

        # ---------- snippet 69 (CODE: rsi_ma50 < 100) ----------
        snip69_step1 = (
            self._gt(linear_regression, 0)
            and self._lt(points, bblower)
            and self._gt(c, bbupper)
        )
        if st.snip69_state == 0:
            if snip69_step1:
                st.snip69_state = 1
        elif st.snip69_state == 1:
            if (self._lt(long350, long500)
                    and self._lt(bbbasis, long500)
                    and self._lt(c, long500)):
                st.snip69_state = 2
        elif st.snip69_state == 2:
            if self._lt(rsi_ma50, 100):  # CODE
                st.snip69_state = 0
                st.snip69_trigger = True

        # ---------- snippet 70 ----------
        snip70_step1 = (
            self._lt(long500, bblower) and self._gt(c, bbupper)
        )
        if st.snip70_state == 0:
            if snip70_step1:
                st.snip70_state = 1
        elif st.snip70_state == 1:
            if (self._lt(long350, long500)
                    and self._lt(c, long500)
                    and self._lt(bbbasis, long500)):
                st.snip70_state = 2
        elif st.snip70_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip70_state = 0
                st.snip70_trigger = True

        # ---------- snippet 72 ----------
        snip72_condition = (
            st.snip69_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bblower)
            and self._gt(c, bblower)
        )
        if st.snip72_state == 0:
            if snip72_condition:
                st.snip72_state = 1
        elif st.snip72_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip72_state = 2
        elif st.snip72_state == 2:
            if self._gt(rsi, 93):
                st.snip72_state = 3
        elif st.snip72_state == 3:
            st.snip72_state = 0
            st.snip72_trigger = True

        # ---------- snippet 73 ----------
        snip73_step1 = (
            st.snip69_trigger
            and self._gt(linear_regression, 0)
            and self._gt(points, bblower)
        )
        if st.snip73_state == 0:
            if snip73_step1:
                st.snip73_state = 1
        elif st.snip73_state == 1:
            if (self._gt(bbbasis, long500)
                    and self._gt(long350, long500)
                    and self._gt(c, bbupper)):
                st.snip73_state = 2
        elif st.snip73_state == 2:
            st.snip73_state = 0
            st.snip73_trigger = True

        # ---------- snippet 74 ----------
        snip74_condition = (
            st.snip69_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if snip74_condition:
            st.snip74_trigger = True

        # ---------- snippet 76 ----------
        snip76_step1 = (
            st.snip70_trigger
            and self._lt(linear_regression, 0)
            and self._gt(points, long500)
        )
        if st.snip76_state == 0:
            if snip76_step1:
                st.snip76_state = 1
        elif st.snip76_state == 1:
            if self._gt(bbbasis, long500) and self._gt(c, bbupper):
                st.snip76_state = 2
        elif st.snip76_state == 2:
            st.snip76_state = 0
            st.snip76_trigger = True

        # ---------- snippet 78 ----------
        snip78_condition = (
            st.snip70_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if snip78_condition:
            st.snip78_trigger = True

        # ---------- snippet 79 (CODE: rsi_ma50 > 0) ----------
        if st.snip79_state == 0:
            if st.snip78_trigger:
                st.snip79_state = 1
        elif st.snip79_state == 1:
            if self._gt(bbbasis, long500) and self._gt(c, bbupper):
                st.snip79_state = 2
        elif st.snip79_state == 2:
            if self._gt(rsi_ma50, 0):  # CODE
                st.snip79_state = 0
                st.snip79_trigger = True

        # ---------- snippet 82 ----------
        snip82_step1 = (
            self._gt(long350, bbupper)
            and self._lt(long500, bbupper)
            and self._lt(c, bblower)
        )
        if st.snip82_state == 0:
            if snip82_step1:
                st.snip82_state = 1
        elif st.snip82_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip82_state = 2
        elif st.snip82_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip82_state = 3
        elif st.snip82_state == 3:
            st.snip82_state = 0
            st.snip82_trigger = True

        # ---------- snippet 83 ----------
        if st.snip83_state == 0:
            if self._gt(rsi_ma50, 70) and self._gt(bbbasis, long500):
                st.snip83_state = 1
        elif st.snip83_state == 1:
            if self._lt(rsi_ma50, 35) and self._gt(bbbasis, long500):
                st.snip83_state = 0
                st.snip83_trigger = True

        # ---------- snippet 84 ----------
        snip84_step1 = (
            self._lt(long350, bbupper)
            and self._gt(long500, bbupper)
            and self._lt(c, bblower)
        )
        if st.snip84_state == 0:
            if snip84_step1:
                st.snip84_state = 1
        elif st.snip84_state == 1:
            if (self._gt(bbbasis, long500)
                    and self._gt(long350, long500)
                    and self._gt(c, bbupper)):
                st.snip84_state = 2
        elif st.snip84_state == 2:
            if self._gt(rsi_ma50, 65):
                st.snip84_state = 0
                st.snip84_trigger = True

        # ---------- snippet 85 ----------
        snip85_step1 = self._lt(long500, bblower) and self._gt(c, bbupper)
        if st.snip85_state == 0:
            if snip85_step1:
                st.snip85_state = 1
        elif st.snip85_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip85_state = 2
        elif st.snip85_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip85_state = 3
        elif st.snip85_state == 3:
            if self._gt(rsi_ma50, 53):
                st.snip85_state = 4
        elif st.snip85_state == 4:
            if self._lt(rsi_ma50, 50):
                st.snip85_state = 0
                st.snip85_trigger = True

        # ---------- snippet 86 ----------
        snip86_step1 = self._lt(long500, bblower) and self._gt(c, bblower)
        if st.snip86_state == 0:
            if snip86_step1:
                st.snip86_state = 1
        elif st.snip86_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip86_state = 2
        elif st.snip86_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip86_state = 3
        elif st.snip86_state == 3:
            if self._gt(rsi_ma50, 53):
                st.snip86_state = 4
        elif st.snip86_state == 4:
            if self._lt(rsi_ma50, 50):
                st.snip86_state = 0
                st.snip86_trigger = True

        # ---------- snippet 88 ----------
        snip88_step1 = (
            st.snip85_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip88_state == 0:
            if snip88_step1:
                st.snip88_state = 1
        elif st.snip88_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip88_state = 2
        elif st.snip88_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip88_state = 0
                st.snip88_trigger = True

        # ---------- snippet 89 ----------
        snip89_step1 = (
            st.snip86_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip89_state == 0:
            if snip89_step1:
                st.snip89_state = 1
        elif st.snip89_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip89_state = 2
        elif st.snip89_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip89_state = 0
                st.snip89_trigger = True

        # ---------- snippet 90 ----------
        snip90_condition = (
            st.snip86_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if snip90_condition:
            st.snip90_trigger = True

        # ---------- snippet 91 ----------
        snip91_condition = (
            st.snip85_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if snip91_condition:
            st.snip91_trigger = True

        # ---------- snippet 94 ----------
        if st.snip94_state == 0:
            if st.snip61_trigger_phone:
                st.snip94_state = 1
        elif st.snip94_state == 1:
            if self._gt(rsi_ma50, 65) and self._gt(long350, long500):
                st.snip94_state = 0
                st.snip94_trigger = True

        # ---------- snippet 95 (CODE: <37, >1, <100) ----------
        if st.snip95_state == 0:
            if st.snip61_trigger_phone:
                st.snip95_state = 1
        elif st.snip95_state == 1:
            if self._lt(rsi_ma50, 37):  # CODE
                st.snip95_state = 2
        elif st.snip95_state == 2:
            if self._gt(rsi_ma50, 1):  # CODE
                st.snip95_state = 3
        elif st.snip95_state == 3:
            if self._lt(rsi_ma50, 100):  # CODE
                st.snip95_state = 0
                st.snip95_trigger = True

        # ---------- snippet 97 (CODE: >74, <37) ----------
        if st.snip97_state == 0:
            if self._gt(rsi_ma50, 74):  # CODE
                st.snip97_state = 1
        elif st.snip97_state == 1:
            if self._lt(rsi_ma50, 37):  # CODE
                st.snip97_state = 2
        elif st.snip97_state == 2:
            st.snip97_state = 0
            st.snip97_trigger = True

        # ---------- snippet 99 ----------
        snip99_step1 = (
            st.snip79_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip99_state == 0:
            if snip99_step1:
                st.snip99_state = 1
        elif st.snip99_state == 1:
            if self._lt(rsi_ma50, 36):
                st.snip99_state = 2
        elif st.snip99_state == 2:
            st.snip99_state = 0
            st.snip99_trigger = True

        # ---------- snippet 100 ----------
        snip100_step1 = (
            st.snip79_trigger
            and self._gt(linear_regression, 0)
            and self._gt(points, long500)
        )
        if st.snip100_state == 0:
            if snip100_step1:
                st.snip100_state = 1
        elif st.snip100_state == 1:
            if self._lt(rsi_ma50, 36):
                st.snip100_state = 2
        elif st.snip100_state == 2:
            st.snip100_state = 0
            st.snip100_trigger = True

        # ---------- snippet 102 ----------
        snip102_step1 = (
            st.snip84_trigger
            and self._lt(linear_regression, 0)
            and self._gt(points, long500)
        )
        if st.snip102_state == 0:
            if snip102_step1:
                st.snip102_state = 1
        elif st.snip102_state == 1:
            st.snip102_state = 0
            st.snip102_trigger = True

        # ---------- snippet 104 ----------
        snip104_step1 = (
            st.snip83_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip104_state == 0:
            if snip104_step1:
                st.snip104_state = 1
        elif st.snip104_state == 1:
            if self._gt(rsi_ma50, 66):
                st.snip104_state = 2
        elif st.snip104_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip104_state = 3
        elif st.snip104_state == 3:
            st.snip104_state = 0
            st.snip104_trigger = True

        # ---------- snippet 105 (CODE: long350 > long500) ----------
        snip105_step1 = (
            st.snip83_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
            and self._lt(long500, bblower)
        )
        if st.snip105_state == 0:
            if snip105_step1:
                st.snip105_state = 1
        elif st.snip105_state == 1:
            if self._gt(rsi_ma50, 66):
                st.snip105_state = 2
        elif st.snip105_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip105_state = 3
        elif st.snip105_state == 3:
            if self._lt(bbbasis, long350) and self._gt(long350, long500):  # CODE: >
                st.snip105_state = 4
        elif st.snip105_state == 4:
            st.snip105_state = 0
            st.snip105_trigger = True

        # ---------- snippet 107 ----------
        snip107_step1 = (
            st.snip76_trigger
            and self._gt(linear_regression, 0)
            and self._gt(points, long500)
        )
        if st.snip107_state == 0:
            if snip107_step1:
                st.snip107_state = 1
        elif st.snip107_state == 1:
            if self._lt(long350, long500):
                st.snip107_state = 2
        elif st.snip107_state == 2:
            st.snip107_state = 0
            st.snip107_trigger = True

        # ---------- snippet 108 ----------
        snip108_step1 = (
            st.snip76_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, long500)
        )
        if st.snip108_state == 0:
            if snip108_step1:
                st.snip108_state = 1
        elif st.snip108_state == 1:
            st.snip108_state = 0
            st.snip108_trigger = True

        # ---------- snippet 109 ----------
        snip109_step1 = (
            st.snippet1_condition
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip109_state == 0:
            if snip109_step1:
                st.snip109_state = 1
        elif st.snip109_state == 1:
            if self._lt(rsi_ma50, 37):
                st.snip109_state = 2
        elif st.snip109_state == 2:
            st.snip109_state = 0
            st.snip109_trigger = True

        # ---------- snippet 111 ----------
        snip111_step1 = (
            st.snippet1_condition
            and self._gt(linear_regression, 0)
            and self._lt(points, long500)
        )
        if st.snip111_state == 0:
            if snip111_step1:
                st.snip111_state = 1
        elif st.snip111_state == 1:
            st.snip111_state = 0
            st.snip111_trigger = True

        # ---------- snippet 112 ----------
        snip112_step1 = (
            st.snip34_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, long500)
        )
        if st.snip112_state == 0:
            if snip112_step1:
                st.snip112_state = 1
        elif st.snip112_state == 1:
            st.snip112_state = 0
            st.snip112_trigger = True

        # ---------- snippet 114 ----------
        snip114_step1 = (
            self._gt(long350, bbupper)
            and self._gt(long500, bbupper)
            and self._gt(c, long500)
        )
        if st.snip114_state == 0:
            if snip114_step1:
                st.snip114_state = 1
        elif st.snip114_state == 1:
            st.snip114_state = 0
            st.snip114_trigger = True

        # ---------- snippet 119 ----------
        snip119_step1 = (
            st.snip67_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, long500)
        )
        if st.snip119_state == 0:
            if snip119_step1:
                st.snip119_state = 1
        elif st.snip119_state == 1:
            st.snip119_state = 0
            st.snip119_trigger = True

        # ---------- snippet 120 ----------
        snip120_step1 = (
            st.snip73_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip120_state == 0:
            if snip120_step1:
                st.snip120_state = 1
        elif st.snip120_state == 1:
            if self._lt(c, bblower):
                st.snip120_state = 2
        elif st.snip120_state == 2:
            if self._gt(rsi_ma50, 66):
                st.snip120_state = 3
        elif st.snip120_state == 3:
            if self._lt(rsi_ma50, 37):
                st.snip120_state = 4
        elif st.snip120_state == 4:
            if self._gt(rsi_ma50, 65):
                st.snip120_state = 5
        elif st.snip120_state == 5:
            if self._lt(rsi_ma50, 37):
                st.snip120_state = 6
        elif st.snip120_state == 6:
            st.snip120_state = 0
            st.snip120_trigger = True

        # ---------- snippet 121 ----------
        snip121_step1 = (
            st.snip73_trigger
            and self._gt(linear_regression, 0)
            and self._lt(points, bbupper)
        )
        if st.snip121_state == 0:
            if snip121_step1:
                st.snip121_state = 1
        elif st.snip121_state == 1:
            st.snip121_state = 0
            st.snip121_trigger = True

        # ---------- snippet 123 (CODE: <35, >67, <37) ----------
        snip123_step1 = (
            self._gt(linear_regression, 0)
            and self._gt(points, bbupper)
            and self._gt(c, long350)
            and self._gt(long350, long500)
        )
        if st.snip123_state == 0:
            if snip123_step1:
                st.snip123_state = 1
        elif st.snip123_state == 1:
            if self._lt(rsi_ma50, 35):  # CODE
                st.snip123_state = 2
        elif st.snip123_state == 2:
            if self._gt(rsi_ma50, 67):  # CODE
                st.snip123_state = 3
        elif st.snip123_state == 3:
            if self._lt(rsi_ma50, 37):  # CODE
                st.snip123_state = 4
        elif st.snip123_state == 4:
            st.snip123_state = 0
            st.snip123_trigger = True

        # ---------- snippet 125 (CODE: >66, <37) ----------
        snip125_step1 = self._lt(long350, bblower) and self._lt(long500, bblower)
        if st.snip125_state == 0:
            if snip125_step1:
                st.snip125_state = 1
        elif st.snip125_state == 1:
            if self._lt(rsi_ma50, 35):
                st.snip125_state = 2
        elif st.snip125_state == 2:
            if self._gt(rsi_ma50, 66):  # CODE
                st.snip125_state = 3
        elif st.snip125_state == 3:
            if self._lt(rsi_ma50, 37):  # CODE
                st.snip125_state = 4
        elif st.snip125_state == 4:
            st.snip125_state = 0
            st.snip125_trigger = True

        # ---------- snippet 126 (CODE: step3 is long500 < BBupper) ----------
        snip126_step1 = self._lt(long350, bblower) and self._lt(long500, bblower)
        if st.snip126_state == 0:
            if snip126_step1:
                st.snip126_state = 1
        elif st.snip126_state == 1:
            if self._gt(long500, bbupper):
                st.snip126_state = 2
        elif st.snip126_state == 2:
            if self._lt(long500, bbupper):  # CODE
                st.snip126_state = 3
        elif st.snip126_state == 3:
            if self._lt(long500, bblower):
                st.snip126_state = 0
                st.snip126_trigger_93 = True
            elif self._gt(long500, bbupper):
                st.snip126_state = 0
                st.snip126_trigger_50 = True

        # ---------- snippet 127 ----------
        snip127_step1 = self._gt(long350, bbupper) and self._gt(long500, bbupper)
        if st.snip127_state == 0:
            if snip127_step1:
                st.snip127_state = 1
        elif st.snip127_state == 1:
            if self._gt(long350, long500) and self._gt(rsi_ma50, 64):
                st.snip127_state = 2
        elif st.snip127_state == 2:
            st.snip127_state = 0
            st.snip127_trigger = True

        # ---------- snippet 128 ----------
        if st.snip128_state == 0:
            if st.snip126_trigger_50:
                st.snip128_state = 1
        elif st.snip128_state == 1:
            if self._lt(long500, bblower):
                st.snip128_state = 2
        elif st.snip128_state == 2:
            if self._gt(bbbasis, long500) and self._lt(long350, long500):
                st.snip128_state = 3
        elif st.snip128_state == 3:
            if self._lt(rsi_ma50, 35):
                st.snip128_state = 4
        elif st.snip128_state == 4:
            if self._gt(rsi_ma50, 60):
                st.snip128_state = 0
                st.snip128_trigger = True

        # ---------- snippet 129 ----------
        snip129_step1 = self._gt(long500, bbupper) and self._gt(long350, bbupper)
        if st.snip129_state == 0:
            if snip129_step1:
                st.snip129_state = 1
        elif st.snip129_state == 1:
            if self._lt(long500, bblower) and self._gt(c, bbupper):
                st.snip129_state = 0
                st.snip129_trigger = True

        # ---------- snippet 130 ----------
        if st.snip130_state == 0:
            if st.snip109_trigger:
                st.snip130_state = 1
        elif st.snip130_state == 1:
            if self._gt(rsi_ma50, 69):
                st.snip130_state = 2
        elif st.snip130_state == 2:
            if self._lt(rsi_ma50, 37):
                st.snip130_state = 3
        elif st.snip130_state == 3:
            st.snip130_state = 0
            st.snip130_trigger = True

        # ---------- snippet 131 ----------
        snip131_step1 = (
            st.snip89_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if snip131_step1:
            st.snip131_trigger = True

        # ---------- snippet 132 ----------
        snip132_step1 = (
            st.snip88_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if snip132_step1:
            st.snip132_trigger = True

        # ---------- snippet 133 ----------
        snip133_step1 = (
            st.snip91_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if snip133_step1:
            st.snip133_trigger = True

        # ---------- snippet 134 ----------
        snip134_step1 = (
            st.snip90_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if snip134_step1:
            st.snip134_trigger = True

        # ---------- snippet 135 ----------
        snip135_step1 = (
            st.snip95_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if st.snip135_state == 0:
            if snip135_step1:
                st.snip135_state = 1
        elif st.snip135_state == 1:
            if self._gt(rsi_ma50, 64):
                st.snip135_state = 2
        elif st.snip135_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip135_state = 3
        elif st.snip135_state == 3:
            st.snip135_state = 0
            st.snip135_trigger = True

        # ---------- snippet 136 ----------
        snip136_step1 = (
            st.snip104_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if st.snip136_state == 0:
            if snip136_step1:
                st.snip136_state = 1
        elif st.snip136_state == 1:
            if self._gt(rsi_ma50, 55):
                st.snip136_state = 2
        elif st.snip136_state == 2:
            if self._lt(rsi_ma50, 49):
                st.snip136_state = 3
        elif st.snip136_state == 3:
            st.snip136_state = 0
            st.snip136_trigger = True

        # ---------- snippet 137 ----------
        snip137_step1 = (
            st.snip105_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if snip137_step1:
            st.snip137_trigger = True

        # ---------- snippet 138 ----------
        snip138_step1 = (
            st.snip97_trigger
            and self._gt(bbbasis, long500)
            and self._gt(long350, long500)
        )
        if st.snip138_state == 0:
            if snip138_step1:
                st.snip138_state = 1
        elif st.snip138_state == 1:
            if self._gt(rsi_ma50, 65):
                st.snip138_state = 2
        elif st.snip138_state == 2:
            if self._lt(rsi_ma50, 37):
                st.snip138_state = 3
        elif st.snip138_state == 3:
            st.snip138_state = 0
            st.snip138_trigger = True

        # ---------- snippet 139 ----------
        if st.snip139_state == 0:
            if st.snip125_trigger:
                st.snip139_state = 1
        elif st.snip139_state == 1:
            if self._gt(rsi_ma50, 66):
                st.snip139_state = 2
        elif st.snip139_state == 2:
            if self._lt(rsi_ma50, 37):
                st.snip139_state = 3
        elif st.snip139_state == 3:
            st.snip139_state = 0
            st.snip139_trigger = True

        # ---------- snippet 140 ----------
        if st.snip140_state == 0:
            if st.snip123_trigger:
                st.snip140_state = 1
        elif st.snip140_state == 1:
            if self._gt(rsi_ma50, 66):
                st.snip140_state = 2
        elif st.snip140_state == 2:
            if self._lt(rsi_ma50, 37):
                st.snip140_state = 3
        elif st.snip140_state == 3:
            if self._gt(rsi_ma50, 65):
                st.snip140_state = 4
        elif st.snip140_state == 4:
            if self._lt(rsi_ma50, 37):
                st.snip140_state = 5
        elif st.snip140_state == 5:
            st.snip140_state = 0
            st.snip140_trigger = True

        # ---------- snippet 141 (CODE: rsi_ma50 < 40) ----------
        if st.snip141_state == 0:
            if st.snip130_trigger:
                st.snip141_state = 1
        elif st.snip141_state == 1:
            if self._gt(rsi_ma50, 66):
                st.snip141_state = 2
        elif st.snip141_state == 2:
            if self._lt(rsi_ma50, 40):  # CODE
                st.snip141_state = 3
        elif st.snip141_state == 3:
            st.snip141_state = 0
            st.snip141_trigger = True

        # ---------- snippet 142 ----------
        snip142_step1 = (
            st.snip74_trigger
            and self._lt(linear_regression, 0)
            and self._gt(points, long500)
        )
        if st.snip142_state == 0:
            if snip142_step1:
                st.snip142_state = 1
        elif st.snip142_state == 1:
            if self._gt(long350, long500) and self._gt(c, bbupper):
                st.snip142_state = 2
        elif st.snip142_state == 2:
            if self._lt(rsi_ma50, 35):
                st.snip142_state = 0
                st.snip142_trigger = True

        # ---------- snippet 143 ----------
        snip143_step1 = (
            st.snip74_trigger
            and self._lt(linear_regression, 0)
            and self._lt(points, long500)
        )
        if st.snip143_state == 0:
            if snip143_step1:
                st.snip143_state = 1
        elif st.snip143_state == 1:
            if self._gt(long350, long500) and self._gt(rsi_ma50, 64):
                st.snip143_state = 0
                st.snip143_trigger = True

        # ---------- background fill swap (kept, not plotted) ----------
        if st.snip64_trigger:
            st.bg_state = 1
        if st.snippet1_condition:
            st.bg_state = 2

        # ---------- aggregate ----------
        triggers = {}
        signals = {}
        plotted_emojis = []
        levels_used = set()
        per_level: Dict[int, List[str]] = {lv: [] for lv in ALL_LEVELS}

        for name, (level, emoji, is_plotted) in SNIPPET_SPECS.items():
            trg = self._get_trigger_for(st, name)
            triggers[name] = trg
            signals[name] = emoji if trg else ""
            if trg and is_plotted and level is not None:
                plotted_emojis.append(emoji)
                levels_used.add(level)
                per_level[level].append(emoji)

        plotted = " ".join(plotted_emojis)
        levels = ",".join(str(lv) for lv in sorted(levels_used))
        per_level_out = {f"level_{lv}": " ".join(per_level[lv])
                         for lv in ALL_LEVELS}

        return {
            "triggers": triggers,
            "signals": signals,
            "plotted": plotted,
            "levels": levels,
            "per_level": per_level_out,
        }

    @staticmethod
    def _get_trigger_for(st: SnippetState, name: str) -> bool:
        mapping = {
            "snip1":   None,
            "snip6":   None,
            "snip7":   None,
            "snip9":   None,
            "snip15":  "snip15_trigger",
            "snip16":  "snip16_trigger2",
            "snip34":  "snip34_trigger",
            "snip47":  "snip47_trigger_path1",
            "snip61":  "snip61_trigger1",
            "snip63":  "snip63_trigger",
            "snip64":  "snip64_trigger",
            "snip66":  "snip66_trigger_anchor",
            "snip67":  "snip67_trigger",
            "snip68":  "snip68_trigger_mic",
            "snip69":  "snip69_trigger",
            "snip70":  "snip70_trigger",
            "snip72":  "snip72_trigger",
            "snip73":  "snip73_trigger",
            "snip74":  "snip74_trigger",
            "snip76":  "snip76_trigger",
            "snip78":  "snip78_trigger",
            "snip79":  "snip79_trigger",
            "snip82":  "snip82_trigger",
            "snip83":  "snip83_trigger",
            "snip84":  "snip84_trigger",
            "snip85":  "snip85_trigger",
            "snip86":  "snip86_trigger",
            "snip88":  "snip88_trigger",
            "snip89":  "snip89_trigger",
            "snip90":  "snip90_trigger",
            "snip91":  "snip91_trigger",
            "snip94":  "snip94_trigger",
            "snip95":  "snip95_trigger",
            "snip97":  "snip97_trigger",
            "snip99":  "snip99_trigger",
            "snip100": "snip100_trigger",
            "snip102": "snip102_trigger",
            "snip104": "snip104_trigger",
            "snip105": "snip105_trigger",
            "snip107": "snip107_trigger",
            "snip108": "snip108_trigger",
            "snip109": "snip109_trigger",
            "snip111": "snip111_trigger",
            "snip112": "snip112_trigger",
            "snip114": "snip114_trigger",
            "snip119": "snip119_trigger",
            "snip120": "snip120_trigger",
            "snip121": "snip121_trigger",
            "snip123": "snip123_trigger",
            "snip125": "snip125_trigger",
            "snip126": "snip126_trigger_93",
            "snip127": "snip127_trigger",
            "snip128": "snip128_trigger",
            "snip129": "snip129_trigger",
            "snip130": "snip130_trigger",
            "snip131": "snip131_trigger",
            "snip132": "snip132_trigger",
            "snip133": "snip133_trigger",
            "snip134": "snip134_trigger",
            "snip135": "snip135_trigger",
            "snip136": "snip136_trigger",
            "snip137": "snip137_trigger",
            "snip138": "snip138_trigger",
            "snip139": "snip139_trigger",
            "snip140": "snip140_trigger",
            "snip141": "snip141_trigger",
            "snip142": "snip142_trigger",
            "snip143": "snip143_trigger",
        }
        attr = mapping.get(name)
        if attr is None:
            return False
        return bool(getattr(st, attr, False))


# ==================== SHARED STATE ====================
class Shared:
    def __init__(self, rsi_len, sma_len, merged_cfg):
        self.lock = threading.RLock()
        self.rsi_state = PineRsiState(rsi_len, sma_len)
        self.merged_state = MergedState(merged_cfg)
        self.snippet_state = SnippetState()
        self.evaluator = SnippetEvaluator(merged_cfg)
        self.last_ms: int = 0


shared: Optional[Shared] = None


# ==================== ROW BUILDER ====================
def build_row(utc, ms, close, high, low, rsi, rsi_ma, ind, snip_result):
    """
    Build one CSV row using the slim schema. Column order:
        identity -> oscillators -> snippet aggregations -> the rest.
    Per-snippet trigger/signal columns are NOT written (they'd balloon
    the file); only the aggregated `plotted`, `levels`, and per-level
    emoji columns are persisted, alongside the full indicator set.
    """
    def s(v, nd=8):
        if v is None:
            return ""
        try:
            f = float(v)
        except (TypeError, ValueError):
            return ""
        if f != f:
            return ""
        return f"{f:.{nd}f}"

    def b(v):
        return 1 if v else 0

    return [
        # identity
        utc,
        ms,
        s(close, 8),
        s(high, 8),
        s(low, 8),

        # base oscillators
        s(rsi, 6),
        s(rsi_ma, 6),

        # snippet aggregations (moved left)
        snip_result.get("plotted", ""),
        snip_result.get("levels", ""),
        *[snip_result["per_level"].get(f"level_{lv}", "") for lv in ALL_LEVELS],

        # ma_inv.py block
        s(ind.get("ma_350"), 8),
        s(ind.get("ma_500"), 8),
        s(ind.get("bbbasis"), 8),
        s(ind.get("bbdev"), 8),
        s(ind.get("bbupper"), 8),
        s(ind.get("bblower"), 8),
        s(ind.get("lr"), 8),
        b(ind.get("cond1")),
        b(ind.get("cond2")),
        s(ind.get("points"), 8),
        s(ind.get("col_up"), 8),
        s(ind.get("col_dn"), 8),
        b(ind.get("col_up_active")),
        b(ind.get("col_dn_active")),

        # pinescript.txt SMA block
        s(ind.get("short_002"), 8),
        s(ind.get("short_007"), 8),
        s(ind.get("short_14"), 8),
        s(ind.get("short_50"), 8),
        s(ind.get("long_100"), 8),
        s(ind.get("long_200"), 8),
        s(ind.get("long_350"), 8),
        s(ind.get("long_500"), 8),

        # pinescript.txt Auto-Fib block
        DEFAULTS["fiblength"],
        s(ind.get("maxr"), 8),
        s(ind.get("minr"), 8),
        s(ind.get("ranr"), 8),
        s(ind.get("fibo_1"), 8),
        s(ind.get("fibo_0_764"), 8),
        s(ind.get("fibo_0_618"), 8),
        s(ind.get("fibo_0_5"), 8),
        s(ind.get("fibo_0_382"), 8),
        s(ind.get("fibo_0_236"), 8),
        s(ind.get("fibo_0"), 8),
    ]


# ==================== COMPUTE FULL ====================
def compute_full(opens, highs, lows, closes, rsi_len, sma_len, merged_cfg):
    rsi_state = PineRsiState(rsi_len, sma_len)
    merged_state = MergedState(merged_cfg)
    snippet_state = SnippetState()
    evaluator = SnippetEvaluator(merged_cfg)

    rsi_out: List[Optional[float]] = []
    rsi_ma_out: List[Optional[float]] = []
    ind_out: List[dict] = []
    snip_out: List[dict] = []

    n = len(closes)
    for i in range(n):
        c = closes[i]
        h = highs[i] if i < len(highs) else c
        l = lows[i] if i < len(lows) else c

        r, rm = rsi_state.step(c)
        rsi_out.append(r)
        rsi_ma_out.append(rm)

        ind = merged_state.step(h, l, c)
        ind["close"] = c
        ind["high"] = h
        ind["low"] = l
        ind["rsi"] = r
        ind["rsi_ma50"] = rm

        snip_res = evaluator.evaluate(snippet_state, ind)
        ind["snippet1_condition"] = snippet_state.snippet1_condition
        ind["snippet6_condition"] = snippet_state.snippet6_condition
        ind["snippet7_condition"] = snippet_state.snippet7_condition
        ind["snippet9_condition"] = snippet_state.snippet9_condition
        ind_out.append(ind)
        snip_out.append(snip_res)

    return (rsi_out, rsi_ma_out, ind_out, snip_out,
            rsi_state, merged_state, snippet_state)


# ==================== STATE FILE ====================
def save_state(path: Path, last_open_time_ms: int,
               rsi_state: PineRsiState,
               merged_state: MergedState,
               snippet_state: SnippetState):
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = {
        "last_open_time_ms": last_open_time_ms,
        "rsi_state": rsi_state.to_dict(),
        "merged_state": merged_state.to_dict(),
        "snippet_state": _snippet_state_to_dict(snippet_state),
        "saved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    os.replace(tmp, path)


def _snippet_state_to_dict(st: SnippetState) -> dict:
    d = {}
    for k, v in st.__dict__.items():
        if isinstance(v, (bool, int, float, str)) or v is None:
            d[k] = v
    return d


def load_state(path: Path, merged_cfg):
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        last = int(data.get("last_open_time_ms") or 0)
        rs = PineRsiState.from_dict(data["rsi_state"])
        ms = MergedState.from_dict(data.get("merged_state") or {}, merged_cfg)
        ss = SnippetState()
        for k, v in (data.get("snippet_state") or {}).items():
            if hasattr(ss, k):
                setattr(ss, k, v)
        return last, rs, ms, ss
    except Exception as e:
        log("err", f"state load failed for {path}: {e}")
        return None


# ==================== TABLES ====================
def render_snippet_tail_table(rows: List[dict], n: int = 10) -> str:
    if not rows:
        return "(no data)"
    tail = rows[-n:]
    DIM = "\x1b[2m"
    RST = "\x1b[0m"

    def c_time(s):
        return f"\x1b[36m{s}{RST}"

    def c_close(v, prev):
        if v is None:
            return f"{DIM}—{RST}"
        try:
            f = float(v)
        except (TypeError, ValueError):
            return f"{DIM}—{RST}"
        txt = f"{f:,.2f}"
        if prev is None:
            return f"\x1b[37m{txt}{RST}"
        try:
            p = float(prev)
        except (TypeError, ValueError):
            return f"\x1b[37m{txt}{RST}"
        if f > p:
            return f"\x1b[1;32m{txt}{RST}"
        if f < p:
            return f"\x1b[1;31m{txt}{RST}"
        return f"\x1b[37m{txt}{RST}"

    def c_plotted(s):
        if not s:
            return f"{DIM}·{RST}"
        return f"\x1b[1;33m{s}{RST}"

    def c_levels(s):
        if not s:
            return f"{DIM}·{RST}"
        return f"\x1b[1;36m{s}{RST}"

    prevs = [None] + [r.get("close") for r in rows[-n - 1:-1]]
    out = []
    for r, prev in zip(tail, prevs):
        out.append([
            c_time(r.get("open_time_utc", "")),
            c_close(r.get("close"), prev),
            c_plotted(r.get("plotted", "")),
            c_levels(r.get("levels", "")),
        ])
    return tabulate(
        out,
        headers=["time (eat)", "close", "plotted", "levels"],
        tablefmt="rounded_outline",
        colalign=("left", "right", "left", "left"),
        disable_numparse=True,
    )


def print_tail_panel(rows, title: str):
    body = render_snippet_tail_table(rows, n=10)
    console.print(Panel(
        body,
        title=f"[bold white]{title}[/bold white]",
        border_style="bright_blue",
        padding=(0, 1),
    ))


# ==================== BANNER ====================
def print_banner(in_path, out_path, state_path):
    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="right", style="bold bright_black")
    grid.add_column(style="white")

    grid.add_row("input",   f"[cyan]{in_path}[/cyan]")
    grid.add_row("output",  f"[cyan]{out_path}[/cyan]")
    grid.add_row("state",   f"[cyan]{state_path}[/cyan]")
    grid.add_row("rsi len", f"[magenta]{DEFAULTS['rsi_length']}[/magenta]")
    grid.add_row("rsi sma", f"[magenta]{DEFAULTS['rsi_sma_length']}[/magenta]")
    grid.add_row("bb",      f"[magenta]{DEFAULTS['bb_length']}, {DEFAULTS['bb_mult']}[/magenta]")
    grid.add_row("lr len",  f"[magenta]{DEFAULTS['lr_length']}[/magenta]")
    grid.add_row("inv lb",  f"[magenta]{DEFAULTS['inv_lookback']}[/magenta]")
    grid.add_row("fiblength", f"[magenta]{DEFAULTS['fiblength']}[/magenta]")
    grid.add_row("snippets", f"[magenta]{len(SNIPPET_SPECS)}[/magenta]")
    grid.add_row("levels",  f"[magenta]{ALL_LEVELS}[/magenta]")
    grid.add_row("columns", f"[magenta]{len(OUTPUT_COLUMNS)}[/magenta]")
    grid.add_row("poll",    f"[green]{DEFAULTS['poll']}s[/green]")

    console.print(Panel(
        Align.left(grid),
        title="[bold white on blue] PINE SNIPPETS -> PYTHON (CODE-FAITHFUL) [/bold white on blue]",
        subtitle="[italic bright_black]Based on the RUNNABLE Pine code; comments ignored[/italic bright_black]",
        border_style="bright_blue",
        padding=(1, 2),
    ))


# ==================== MAIN ====================
def main():
    global shared

    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=DEFAULTS["input_csv"])
    p.add_argument("--output", default=DEFAULTS["output_csv"])
    p.add_argument("--state", default=DEFAULTS["state_json"])
    p.add_argument("--poll", type=float, default=DEFAULTS["poll"])
    args = p.parse_args()

    in_path = Path(args.csv)
    out_path = Path(args.output)
    state_path = Path(args.state)

    merged_cfg = {
        "ma_fast": DEFAULTS["ma_fast"],
        "ma_slow": DEFAULTS["ma_slow"],
        "bb_length": DEFAULTS["bb_length"],
        "bb_mult": DEFAULTS["bb_mult"],
        "lr_length": DEFAULTS["lr_length"],
        "lr_norm_len": DEFAULTS["lr_norm_len"],
        "bar_index_base": 0,
        "inv_lookback": DEFAULTS["inv_lookback"],
        "short_002": DEFAULTS["short_002"],
        "short_007": DEFAULTS["short_007"],
        "short_14":  DEFAULTS["short_14"],
        "short_50":  DEFAULTS["short_50"],
        "long_100":  DEFAULTS["long_100"],
        "long_200":  DEFAULTS["long_200"],
        "long_350":  DEFAULTS["long_350"],
        "long_500":  DEFAULTS["long_500"],
        "fiblength": DEFAULTS["fiblength"],
    }

    shared = Shared(DEFAULTS["rsi_length"], DEFAULTS["rsi_sma_length"], merged_cfg)

    ensure_output_header(out_path)

    console.print()
    print_banner(in_path, out_path, state_path)
    console.print(Rule(style="bright_black"))

    log("boot", f"reading [cyan]{in_path}[/cyan] …")
    times_utc, times_ms, opens, highs, lows, closes = read_ohlc_csv(in_path)
    log("boot", f"loaded [bold]{len(closes)}[/bold] bars")

    with shared.lock:
        if closes:
            with Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[bold cyan]computing indicators + all snippets[/bold cyan]"),
                BarColumn(bar_width=None, complete_style="cyan",
                          finished_style="green"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console, transient=True,
            ) as prog:
                task = prog.add_task("compute", total=len(closes))
                (rsi, rsi_ma, ind_out, snip_out,
                 rs, ms, ss) = compute_full(
                    opens, highs, lows, closes,
                    DEFAULTS["rsi_length"], DEFAULTS["rsi_sma_length"],
                    merged_cfg)
                prog.update(task, advance=len(closes))

            rows = []
            display_rows = []
            for i in range(len(closes)):
                row = build_row(
                    times_utc[i], times_ms[i], closes[i],
                    highs[i], lows[i],
                    rsi[i], rsi_ma[i], ind_out[i], snip_out[i],
                )
                rows.append(row)
                d = dict(ind_out[i])
                d["open_time_utc"] = times_utc[i]
                d["close"] = closes[i]
                d["plotted"] = snip_out[i]["plotted"]
                d["levels"] = snip_out[i]["levels"]
                display_rows.append(d)

            write_output_csv(out_path, rows)
            shared.rsi_state = rs
            shared.merged_state = ms
            shared.snippet_state = ss
            shared.last_ms = times_ms[-1]
            save_state(state_path, shared.last_ms,
                       shared.rsi_state, shared.merged_state,
                       shared.snippet_state)

            log("boot", f"wrote [bold]{len(rows)}[/bold] rows → "
                        f"[cyan]{out_path}[/cyan]")
            log("boot", f"state saved: last_open_time_ms="
                        f"[cyan]{ms_to_iso_eat(shared.last_ms)}[/cyan]")

            print_tail_panel(display_rows, "tail (computed at boot)")
        else:
            shared.last_ms = 0
            log("warn", "no data yet; will poll until the CSV appears.")

    console.print(Rule(style="bright_black"))
    log("watch", f"polling [cyan]{in_path}[/cyan] every "
                 f"[green]{args.poll}s[/green]; Ctrl+C to stop")
    console.print(Rule(style="bright_black"))

    last_heartbeat = time.time()

    while keep_running:
        time.sleep(args.poll)
        if not keep_running:
            break

        try:
            times_utc, times_ms, opens, highs, lows, closes = read_ohlc_csv(in_path)
            if not closes:
                continue

            with shared.lock:
                max_ms = times_ms[-1]
                if shared.last_ms > max_ms:
                    log("watch", f"input CSV was reset "
                                 f"(last_ms={ms_to_iso_eat(shared.last_ms)} > "
                                 f"max_ms={ms_to_iso_eat(max_ms)}); "
                                 f"full recompute.")
                    (rsi, rsi_ma, ind_out, snip_out,
                     rs, ms, ss) = compute_full(
                        opens, highs, lows, closes,
                        DEFAULTS["rsi_length"], DEFAULTS["rsi_sma_length"],
                        merged_cfg)
                    rows = []
                    display_rows = []
                    for i in range(len(closes)):
                        rows.append(build_row(
                            times_utc[i], times_ms[i], closes[i],
                            highs[i], lows[i],
                            rsi[i], rsi_ma[i], ind_out[i], snip_out[i],
                        ))
                        d = dict(ind_out[i])
                        d["open_time_utc"] = times_utc[i]
                        d["close"] = closes[i]
                        d["plotted"] = snip_out[i]["plotted"]
                        d["levels"] = snip_out[i]["levels"]
                        display_rows.append(d)
                    write_output_csv(out_path, rows)
                    shared.rsi_state = rs
                    shared.merged_state = ms
                    shared.snippet_state = ss
                    shared.last_ms = times_ms[-1]
                    save_state(state_path, shared.last_ms,
                               shared.rsi_state, shared.merged_state,
                               shared.snippet_state)
                    print_tail_panel(display_rows,
                                     "tail (after reset recompute)")
                    continue

                new_indices = [i for i, t in enumerate(times_ms)
                               if t > shared.last_ms]
                if new_indices:
                    rows_to_append = []
                    for i in new_indices:
                        close = closes[i]
                        high = highs[i] if i < len(highs) else close
                        low = lows[i] if i < len(lows) else close

                        rsi, rsi_ma = shared.rsi_state.step(close)
                        ind = shared.merged_state.step(high, low, close)
                        ind["close"] = close
                        ind["high"] = high
                        ind["low"] = low
                        ind["rsi"] = rsi
                        ind["rsi_ma50"] = rsi_ma

                        snip_res = shared.evaluator.evaluate(
                            shared.snippet_state, ind)
                        ind["snippet1_condition"] = shared.snippet_state.snippet1_condition
                        ind["snippet6_condition"] = shared.snippet_state.snippet6_condition
                        ind["snippet7_condition"] = shared.snippet_state.snippet7_condition
                        ind["snippet9_condition"] = shared.snippet_state.snippet9_condition

                        rows_to_append.append(build_row(
                            times_utc[i], times_ms[i], close,
                            high, low,
                            rsi, rsi_ma, ind, snip_res,
                        ))

                        line = Text()
                        line.append("  + ", style="bright_black")
                        line.append(times_utc[i], style="time")
                        line.append(f"  close={close:.2f}")
                        if snip_res["plotted"]:
                            line.append("  plotted=")
                            line.append(snip_res["plotted"],
                                        style="bold yellow")
                            line.append(f"  levels={snip_res['levels']}",
                                        style="cyan")
                        console.print(line)

                    if rows_to_append:
                        append_output_rows(out_path, rows_to_append)
                        shared.last_ms = times_ms[-1]
                        save_state(state_path, shared.last_ms,
                                   shared.rsi_state, shared.merged_state,
                                   shared.snippet_state)

                        o_rows = _read_output_tail(out_path, 10)
                        if o_rows:
                            print_tail_panel(o_rows, "tail (last 10 bars)")

            if time.time() - last_heartbeat > DEFAULTS["heartbeat_seconds"]:
                last_heartbeat = time.time()
                console.print(Rule(style="bright_black"))
                log("hb", f"{ms_to_iso_eat(int(time.time() * 1000))}  "
                          f"last_bar=[cyan]{ms_to_iso_eat(shared.last_ms)}[/cyan]  "
                          f"bars_fed=[magenta]{shared.rsi_state.bars_fed}[/magenta]")
                console.print(Rule(style="bright_black"))

        except Exception as e:
            log("err", f"watch error: {e}")
            time.sleep(1.0)

    log("ok", "done.")


def _read_output_tail(path: Path, n: int) -> List[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    dq = deque(maxlen=n)
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            dq.append(row)
    return list(dq)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print()
        console.print("[bold red]interrupted.[/bold red]")