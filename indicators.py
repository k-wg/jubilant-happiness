#!/usr/bin/env python3
# Pine-faithful RSI / SMA / Bollinger / LR-invalidation calculator for
# Binance 3m OHLC CSV data (persistent, merged).
#
# This file merges the original rsi_calc.py (base) with the math from
# ma_inv.py AND the SMA / Auto-Fib math from pinescript.txt.
#
# RSI / SMA(rsi) math, persistent loop, integrity thread, state file,
# terminal style: from rsi_calc.py.
# MA(350/500), Bollinger(200,2.0), LR oscillator, invalidation levels:
# from ma_inv.py (verbatim formulas, ddof=0 population stdev, newest-first
# OLS window, z-score normalization, ta.crossover/crossunder, points var).
# SMA(2/7/14/50/100/200/350/500) + Auto-Fib(5500): from pinescript.txt
# (ta.sma, ta.highest, ta.lowest, fibo_* levels — verbatim).
#
# Design:
#   - Always running. No exit path except SIGINT/SIGTERM.
#   - On boot:
#       * read binance_3m_closed.csv in full
#       * compute rsi / rsi_ma50 from bar 0 using Pine v6 recursion
#       * compute ma_350 / ma_500 / bbbasis / bbdev / bbupper / bblower /
#         lr / cond1 / cond2 / points / col_up / col_dn from bar 0
#       * compute short_002 / short_007 / short_14 / short_50 /
#         long_100 / long_200 / long_350 / long_500 and the Auto-Fib
#         block (maxr / minr / ranr / fibo_*) from bar 0
#       * write the full merged CSV
#       * save recursion + rolling-window state to rsi_state.json
#   - While running:
#       * poll the OHLC CSV for new closed bars
#       * advance the recursion one bar at a time
#       * append new rows to the merged CSV
#       * update the state file
#   - Integrity thread (parallel, daemon):
#       * every 10 min : tail check (last 50 rows).
#       * every 180 min: full check.
#       * on inconsistency: recompute the whole series from
#         binance_3m_closed.csv using the same Pine recursions.
#   - Heartbeat every 5 minutes.
#
# Pine v6 ta.rsi:
#   chg = ta.change(src); u = max(chg,0); d = max(-chg,0)
#   rsiu = ta.rma(u,length); rsid = ta.rma(d,length)
#   rsi  = rsid==0 ? 100 : rsiu==0 ? 0 : 100 - 100/(1 + rsiu/rsid)
# Pine ta.rma: alpha = 1/length; seed = sma(src,length) at index length-1.
# Pine ta.sma : rolling mean over the last `length` values.
# Pine ta.stdev: population stdev (ddof=0).
# Pine ta.highest / ta.lowest: rolling max / min over `length` values,
#   first output at index `length - 1`.
#
# Visuals: rich for panels / rules / colors / spinners;
#          tabulate for the data tables (rounded_outline).
# Math is UNCHANGED from the original rsi_calc.py and ma_inv.py; the
# pinescript.txt addition is additive only.

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
from typing import List, Optional, Tuple

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
    "output_csv": "indicators_data.csv",
    "state_json": "rsi_state.json",
    "rsi_length": 7,
    "sma_length": 50,

    # Merged from ma_inv.py CONFIG.
    "ma_fast": 350,
    "ma_slow": 500,
    "bb_length": 200,
    "bb_mult": 2.0,
    "lr_length": 250,
    "lr_upper": 1.5,
    "lr_lower": -1.5,
    "lr_norm_len": 100,
    "bar_index_base": 0,
    "inv_lookback": 5,

    # ----- from pinescript.txt -----
    # Moving-average block. Fixed lengths; they match the Pine source.
    "short_002": 2,
    "short_007": 7,
    "short_14":  14,
    "short_50":  50,
    "long_100":  100,
    "long_200":  200,
    "long_350":  350,
    "long_500":  500,
    # Auto-Fib input (input(5500) in the Pine source).
    "fiblength": 5500,

    "poll": 1.0,
    "heartbeat_seconds": 300.0,

    # Integrity timing (seconds)
    "tail_check_seconds": 600.0,      # 10 min
    "full_check_seconds": 10800.0,    # 180 min
    "integrity_tick_seconds": 60.0,   # thread wake-up cadence

    # How many tail rows the cheap check inspects.
    "integrity_tail_rows": 50,

    # Expected bar spacing (ms). 3m = 180_000.
    "interval_ms": 180_000,

    # Display timezone (EAT = +03:00) — from ma_inv.py.
    "display_tz_offset_hours": 3,
}

DISPLAY_TZ = timezone(timedelta(hours=DEFAULTS["display_tz_offset_hours"]))

OUTPUT_COLUMNS = [
    # --- from rsi_calc.py ---
    "open_time_utc",
    "open_time_ms",
    "close",
    "rsi",
    "rsi_ma50",
    # --- from ma_inv.py ---
    "high",
    "low",
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
    # --- from pinescript.txt (Moving average block) ---
    "short_002",
    "short_007",
    "short_14",
    "short_50",
    "long_100",
    "long_200",
    "long_350",
    "long_500",
    # --- from pinescript.txt (Auto_Fib block) ---
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
    "rsi.ob":        "bold red",
    "rsi.strong":    "bold yellow",
    "rsi.mid":       "yellow",
    "rsi.weak":      "green",
    "rsi.os":        "bold green",
    "rsi.na":        "dim white",
    "close.up":      "bold green",
    "close.down":    "bold red",
    "close.flat":    "white",
    "time":          "cyan",
    "ma350":         "bold red",
    "ma500":         "bold blue",
    "bbbasis":       "bold cyan",
    "bbupper":       "grey70",
    "bblower":       "grey70",
    "lr":            "bold yellow",
    "col_up":        "bold #10cab8",
    "col_dn":        "bold blue",
    "up":            "bold green",
    "down":          "bold red",
    "flat":          "white",
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


# ==================== COLOR HELPERS ====================
def rsi_style(v: Optional[float]) -> str:
    if v is None:
        return "rsi.na"
    if v >= 70:
        return "rsi.ob"
    if v >= 60:
        return "rsi.strong"
    if v >= 50:
        return "rsi.mid"
    if v >= 40:
        return "rsi.weak"
    return "rsi.os"


def rsi_text(v: Optional[float]) -> Text:
    if v is None:
        return Text("n/a", style="rsi.na")
    return Text(f"{v:7.3f}", style=rsi_style(v))


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


def ms_to_iso(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


def ms_to_iso_eat(ms) -> str:
    """Display timezone formatter (EAT, +03:00) — from ma_inv.py."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=DISPLAY_TZ) \
                       .isoformat(timespec="seconds")
    except Exception:
        return ""


# ==================== CSV I/O ====================
def read_ohlc_csv(path: Path):
    """
    Read the OHLC CSV. Returns (times_utc, times_ms, closes, highs, lows).
    """
    times_utc: List[str] = []
    times_ms: List[int] = []
    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    if not path.exists():
        return times_utc, times_ms, closes, highs, lows
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
                c = float(row["close"])
                h = float(row.get("high", c))
                l = float(row.get("low", c))
            except (KeyError, ValueError, TypeError):
                continue
            times_utc.append(row.get("open_time_utc", ""))
            times_ms.append(t_ms)
            closes.append(c)
            highs.append(h)
            lows.append(l)
    return times_utc, times_ms, closes, highs, lows


def read_ohlc_csv_tail(path: Path, n: int):
    """Read only the last n rows of the OHLC CSV."""
    if not path.exists() or path.stat().st_size == 0:
        return [], [], [], [], []
    dq_utc: deque = deque(maxlen=n)
    dq_ms: deque = deque(maxlen=n)
    dq_close: deque = deque(maxlen=n)
    dq_high: deque = deque(maxlen=n)
    dq_low: deque = deque(maxlen=n)
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
            try:
                dq_high.append(float(row.get("high", c)))
            except (TypeError, ValueError):
                dq_high.append(c)
            try:
                dq_low.append(float(row.get("low", c)))
            except (TypeError, ValueError):
                dq_low.append(c)
    return list(dq_utc), list(dq_ms), list(dq_close), \
           list(dq_high), list(dq_low)


def read_rsi_csv_tail(path: Path, n: int):
    """Read only the last n rows of the merged output CSV."""
    if not path.exists() or path.stat().st_size == 0:
        return {}, []
    dq_rows: deque = deque(maxlen=n)
    dq_ms: deque = deque(maxlen=n)
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
            except (KeyError, ValueError, TypeError):
                continue
            dq_rows.append(row)
            dq_ms.append(t_ms)
    return list(dq_rows), list(dq_ms)


def read_rsi_csv_full(path: Path):
    """Read the entire merged output CSV (as dicts + ms)."""
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    out_rows: List[dict] = []
    out_ms: List[int] = []
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                t_ms = int(row["open_time_ms"])
            except (KeyError, ValueError, TypeError):
                continue
            out_rows.append(row)
            out_ms.append(t_ms)
    return out_rows, out_ms


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


# ==================== PINE RSI RECURSION (UNCHANGED) ====================
class PineRsiState:
    """
    Holds exactly what Pine holds for ta.rsi + ta.sma(rsi, N).
    """
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


# ==================== MERGED MA / BB / LR STATE ====================
class MergedState:
    """
    Holds the rolling windows required to advance the ma_inv.py math AND
    the pinescript.txt SMA / Auto-Fib math one bar at a time.

    Windows kept:
      * close_window_fast / close_window_slow : last ma_fast / ma_slow closes
      * close_window_bb                   : last bb_length closes
      * high_window_inv / low_window_inv  : last inv_lookback highs / lows
      * lr_window                         : last lr_length closes (chronological)
      * lr_series_window                  : last lr_norm_len non-NaN lr values
      * lr_series_sum / lr_series_sumsq   : running sum / sum of squares of
                                            lr_series_window
      * last_points                       : last non-NaN `points` value
      * last_lr                           : last `lr` value (for crossover)
      * last_cond1 / last_cond2           : last cond1 / cond2 (for cross)

    Windows added for pinescript.txt:
      * s002_win, s007_win, s14_win, s50_win
      * l100_win, l200_win, l350_win, l500_win
      * fib_win (deque maxlen=fiblength, of closes for ta.highest/ta.lowest)

    IMPORTANT: the original `ma_350` / `ma_500` columns keep their exact
    Pine names from ma_inv.py (short_50 is *not* re-used for them). The
    pinescript.txt SMA block writes its own separate columns
    (short_002, ..., long_500). This preserves the merged-CSV contract.
    """
    def __init__(self, cfg):
        self.ma_fast = cfg["ma_fast"]
        self.ma_slow = cfg["ma_slow"]
        self.bb_length = cfg["bb_length"]
        self.bb_mult = cfg["bb_mult"]
        self.lr_length = cfg["lr_length"]
        self.lr_norm_len = cfg["lr_norm_len"]
        self.bar_index_base = cfg["bar_index_base"]
        self.inv_lookback = cfg["inv_lookback"]

        # pinescript.txt MA lengths.
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

        # Chronological close window for the LR oscillator.
        self.lr_window: deque = deque(maxlen=self.lr_length)

        # Rolling stats of the `lr` series for the z-score normalization.
        self.lr_series_window: deque = deque(maxlen=self.lr_norm_len)
        self.lr_series_sum: float = 0.0
        self.lr_series_sumsq: float = 0.0

        # Crossover / invalidation state.
        self.last_points: Optional[float] = None
        self.last_lr: Optional[float] = None
        self.last_cond1: bool = False
        self.last_cond2: bool = False

        # ---- pinescript.txt SMA windows ----
        self.s002_win: deque = deque(maxlen=self.s002)
        self.s007_win: deque = deque(maxlen=self.s007)
        self.s14_win:  deque = deque(maxlen=self.s14)
        self.s50_win:  deque = deque(maxlen=self.s50)
        self.l100_win: deque = deque(maxlen=self.l100)
        self.l200_win: deque = deque(maxlen=self.l200)
        self.l350_win: deque = deque(maxlen=self.l350)
        self.l500_win: deque = deque(maxlen=self.l500)

        # ---- pinescript.txt Auto-Fib window ----
        # ta.highest(close, fiblength) / ta.lowest(close, fiblength):
        # rolling extrema over the last `fiblength` *closes*.
        self.fib_win: deque = deque(maxlen=self.fiblength)

        self.bar_index: int = 0  # local counter; base added when used

    # ---------- helpers ----------
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
        """Pine ta.sma: rolling mean, None until the window is full."""
        if len(window) < window.maxlen:
            return None
        return sum(window) / window.maxlen

    # ---------- main step ----------
    def step(self, high: float, low: float, close: float):
        """
        Advance one bar. Returns a dict with the ma_inv.py outputs AND
        the pinescript.txt outputs. All values may be None during warmup.
        """
        self.bar_index += 1

        # --- SMA windows (ma_inv.py) ---
        self.close_window_fast.append(close)
        self.close_window_slow.append(close)
        ma_fast = (sum(self.close_window_fast) / self.ma_fast
                   if len(self.close_window_fast) == self.ma_fast else None)
        ma_slow = (sum(self.close_window_slow) / self.ma_slow
                   if len(self.close_window_slow) == self.ma_slow else None)

        # --- Bollinger windows ---
        self.close_window_bb.append(close)
        if len(self.close_window_bb) == self.bb_length:
            bb_window = list(self.close_window_bb)
            bb_n = self.bb_length
            bb_mean = sum(bb_window) / bb_n
            # Population stdev (ddof=0) — exactly like Pine's ta.stdev.
            bb_var = sum((x - bb_mean) ** 2 for x in bb_window) / bb_n
            bb_std = math.sqrt(bb_var)
            bbbasis = bb_mean
            bbdev = self.bb_mult * bb_std
            bbupper = bbbasis + bbdev
            bblower = bbbasis - bbdev
        else:
            bbbasis = bbdev = bbupper = bblower = None

        # --- LR oscillator ---
        self.lr_window.append(close)
        lr_raw = None
        if len(self.lr_window) == self.lr_length:
            # Pine's source[i] is i bars ago -> newest-first ordering.
            window_newest_first = list(self.lr_window)[::-1]
            m, c = self._ols_slope_intercept_newest_first(window_newest_first)
            if not (math.isnan(m) or math.isnan(c)):
                bar_index_t = self.bar_index_base + (self.bar_index - 1)
                lr_raw = -(m * bar_index_t + c)

        lr = None
        if lr_raw is not None:
            # Add lr_raw to the rolling window; update running sums.
            if len(self.lr_series_window) == self.lr_norm_len:
                old = self.lr_series_window[0]  # will be evicted by deque
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

        # --- crossover / crossunder vs 0 ---
        cond1 = False
        cond2 = False
        if self.last_lr is not None and lr is not None:
            # ta.crossunder(lr, 0): prev >= 0 and cur < 0
            cond1 = (self.last_lr >= 0.0) and (lr < 0.0)
            # ta.crossover(lr, 0): prev <= 0 and cur > 0
            cond2 = (self.last_lr <= 0.0) and (lr > 0.0)
        if lr is not None:
            self.last_lr = lr

        # --- invalidation levels ---
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

        # col_up_active / col_dn_active
        col_up_active = (points is not None) and (lr is not None) and (lr > 0)
        col_dn_active = (points is not None) and (lr is not None) and (lr <= 0)

        # Pine skips the signal bar itself: ta.change(points) == 0
        col_up = None
        col_dn = None
        if points is not None and self._prev_points_for_change is not None \
                and self._prev_points_for_change == points:
            if col_up_active:
                col_up = points
            if col_dn_active:
                col_dn = points
        self._prev_points_for_change = points

        # ==================== pinescript.txt additions ====================
        # ----- Moving average block (verbatim ta.sma) -----
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

        # ----- Auto_Fib block (verbatim ta.highest / ta.lowest + fibs) -----
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
            # ma_inv.py outputs (unchanged)
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
            # pinescript.txt outputs
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

    # We keep the previous `points` separately for the ta.change check.
    _prev_points_for_change: Optional[float] = None

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
            # pinescript.txt SMA windows
            "s002_win": list(self.s002_win),
            "s007_win": list(self.s007_win),
            "s14_win":  list(self.s14_win),
            "s50_win":  list(self.s50_win),
            "l100_win": list(self.l100_win),
            "l200_win": list(self.l200_win),
            "l350_win": list(self.l350_win),
            "l500_win": list(self.l500_win),
            # pinescript.txt Auto-Fib window
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
        # pinescript.txt SMA windows
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


# ==================== COMPUTE FROM FULL HISTORY ====================
def compute_full(closes, highs, lows, rsi_len, sma_len, merged_cfg):
    """
    Recompute the whole series from scratch.
    """
    rsi_state = PineRsiState(rsi_len, sma_len)
    merged_state = MergedState(merged_cfg)

    rsi_out: List[Optional[float]] = []
    ma_out: List[Optional[float]] = []
    merged_out: List[dict] = []

    n = len(closes)
    for i in range(n):
        c = closes[i]
        h = highs[i] if i < len(highs) else c
        l = lows[i] if i < len(lows) else c

        r, m = rsi_state.step(c)
        rsi_out.append(r)
        ma_out.append(m)

        merged_out.append(merged_state.step(h, l, c))

    return rsi_out, ma_out, rsi_state, merged_out, merged_state


# ==================== STATE FILE ====================
def save_state(path: Path, last_open_time_ms: int,
               rsi_state: PineRsiState, merged_state: MergedState):
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = {
        "last_open_time_ms": last_open_time_ms,
        "state": rsi_state.to_dict(),
        "merged_state": merged_state.to_dict(),
        "saved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    os.replace(tmp, path)


def load_state(path: Path, merged_cfg):
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        last = int(data.get("last_open_time_ms") or 0)
        st = PineRsiState.from_dict(data["state"])
        ms = MergedState.from_dict(data.get("merged_state") or {}, merged_cfg)
        return last, st, ms
    except Exception as e:
        log("err", f"state load failed for {path}: {e}")
        return None


# ==================== SHARED STATE ====================
class Shared:
    def __init__(self, rsi_len: int, sma_len: int, merged_cfg):
        self.lock = threading.RLock()
        self.state: PineRsiState = PineRsiState(rsi_len, sma_len)
        self.merged_state: MergedState = MergedState(merged_cfg)
        self.last_ms: int = 0
        self.last_scan_kind: str = "-"
        self.last_scan_ok: Optional[bool] = None
        self.last_scan_ts: float = 0.0
        self.last_scan_detail: str = ""


shared: Optional[Shared] = None


# ==================== FORMATTING HELPERS ====================
def fmt_num(v, ndigits: int = 4) -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:
        return "—"
    return f"{f:,.{ndigits}f}"


def fmt_bool(v) -> str:
    try:
        b = bool(v)
    except (TypeError, ValueError):
        return "—"
    return "Y" if b else "·"


def _hex_fg(hexcol: str) -> str:
    h = hexcol.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"38;2;{r};{g};{b}"


TEAL = _hex_fg("#10cab8")
BLUE = "1;34"


# ==================== TABLES (tabulate) ====================
def render_rsi_tail_table(times_utc, closes, rsi_vals, ma_vals, n: int = 10) -> str:
    if not closes:
        return "(no data)"
    start = max(0, len(closes) - n)

    def c_rsi(v):
        if v is None:
            return "\x1b[2mn/a\x1b[0m"
        if v >= 70:
            return f"\x1b[1;31m{v:7.3f}\x1b[0m"
        if v >= 60:
            return f"\x1b[1;33m{v:7.3f}\x1b[0m"
        if v >= 50:
            return f"\x1b[33m{v:7.3f}\x1b[0m"
        if v >= 40:
            return f"\x1b[32m{v:7.3f}\x1b[0m"
        return f"\x1b[1;32m{v:7.3f}\x1b[0m"

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
            c_time(times_utc[i]),
            c_close(prev, closes[i]),
            c_rsi(rsi_vals[i]),
            c_rsi(ma_vals[i]),
        ])
        prev = closes[i]

    return tabulate(
        rows,
        headers=["open_time_utc", "close", "rsi", "rsi_ma50"],
        tablefmt="rounded_outline",
        colalign=("left", "right", "right", "right"),
        disable_numparse=True,
    )


def render_merged_tail_table(merged_rows, n: int = 10) -> str:
    """
    Redesigned tail table for the merged ma_inv.py columns.

    NOTE: pinescript.txt columns (short_*, long_*, fibo_*) are written to
    the output CSV but are intentionally NOT displayed here — the
    terminal view stays exactly the same as before.
    """
    if not merged_rows:
        return "(no data)"
    tail = merged_rows[-n:]
    # Previous close for the close-direction colouring.
    prevs = [None] + [r.get("close") for r in merged_rows[-n - 1:-1]]

    # ---------- ANSI helpers ----------
    DIM = "\x1b[2m"
    RST = "\x1b[0m"

    def c_price(v):
        """Price / MA / BB values: 2 decimals, dim when missing."""
        if v is None:
            return f"{DIM}—{RST}"
        try:
            f = float(v)
        except (TypeError, ValueError):
            return f"{DIM}—{RST}"
        if f != f:
            return f"{DIM}—{RST}"
        return f"{f:,.2f}"

    def c_close(cur, prev):
        if cur is None:
            return f"{DIM}—{RST}"
        try:
            f = float(cur)
        except (TypeError, ValueError):
            return f"{DIM}—{RST}"
        if f != f:
            return f"{DIM}—{RST}"
        txt = f"{f:,.2f}"
        if prev is None:
            return f"\x1b[37m{txt}{RST}"
        try:
            p = float(prev)
        except (TypeError, ValueError):
            return f"\x1b[37m{txt}{RST}"
        if p != p:
            return f"\x1b[37m{txt}{RST}"
        if f > p:
            return f"\x1b[1;32m{txt}{RST}"
        if f < p:
            return f"\x1b[1;31m{txt}{RST}"
        return f"\x1b[37m{txt}{RST}"

    def c_ma350(v):
        return c_price(v)

    def c_ma500(v):
        return c_price(v)

    def c_bb_basis(v):
        return f"\x1b[1;36m{c_price(v)}{RST}" if v is not None else f"{DIM}—{RST}"

    def c_bb_upper(v):
        return f"\x1b[37m{c_price(v)}{RST}" if v is not None else f"{DIM}—{RST}"

    def c_bb_lower(v):
        return f"\x1b[37m{c_price(v)}{RST}" if v is not None else f"{DIM}—{RST}"

    def c_lr(v):
        if v is None:
            return f"{DIM}—{RST}"
        try:
            f = float(v)
        except (TypeError, ValueError):
            return f"{DIM}—{RST}"
        if f != f:
            return f"{DIM}—{RST}"
        txt = f"{f:+.2f}"
        if f >= 0:
            return f"\x1b[1;33m{txt}{RST}"
        return f"\x1b[1;35m{txt}{RST}"

    def c_cross(cond, arrow, colour):
        if cond:
            return f"\x1b[{colour}m{arrow}{RST}"
        return f"{DIM}·{RST}"

    def c_signal(v, colour):
        if v is None:
            return f"{DIM}·{RST}"
        try:
            f = float(v)
        except (TypeError, ValueError):
            return f"{DIM}·{RST}"
        if f != f:
            return f"{DIM}·{RST}"
        return f"\x1b[{colour}m{f:,.2f}{RST}"

    def c_time(s):
        return f"\x1b[36m{s}{RST}"

    # ---------- headers ----------
    headers = [
        "time (utc)",
        "close",
        "MA350",
        "MA500",
        "BB mid",
        "BB up",
        "BB lo",
        "LR z",
        "x↓",
        "x↑",
        "col up",
        "col dn",
    ]

    # ---------- rows ----------
    rows = []
    for r, prev in zip(tail, prevs):
        rows.append([
            c_time(r.get("open_time_utc", "")),
            c_close(r.get("close"), prev),
            c_ma350(r.get("ma_350")),
            c_ma500(r.get("ma_500")),
            c_bb_basis(r.get("bbbasis")),
            c_bb_upper(r.get("bbupper")),
            c_bb_lower(r.get("bblower")),
            c_lr(r.get("lr")),
            c_cross(r.get("cond1"), "▼", "1;31"),
            c_cross(r.get("cond2"), "▲", "1;32"),
            c_signal(r.get("col_up"), TEAL),
            c_signal(r.get("col_dn"), BLUE),
        ])

    return tabulate(
        rows,
        headers=headers,
        tablefmt="rounded_outline",
        colalign=(
            "left", "right",
            "right", "right",
            "right", "right", "right",
            "right",
            "center", "center",
            "right", "right",
        ),
        disable_numparse=True,
    )


def print_tail_panels(times_utc, closes, rsi_vals, ma_vals,
                      merged_rows, title_prefix: str = "tail"):
    """Two stacked tables: RSI (from rsi_calc.py) and merged ma_inv.py."""
    console.print(Panel(
        Align.center(render_rsi_tail_table(times_utc, closes, rsi_vals, ma_vals, n=10)),
        title=f"[bold white]{title_prefix} — RSI (last 10 bars)[/bold white]",
        border_style="bright_blue",
        padding=(0, 1),
    ))
    console.print(Panel(
        Align.center(render_merged_tail_table(merged_rows, n=10)),
        title=f"[bold white]{title_prefix} — MA / BB / LR (last 10 bars)[/bold white]",
        border_style="bright_magenta",
        padding=(0, 1),
    ))


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
    when = ms_to_iso(int(shared.last_scan_ts * 1000)) if shared.last_scan_ts else "-"
    rows = [[
        f"\x1b[35m{kind}\x1b[0m",
        ok_txt,
        f"\x1b[36m{when}\x1b[0m",
        detail,
    ]]
    return tabulate(
        rows,
        headers=["scan", "result", "when_utc", "detail"],
        tablefmt="rounded_outline",
        colalign=("left", "left", "left", "left"),
        disable_numparse=True,
    )


# ==================== BANNER ====================
def print_banner(in_path, out_path, state_path, args):
    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="right", style="bold bright_black")
    grid.add_column(style="white")

    grid.add_row("input",   f"[cyan]{in_path}[/cyan]")
    grid.add_row("output",  f"[cyan]{out_path}[/cyan]")
    grid.add_row("state",   f"[cyan]{state_path}[/cyan]")
    grid.add_row("rsi length", f"[magenta]{args.length}[/magenta]")
    grid.add_row("sma length", f"[magenta]{args.sma}[/magenta]")
    grid.add_row("ma fast",   f"[magenta]{DEFAULTS['ma_fast']}[/magenta]")
    grid.add_row("ma slow",   f"[magenta]{DEFAULTS['ma_slow']}[/magenta]")
    grid.add_row("bb length", f"[magenta]{DEFAULTS['bb_length']}[/magenta]")
    grid.add_row("bb mult",   f"[magenta]{DEFAULTS['bb_mult']}[/magenta]")
    grid.add_row("lr length", f"[magenta]{DEFAULTS['lr_length']}[/magenta]")
    grid.add_row("lr norm",   f"[magenta]{DEFAULTS['lr_norm_len']}[/magenta]")
    grid.add_row("inv lookback", f"[magenta]{DEFAULTS['inv_lookback']}[/magenta]")
    grid.add_row("ma block (pine)",
                 f"[magenta]2/7/14/50/100/200/350/500[/magenta]")
    grid.add_row("fiblength",  f"[magenta]{DEFAULTS['fiblength']}[/magenta]")
    grid.add_row("poll",    f"[green]{args.poll}s[/green]")
    grid.add_row(
        "tail check",
        f"[bright_magenta]{DEFAULTS['tail_check_seconds'] / 60:.0f} min[/bright_magenta] "
        f"[bright_black](last {DEFAULTS['integrity_tail_rows']} rows)[/bright_black]",
    )
    grid.add_row(
        "full check",
        f"[bright_magenta]{DEFAULTS['full_check_seconds'] / 60:.0f} min[/bright_magenta] "
        f"[bright_black](entire CSV)[/bright_black]",
    )
    grid.add_row("heartbeat", f"[bright_black]{DEFAULTS['heartbeat_seconds']:.0f}s[/bright_black]")

    console.print(Panel(
        Align.left(grid),
        title="[bold white on blue] RSI / SMA / MA / BB / LR / PINE-MA / FIB CALCULATOR [/bold white on blue]",
        subtitle="[italic bright_black]Pine v6 ta.rsi + ta.sma + ta.stdev + LR osc + ta.highest/lowest · math unchanged[/italic bright_black]",
        border_style="bright_blue",
        padding=(1, 2),
    ))


# ==================== SEQUENCE CHECK ====================
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
                    f"got +{delta}ms ({ms_to_iso(times_ms[i-1])} -> "
                    f"{ms_to_iso(times_ms[i])})")
    return None


# ==================== ROW BUILDERS ====================
def build_row(utc, ms, close, high, low, rsi, rsi_ma, merged):
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

    return [
        utc,
        ms,
        s(close, 8),
        s(rsi, 6),
        s(rsi_ma, 6),
        s(high, 8),
        s(low, 8),
        s(merged.get("ma_350"), 8),
        s(merged.get("ma_500"), 8),
        s(merged.get("bbbasis"), 8),
        s(merged.get("bbdev"), 8),
        s(merged.get("bbupper"), 8),
        s(merged.get("bblower"), 8),
        s(merged.get("lr"), 8),
        1 if merged.get("cond1") else 0,
        1 if merged.get("cond2") else 0,
        s(merged.get("points"), 8),
        s(merged.get("col_up"), 8),
        s(merged.get("col_dn"), 8),
        1 if merged.get("col_up_active") else 0,
        1 if merged.get("col_dn_active") else 0,
        # --- pinescript.txt MA block ---
        s(merged.get("short_002"), 8),
        s(merged.get("short_007"), 8),
        s(merged.get("short_14"), 8),
        s(merged.get("short_50"), 8),
        s(merged.get("long_100"), 8),
        s(merged.get("long_200"), 8),
        s(merged.get("long_350"), 8),
        s(merged.get("long_500"), 8),
        # --- pinescript.txt Auto-Fib block ---
        DEFAULTS["fiblength"],
        s(merged.get("maxr"), 8),
        s(merged.get("minr"), 8),
        s(merged.get("ranr"), 8),
        s(merged.get("fibo_1"), 8),
        s(merged.get("fibo_0_764"), 8),
        s(merged.get("fibo_0_618"), 8),
        s(merged.get("fibo_0_5"), 8),
        s(merged.get("fibo_0_382"), 8),
        s(merged.get("fibo_0_236"), 8),
        s(merged.get("fibo_0"), 8),
    ]


# ==================== RECOMPUTE ====================
def recompute_from_ohlc(in_path: Path, out_path: Path, state_path: Path,
                        rsi_len: int, sma_len: int, merged_cfg, reason: str):
    """
    Full recompute using the SAME compute_full() as boot.
    """
    with Progress(
        SpinnerColumn(style="bright_magenta"),
        TextColumn(f"[bold bright_magenta]recomputing[/bold bright_magenta] "
                   f"[bright_black]({reason})[/bright_black]"),
        BarColumn(bar_width=None, complete_style="bright_magenta",
                  finished_style="green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console, transient=True,
    ) as prog:
        task = prog.add_task("recompute", total=None)
        times_utc, times_ms, closes, highs, lows = read_ohlc_csv(in_path)
        if not closes:
            log("warn", "input CSV empty at recompute time; skipping.")
            return
        prog.update(task, total=len(closes))
        rsi, rsi_ma, rsi_state, merged_out, merged_state = compute_full(
            closes, highs, lows, rsi_len, sma_len, merged_cfg)
        rows = []
        for i in range(len(closes)):
            rows.append(build_row(
                times_utc[i], times_ms[i], closes[i], highs[i], lows[i],
                rsi[i], rsi_ma[i], merged_out[i],
            ))
        prog.update(task, advance=len(closes))

    write_output_csv(out_path, rows)
    shared.state = rsi_state
    shared.merged_state = merged_state
    shared.last_ms = times_ms[-1]
    save_state(state_path, shared.last_ms, shared.state, shared.merged_state)

    log("ok", f"recompute done: {len(rows)} rows, "
              f"last=[cyan]{ms_to_iso(shared.last_ms)}[/cyan]")
    console.print(Rule(style="bright_black"))


# ==================== INTEGRITY CHECKS ====================
def integrity_check_tail(in_path, out_path, state_path,
                         rsi_len, sma_len, merged_cfg,
                         interval_ms) -> bool:
    n = DEFAULTS["integrity_tail_rows"]
    with shared.lock:
        in_utc, in_ms, in_close, in_high, in_low = read_ohlc_csv_tail(in_path, n)
        out_rows, out_ms = read_rsi_csv_tail(out_path, n)

        if not in_ms:
            shared.last_scan_kind = "tail"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = "empty OHLC CSV"
            log("integrity", "OHLC tail empty; nothing to verify.")
            return True

        reasons = []

        seq_reason = check_sequence(in_ms, interval_ms)
        if seq_reason:
            reasons.append(f"OHLC tail sequence: {seq_reason}")

        if len(out_ms) < len(in_ms):
            reasons.append(
                f"output tail shorter than OHLC tail "
                f"({len(out_ms)} < {len(in_ms)} rows in last {n})"
            )
        else:
            in_tail = in_ms[-len(in_ms):]
            out_tail = out_ms[-len(in_ms):]
            mismatches = 0
            first_bad = None
            for i in range(len(in_tail)):
                if out_tail[i] != in_tail[i]:
                    mismatches += 1
                    if first_bad is None:
                        first_bad = (i, in_tail[i], out_tail[i])
            if mismatches:
                i, exp, got = first_bad
                reasons.append(
                    f"output tail time mismatch at offset {i}: "
                    f"expected {ms_to_iso(exp)}, got {ms_to_iso(got)}; "
                    f"{mismatches} mismatch(es)"
                )

        out_seq_reason = check_sequence(out_ms, interval_ms)
        if out_seq_reason:
            reasons.append(f"output tail sequence: {out_seq_reason}")

        max_in = in_ms[-1]
        if shared.last_ms != max_in:
            reasons.append(
                f"shared.last_ms={ms_to_iso(shared.last_ms)} "
                f"!= newest OHLC tail bar={ms_to_iso(max_in)}"
            )

        loaded = load_state(state_path, merged_cfg)
        if loaded is None:
            reasons.append("state file missing or unreadable")
        else:
            file_last, _, _ = loaded
            if file_last != shared.last_ms:
                reasons.append(
                    f"state file last={ms_to_iso(file_last)} "
                    f"!= shared.last_ms={ms_to_iso(shared.last_ms)}"
                )

        if not reasons:
            shared.last_scan_kind = "tail"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = f"last {len(in_ms)} rows OK"
            log("ok", f"tail OK — last [bold]{len(in_ms)}[/bold] rows, "
                      f"last=[cyan]{ms_to_iso(max_in)}[/cyan]")
            return True

        console.print(Rule(style="yellow"))
        log("warn", "[bold]TAIL INCONSISTENCY[/bold]:")
        for r in reasons:
            log("warn", f"  • {r}")

        shared.last_scan_kind = "tail"
        shared.last_scan_ok = False
        shared.last_scan_ts = time.time()
        shared.last_scan_detail = "; ".join(reasons)[:200]

    with shared.lock:
        recompute_from_ohlc(in_path, out_path, state_path,
                            rsi_len, sma_len, merged_cfg, "tail")
    return False


def integrity_check_full(in_path, out_path, state_path,
                         rsi_len, sma_len, merged_cfg,
                         interval_ms) -> bool:
    with shared.lock:
        in_utc, in_ms, in_close, in_high, in_low = read_ohlc_csv(in_path)
        out_rows, out_ms = read_rsi_csv_full(out_path)

        if not in_ms:
            shared.last_scan_kind = "full"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = "empty OHLC CSV"
            log("full", "OHLC CSV empty; nothing to verify.")
            return True

        reasons = []

        seq_reason = check_sequence(in_ms, interval_ms)
        if seq_reason:
            reasons.append(f"OHLC sequence: {seq_reason}")

        if len(out_ms) != len(in_ms):
            reasons.append(
                f"output rows {len(out_ms)} != OHLC rows {len(in_ms)}"
            )
        else:
            mismatches = 0
            first_bad = None
            for i in range(len(in_ms)):
                if out_ms[i] != in_ms[i]:
                    mismatches += 1
                    if first_bad is None:
                        first_bad = (i, in_ms[i], out_ms[i])
            if mismatches:
                i, exp, got = first_bad
                reasons.append(
                    f"output time mismatch at row {i}: "
                    f"expected {ms_to_iso(exp)}, got {ms_to_iso(got)}; "
                    f"{mismatches} total mismatches"
                )

        max_in = in_ms[-1]
        if shared.last_ms != max_in:
            reasons.append(
                f"shared.last_ms={ms_to_iso(shared.last_ms)} "
                f"!= OHLC max={ms_to_iso(max_in)}"
            )

        loaded = load_state(state_path, merged_cfg)
        if loaded is None:
            reasons.append("state file missing or unreadable")
        else:
            file_last, _, _ = loaded
            if file_last != shared.last_ms:
                reasons.append(
                    f"state file last={ms_to_iso(file_last)} "
                    f"!= shared.last_ms={ms_to_iso(shared.last_ms)}"
                )

        if not reasons:
            shared.last_scan_kind = "full"
            shared.last_scan_ok = True
            shared.last_scan_ts = time.time()
            shared.last_scan_detail = f"{len(in_ms)} rows OK"
            log("ok", f"FULL scan OK — [bold]{len(in_ms)}[/bold] rows, "
                      f"last=[cyan]{ms_to_iso(max_in)}[/cyan]")
            return True

        console.print(Rule(style="yellow"))
        log("warn", "[bold]FULL-SCAN INCONSISTENCY[/bold]:")
        for r in reasons:
            log("warn", f"  • {r}")

        shared.last_scan_kind = "full"
        shared.last_scan_ok = False
        shared.last_scan_ts = time.time()
        shared.last_scan_detail = "; ".join(reasons)[:200]

    with shared.lock:
        recompute_from_ohlc(in_path, out_path, state_path,
                            rsi_len, sma_len, merged_cfg, "full")
    return False


# ==================== INTEGRITY THREAD ====================
def integrity_thread(in_path, out_path, state_path,
                     rsi_len, sma_len, merged_cfg, interval_ms):
    tick = DEFAULTS["integrity_tick_seconds"]
    tail_every = DEFAULTS["tail_check_seconds"]
    full_every = DEFAULTS["full_check_seconds"]

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

        if now >= next_full:
            try:
                integrity_check_full(in_path, out_path, state_path,
                                     rsi_len, sma_len, merged_cfg, interval_ms)
            except Exception as e:
                log("err", f"full scan error: {e}")
            next_full = time.time() + full_every
            next_tail = time.time() + tail_every
            continue

        if now >= next_tail:
            try:
                integrity_check_tail(in_path, out_path, state_path,
                                     rsi_len, sma_len, merged_cfg, interval_ms)
            except Exception as e:
                log("err", f"tail scan error: {e}")
            next_tail = time.time() + tail_every


# ==================== MAIN LOOP ====================
def main():
    global shared

    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=DEFAULTS["input_csv"])
    p.add_argument("--output", default=DEFAULTS["output_csv"])
    p.add_argument("--state", default=DEFAULTS["state_json"])
    p.add_argument("--length", type=int, default=DEFAULTS["rsi_length"])
    p.add_argument("--sma", type=int, default=DEFAULTS["sma_length"])
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
        "bar_index_base": DEFAULTS["bar_index_base"],
        "inv_lookback": DEFAULTS["inv_lookback"],
        # pinescript.txt additions
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

    shared = Shared(args.length, args.sma, merged_cfg)

    ensure_output_header(out_path)

    console.print()
    print_banner(in_path, out_path, state_path, args)
    console.print(Rule(style="bright_black"))

    # ---- On boot: full recompute from the OHLC CSV ----
    log("boot", f"reading [cyan]{in_path}[/cyan] …")
    times_utc, times_ms, closes, highs, lows = read_ohlc_csv(in_path)
    log("boot", f"loaded [bold]{len(closes)}[/bold] bars")

    with shared.lock:
        if closes:
            with Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[bold cyan]computing RSI + MA + BB + LR + PINE-MA + FIB[/bold cyan]"),
                BarColumn(bar_width=None, complete_style="cyan",
                          finished_style="green"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console, transient=True,
            ) as prog:
                task = prog.add_task("compute", total=len(closes))
                rsi, rsi_ma, rsi_state, merged_out, merged_state = compute_full(
                    closes, highs, lows, args.length, args.sma, merged_cfg)
                prog.update(task, advance=len(closes))

            rows = []
            for i in range(len(closes)):
                rows.append(build_row(
                    times_utc[i], times_ms[i], closes[i], highs[i], lows[i],
                    rsi[i], rsi_ma[i], merged_out[i],
                ))
            write_output_csv(out_path, rows)
            shared.state = rsi_state
            shared.merged_state = merged_state
            shared.last_ms = times_ms[-1]
            save_state(state_path, shared.last_ms, shared.state,
                       shared.merged_state)

            log("boot", f"wrote [bold]{len(rows)}[/bold] rows → "
                        f"[cyan]{out_path}[/cyan]")
            log("boot", f"state saved: last_open_time_ms="
                        f"[cyan]{ms_to_iso(shared.last_ms)}[/cyan]")

            # Build display rows for the merged tail panel (unchanged view).
            display_rows = []
            for i in range(len(closes)):
                d = dict(merged_out[i])
                d["open_time_utc"] = times_utc[i]
                d["close"] = closes[i]
                display_rows.append(d)

            print_tail_panels(times_utc, closes, rsi, rsi_ma,
                              display_rows,
                              title_prefix="tail (computed at boot)")
        else:
            shared.state = PineRsiState(args.length, args.sma)
            shared.merged_state = MergedState(merged_cfg)
            shared.last_ms = 0
            log("warn", "no data yet; will poll until the CSV appears.")

    # ---- Start the integrity thread ----
    it = threading.Thread(
        target=integrity_thread,
        args=(in_path, out_path, state_path,
              args.length, args.sma, merged_cfg, DEFAULTS["interval_ms"]),
        daemon=True,
        name="integrity",
    )
    it.start()
    log("integrity",
        f"thread started; tail every "
        f"[bright_magenta]{DEFAULTS['tail_check_seconds'] / 60:.0f} min[/bright_magenta], "
        f"full every "
        f"[bright_magenta]{DEFAULTS['full_check_seconds'] / 60:.0f} min[/bright_magenta]")

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
            times_utc, times_ms, closes, highs, lows = read_ohlc_csv(in_path)
            if not closes:
                continue

            with shared.lock:
                max_ms = times_ms[-1]
                if shared.last_ms > max_ms:
                    log("watch", f"input CSV was reset "
                                 f"(last_ms={ms_to_iso(shared.last_ms)} > "
                                 f"max_ms={ms_to_iso(max_ms)}); "
                                 f"full recompute.")
                    rsi, rsi_ma, rsi_state, merged_out, merged_state = \
                        compute_full(closes, highs, lows,
                                     args.length, args.sma, merged_cfg)
                    rows = []
                    for i in range(len(closes)):
                        rows.append(build_row(
                            times_utc[i], times_ms[i],
                            closes[i], highs[i], lows[i],
                            rsi[i], rsi_ma[i], merged_out[i],
                        ))
                    write_output_csv(out_path, rows)
                    shared.state = rsi_state
                    shared.merged_state = merged_state
                    shared.last_ms = times_ms[-1]
                    save_state(state_path, shared.last_ms,
                               shared.state, shared.merged_state)

                    display_rows = []
                    for i in range(len(closes)):
                        d = dict(merged_out[i])
                        d["open_time_utc"] = times_utc[i]
                        d["close"] = closes[i]
                        display_rows.append(d)
                    print_tail_panels(times_utc, closes, rsi, rsi_ma,
                                      display_rows,
                                      title_prefix="tail (after reset recompute)")
                    continue

                # Incremental.
                new_indices = [i for i, t in enumerate(times_ms)
                               if t > shared.last_ms]
                if new_indices:
                    rows_to_append = []
                    display_new = []
                    for i in new_indices:
                        close = closes[i]
                        high = highs[i] if i < len(highs) else close
                        low = lows[i] if i < len(lows) else close

                        rsi, rsi_ma = shared.state.step(close)
                        merged = shared.merged_state.step(high, low, close)

                        rows_to_append.append(build_row(
                            times_utc[i], times_ms[i],
                            close, high, low,
                            rsi, rsi_ma, merged,
                        ))
                        d = dict(merged)
                        d["open_time_utc"] = times_utc[i]
                        d["close"] = close
                        display_new.append(d)

                        prev_close = closes[i - 1] if i > 0 else None
                        line = Text()
                        line.append("  + ", style="bright_black")
                        line.append(times_utc[i], style="time")
                        line.append("  close=")
                        line.append_text(close_text(prev_close, close))
                        line.append("  rsi=")
                        line.append_text(rsi_text(rsi))
                        line.append("  ma=")
                        line.append_text(rsi_text(rsi_ma))
                        console.print(line)

                    if rows_to_append:
                        append_output_rows(out_path, rows_to_append)
                        shared.last_ms = times_ms[-1]
                        save_state(state_path, shared.last_ms,
                                   shared.state, shared.merged_state)

                        # Refresh both tail tables from the merged file.
                        o_rows, o_ms = read_rsi_csv_tail(out_path, 10)
                        if o_rows:
                            t_utc = []
                            t_close = []
                            t_rsi = []
                            t_ma = []
                            t_merged = []
                            for r in o_rows:
                                t_utc.append(r.get("open_time_utc", ""))
                                try:
                                    t_close.append(float(r.get("close", "")))
                                except (TypeError, ValueError):
                                    t_close.append(None)
                                try:
                                    t_rsi.append(float(r.get("rsi", "")))
                                except (TypeError, ValueError):
                                    t_rsi.append(None)
                                try:
                                    t_ma.append(float(r.get("rsi_ma50", "")))
                                except (TypeError, ValueError):
                                    t_ma.append(None)
                                t_merged.append(r)
                            print_tail_panels(t_utc, t_close, t_rsi, t_ma,
                                              t_merged,
                                              title_prefix="tail (last 10 bars)")

            if time.time() - last_heartbeat > DEFAULTS["heartbeat_seconds"]:
                last_heartbeat = time.time()
                console.print(Rule(style="bright_black"))
                log("hb", f"{ms_to_iso(int(time.time() * 1000))}  "
                          f"last_bar=[cyan]{ms_to_iso(shared.last_ms)}[/cyan]  "
                          f"bars_fed=[magenta]{shared.state.bars_fed}[/magenta]")
                console.print(Rule(style="bright_black"))

        except Exception as e:
            log("err", f"watch error: {e}")
            time.sleep(1.0)

    log("ok", "done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print()
        console.print("[bold red]interrupted.[/bold red]")