#!/usr/bin/env python3
# TradingView automation with human-like token cycling.
#
# Features:
# 1. Reads tokens from token_usdt.txt (default) or fetches positive gainers from Binance (fallback)
# 2. Opens TradingView in Firefox with human-like behavior
# 3. Cycles through tokens with randomized micro-behaviors
# 4. OS-level mouse control via pyautogui for realistic idle movement
# 5. Pure keyboard-driven input (types as if from a real keyboard)
# 6. (DISABLED) RSI/RSI_MA50 extraction from legend tokens
# 7. Rich terminal UI with live status, stats, and cycle summaries
# 8. Rainbow play: every 3rd token, random color; if orange, do timeframe sweep + Ctrl+Left navigation
# 9. Default chart interval is 10R (10-range bars) — configurable via "default_interval"

import os
import re
import sys
import json
import math
import random
import shutil
import signal
import logging
import traceback
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from collections import deque

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import (
    WebDriverException, NoSuchWindowException,
    InvalidSessionIdException, TimeoutException, NoSuchElementException,
)

# Optional OS-level mouse control (best-effort import)
try:
    import pyautogui
    pyautogui.FAILSAFE = False
    pyautogui.PAUSE = 0
    _PYAUTOGUI_AVAILABLE = True
except Exception:
    pyautogui = None
    _PYAUTOGUI_AVAILABLE = False

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn,
    TimeElapsedColumn, TimeRemainingColumn,
)
from rich import box
from rich.align import Align

console = Console()


# ==================== ARGPARSE / CONFIG ====================
def parse_args():
    p = argparse.ArgumentParser(
        description="TradingView human-like token cycler",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--wait-login", action="store_true",
                   help="Pause before starting so you can log in manually.")
    p.add_argument("--no-keep", action="store_true",
                   help="Do not restart on crash.")
    p.add_argument("--debug", action="store_true",
                   help="Enable debug logging.")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch + display tokens but do not touch the browser.")
    p.add_argument("--config", type=str, default=None,
                   help="Path to JSON config file (overrides defaults).")
    p.add_argument("--min-wait", type=float, default=None,
                   help="Override minimum wait between tokens (seconds).")
    p.add_argument("--max-wait", type=float, default=None,
                   help="Override maximum wait between tokens (seconds).")
    p.add_argument("--tokens-file", type=str, default=None,
                   help="Override tokens file path.")
    p.add_argument("--no-human", action="store_true",
                   help="Disable human-like mouse/typing behavior.")
    p.add_argument("--recycle-every", type=int, default=5000,
                   help="Proactively recycle WebDriver session every N polls.")
    p.add_argument("--max-tokens", type=int, default=0,
                   help="Cap the number of tokens cycled per round (0 = no cap).")
    return p.parse_args()


ARGS = parse_args()


CONFIG = {
    "firefox_path": "/usr/bin/firefox-esr",
    "geckodriver_path": None,
    "user_data_dir": str(Path.home() / ".config" / "firefox-esr-tradingview-automation"),
    "url": "https://www.tradingview.com/chart/zFljDA5X/?symbol=BINANCE%3AZECUSDT",
    "timeout": 45,
    "log_dir": str(Path.home() / ".local" / "logs" / "tradingview-automation"),
    "retry_delay": 5,
    "keep_browser_open": True,
    "auto_click_signin": False,
    "restart_on_crash": True,

    # [DISABLED] JSON output / RSI extraction
    "output_json": "tv_data.json",
    "json_heartbeat": 5.0,
    "study_name": "RSI_Top_Bottom",

    "poll_interval": 1.0,
    "verbose_first_seconds": 30,

    # Token cycling
    "tokens_file": "token_usdt.txt",          # <-- default local token file
    "min_wait_between_tokens": 17.0,
    "max_wait_between_tokens": 37.0,
    "binance_fetch_retry_delay": 30,
    "max_binance_retries": 10,
    "max_tokens_per_round": 0,

    # Default chart interval. TradingView accepts "10R" for 10-range bars.
    # Regular time intervals use plain digits ("1", "5", "15", "60", "240", "D", "W").
    "default_interval": "10R",

    # Human behavior
    "human_movement_enabled": True,
    "mouse_wander_probability": 0.35,
    "click_wander_probability": 0.15,
    "use_pyautogui": True,
    "micro_burst_probability": 0.7,

    # Session hygiene
    "recycle_every": 5000,
    "detect_login": True,

    # [DISABLED] legend polling for RSI
    "legend_polling_enabled": False,

    # Timing / behavior tuning
    "human_wait_mu": math.log(23.0),
    "human_wait_sigma": 0.28,

    # Micro-behavior tuning
    "stare_min": 2.0,
    "stare_max": 8.0,
    "small_pause_min": 0.3,
    "small_pause_max": 1.2,

    # Chart load settle
    "min_chart_settle_seconds": 5.0,
    "chart_ready_timeout": 6.0,
    "symbol_switch_timeout": 12.0,

    # Rainbow play
    "rainbow_play_every": 3,                  # run the play every Nth token
    "rainbow_colors": ["red", "orange", "yellow", "green", "blue", "indigo", "violet"],
    "rainbow_orange_pause_min": 60.0,
    "rainbow_orange_pause_max": 99.0,
    "rainbow_sweep_intervals": ["60", "45", "30", "15", "5", "3", "1"],
    "rainbow_ctrl_left_min_pause": 7.0,
    "rainbow_ctrl_left_max_pause": 10.0,
    "rainbow_ctrl_left_repeats_min": 3,
    "rainbow_ctrl_left_repeats_max": 4,
}


def apply_cli_overrides():
    if ARGS.no_keep:
        CONFIG["keep_browser_open"] = False
    if ARGS.debug:
        CONFIG["debug"] = True
    if ARGS.min_wait is not None:
        CONFIG["min_wait_between_tokens"] = ARGS.min_wait
    if ARGS.max_wait is not None:
        CONFIG["max_wait_between_tokens"] = ARGS.max_wait
    if ARGS.tokens_file:
        CONFIG["tokens_file"] = ARGS.tokens_file
    if ARGS.no_human:
        CONFIG["human_movement_enabled"] = False
        CONFIG["use_pyautogui"] = False
    if ARGS.recycle_every:
        CONFIG["recycle_every"] = ARGS.recycle_every
    if ARGS.max_tokens:
        CONFIG["max_tokens_per_round"] = ARGS.max_tokens
    if ARGS.config:
        try:
            with open(ARGS.config) as f:
                user_cfg = json.load(f)
            CONFIG.update(user_cfg)
            console.print(f"[dim]Loaded config from {ARGS.config}[/dim]")
        except Exception as e:
            console.print(f"[yellow]Could not load config {ARGS.config}: {e}[/yellow]")


# ==================== LOGGING ====================
LOG_LEVEL = logging.DEBUG if ARGS.debug else logging.INFO


def setup_logging():
    log_dir = Path(CONFIG["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"tv_legend_hook_{datetime.now():%Y%m%d}.log"
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


logger = setup_logging()
keep_running = True


def signal_handler(signum, frame):
    global keep_running
    logger.info("Received shutdown signal. Cleaning up...")
    keep_running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def human_delay(a=0.5, b=2.0):
    time.sleep(random.uniform(a, b))


def setup_directories():
    Path(CONFIG["log_dir"]).mkdir(parents=True, exist_ok=True)
    Path(CONFIG["user_data_dir"]).mkdir(parents=True, exist_ok=True)


# ==================== STATS ====================
class Stats:
    """Per-round and lifetime statistics for the UI."""

    def __init__(self):
        self.reset_round()

    def reset_round(self):
        self.tokens_attempted = 0
        self.tokens_succeeded = 0
        self.tokens_failed = 0
        self.failed_tokens: List[str] = []
        self.intervals_set_ok = 0
        self.intervals_set_fail = 0
        self.round_started = time.time()
        self.per_token_times: List[float] = []

    def add_attempt(self):
        self.tokens_attempted += 1

    def add_success(self, elapsed: float):
        self.tokens_succeeded += 1
        self.per_token_times.append(elapsed)

    def add_failure(self, token: str):
        self.tokens_failed += 1
        self.failed_tokens.append(token)

    @property
    def round_elapsed(self) -> float:
        return time.time() - self.round_started

    @property
    def avg_token_time(self) -> Optional[float]:
        if not self.per_token_times:
            return None
        return sum(self.per_token_times) / len(self.per_token_times)


STATS = Stats()


# ==================== TOKEN SOURCE ====================
STABLECOINS = {"USDC", "FDUSD", "TUSD", "DAI", "USDP", "USDE", "PYUSD", "EUR", "BUSD"}


def fetch_positive_gainers() -> List[Dict[str, Any]]:
    """Fallback: fetch positive 24h gainers from Binance (USDT pairs, non-stablecoin)."""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        tickers = response.json()

        positive_tokens = []
        for ticker in tickers:
            symbol = ticker["symbol"]
            price_change_percent = float(ticker["priceChangePercent"])
            if price_change_percent > 0 and symbol.endswith("USDT"):
                base_asset = symbol[:-4]
                if base_asset in STABLECOINS:
                    continue
                positive_tokens.append({
                    "symbol": symbol,
                    "change_percent": price_change_percent,
                    "last_price": ticker["lastPrice"],
                })

        positive_tokens.sort(key=lambda x: x["change_percent"], reverse=True)
        return positive_tokens
    except Exception as e:
        logger.error(f"Error fetching data from Binance: {e}")
        return []


def save_tokens_to_file(tokens: List[Dict[str, Any]], file_path: str) -> bool:
    try:
        with open(file_path, "w") as f:
            for token in tokens:
                f.write(f"{token['symbol']}\n")
        return True
    except Exception as e:
        logger.error(f"Error saving to {file_path}: {e}")
        return False


def load_tokens_from_file(file_path: str) -> List[str]:
    """Read tokens from a local file, one per line. Normalizes case and strips whitespace."""
    try:
        p = Path(file_path)
        if not p.exists():
            return []
        raw = p.read_text()
        tokens = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        # Normalize to uppercase so downstream matching is consistent
        return [t.upper() for t in tokens]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return []


def fetch_and_save_tokens_with_retry() -> List[str]:
    """
    Returns the token list for this round.

    Priority:
      1. Local tokens file (token_usdt.txt). If it exists and has content, use it
         as-is (case-normalized). NO network call.
      2. Binance fallback: fetch positive gainers, save to the tokens file, and use
         the freshly fetched list.
    """
    tokens_file = CONFIG["tokens_file"]

    # --- Primary: local file ---
    if Path(tokens_file).exists():
        local_tokens = load_tokens_from_file(tokens_file)
        if local_tokens:
            console.print(Panel.fit(
                f"[bold green]✓[/bold green] Loaded [bold cyan]{len(local_tokens)}[/bold cyan] tokens "
                f"from [yellow]{tokens_file}[/yellow]\n"
                f"[dim]Binance fetch skipped (local file is the primary source).[/dim]",
                border_style="green",
                title="Local Token File",
            ))
            return local_tokens
        else:
            console.print(Panel.fit(
                f"[bold yellow]⚠[/bold yellow] [yellow]{tokens_file}[/yellow] exists but is empty.\n"
                f"[dim]Falling back to Binance fetch...[/dim]",
                border_style="yellow",
                title="Local Token File Empty",
            ))
    else:
        console.print(Panel.fit(
            f"[bold yellow]⚠[/bold yellow] [yellow]{tokens_file}[/yellow] not found.\n"
            f"[dim]Falling back to Binance fetch...[/dim]",
            border_style="yellow",
            title="Local Token File Missing",
        ))

    # --- Fallback: Binance ---
    retries = 0
    max_retries = CONFIG["max_binance_retries"]

    while keep_running and retries < max_retries:
        with console.status("[bold cyan]Fetching positive gainers from Binance...", spinner="dots"):
            tokens = fetch_positive_gainers()

        if tokens:
            if save_tokens_to_file(tokens, tokens_file):
                console.print(Panel.fit(
                    f"[bold green]✓[/bold green] Fetched [bold cyan]{len(tokens)}[/bold cyan] positive gainers\n"
                    f"[dim]Saved to[/dim] [yellow]{tokens_file}[/yellow]",
                    border_style="green",
                    title="Binance Fallback",
                ))
                return [t["symbol"] for t in tokens]
            else:
                console.print("[bold red]✗ Failed to save tokens to file[/bold red]")
        else:
            console.print(
                f"[bold yellow]⚠ No positive gainers found "
                f"(attempt {retries + 1}/{max_retries})[/bold yellow]"
            )

        retries += 1
        if retries < max_retries and keep_running:
            console.print(
                f"[dim]Waiting {CONFIG['binance_fetch_retry_delay']}s before retry...[/dim]"
            )
            time.sleep(CONFIG["binance_fetch_retry_delay"])

    return []


def display_tokens_table(tokens: List[Dict[str, Any]],
                         title: str = "Binance Positive Gainers (USDT)"):
    target_width = int(console.width * (2 / 3))
    table = Table(
        title=title,
        show_header=True,
        header_style="bold cyan",
        width=target_width,
        box=box.ROUNDED,
    )
    table.add_column("#", justify="right", style="dim", width=6)
    table.add_column("Token Pair", justify="left", style="bold white", ratio=2)
    table.add_column("24h Change", justify="right", ratio=2)
    table.add_column("Last Price", justify="right", style="yellow", ratio=2)

    for idx, token in enumerate(tokens, 1):
        formatted_change = f"[bold green]+{token['change_percent']:.2f}%[/bold green]"
        table.add_row(
            str(idx),
            token["symbol"],
            formatted_change,
            str(token["last_price"]),
        )
    console.print(table)


# ==================== PROFILE PREP ====================
FIREFOX_PREFS = {
    "dom.webdriver.enabled": False,
    "useAutomationExtension": False,
    "webgl.disabled": False,
    "webgl.force-enabled": True,
    "layers.acceleration.force-enabled": True,
    "toolkit.legacyUserProfileCustomizations.stylesheets": True,
    "browser.aboutConfig.showWarning": False,
    "browser.sessionstore.resume_from_crash": True,
    "layout.css.devPixelsPerPx": "0.67",
    "browser.startup.homepage_override.mstone": "ignore",
    "startup.homepage_welcome_url": "",
    "startup.homepage_welcome_url.additional": "",
    "browser.shell.checkDefaultBrowser": False,
    "browser.startup.page": 0,
    "browser.startup.homepage": "about:blank",
    "datareporting.policy.dataSubmissionEnabled": False,
    "datareporting.healthreport.uploadEnabled": False,
    "toolkit.telemetry.enabled": False,
    "app.update.auto": False,
    "app.update.enabled": False,
}


def prepare_profile_files():
    profile = Path(CONFIG["user_data_dir"])
    profile.mkdir(parents=True, exist_ok=True)
    for lockname in ("lock", ".parentlock", "parent.lock"):
        (profile / lockname).unlink(missing_ok=True)
    user_js = profile / "user.js"
    lines = [f'user_pref("{k}", {json.dumps(v)});' for k, v in FIREFOX_PREFS.items()]
    user_js.write_text("\n".join(lines) + "\n")
    chrome_dir = profile / "chrome"
    chrome_dir.mkdir(exist_ok=True)
    (chrome_dir / "userChrome.css").write_text(
        "#remote-control-indicator,\n#remote-control-box {\n    display: none !important;\n}\n"
    )


# ==================== HUMAN BEHAVIORS ====================
class HumanMouse:
    """
    Human-like mouse movement.

    Two layers:
      - Selenium ActionChains: used for element-targeted moves (reliable).
      - pyautogui (optional): OS-level cursor moves for idle wander, which
        look much more human to bot detectors since they move the real
        cursor, not just Selenium's internal pointer.
    """

    def __init__(self, driver: webdriver.Firefox):
        self.driver = driver
        self.actions = ActionChains(driver)
        self._last_pos = (random.randint(200, 600), random.randint(200, 400))
        self._pyauto_ok = (
            _PYAUTOGUI_AVAILABLE and CONFIG.get("use_pyautogui", True)
            and CONFIG["human_movement_enabled"]
        )

    # --- Bezier helpers ---
    def _bezier_curve(self, p0, p1, p2, p3, steps=20):
        pts = []
        for i in range(steps + 1):
            t = i / steps
            x = ((1 - t) ** 3 * p0[0] + 3 * (1 - t) ** 2 * t * p1[0]
                 + 3 * (1 - t) * t ** 2 * p2[0] + t ** 3 * p3[0])
            y = ((1 - t) ** 3 * p0[1] + 3 * (1 - t) ** 2 * t * p1[1]
                 + 3 * (1 - t) * t ** 2 * p2[1] + t ** 3 * p3[1])
            pts.append((int(x), int(y)))
        return pts

    def _random_control_points(self, start, end):
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        dist = math.sqrt(dx * dx + dy * dy) or 1.0
        perp_x = -dy / dist
        perp_y = dx / dist
        off1 = random.uniform(-dist * 0.3, dist * 0.3)
        off2 = random.uniform(-dist * 0.3, dist * 0.3)
        cp1 = (int(start[0] + dx * 0.33 + perp_x * off1),
               int(start[1] + dy * 0.33 + perp_y * off1))
        cp2 = (int(start[0] + dx * 0.66 + perp_x * off2),
               int(start[1] + dy * 0.66 + perp_y * off2))
        return cp1, cp2

    # --- Targeted move (Selenium) ---
    def move_to(self, target_x: int, target_y: int):
        if not CONFIG["human_movement_enabled"]:
            try:
                body = self.driver.find_element(By.TAG_NAME, "body")
                self.actions.move_to_element_with_offset(body, target_x, target_y).perform()
            except Exception:
                pass
            return

        start = self._last_pos
        cp1, cp2 = self._random_control_points(start, (target_x, target_y))
        steps = random.randint(15, 30)
        path = self._bezier_curve(start, cp1, cp2, (target_x, target_y), steps)

        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            actions = ActionChains(self.driver)
            for i, (px, py) in enumerate(path):
                if i == 0:
                    actions.move_to_element_with_offset(body, px, py)
                else:
                    actions.move_by_offset(px - path[i - 1][0], py - path[i - 1][1])
                if random.random() < 0.1:
                    actions.pause(random.uniform(0.01, 0.05))
            actions.perform()
        except Exception as e:
            logger.debug(f"move_to failed: {e}")

        self._last_pos = (target_x, target_y)
        time.sleep(random.uniform(0.1, 0.3))

    # --- OS-level wander (pyautogui) ---
    def _pyauto_relative_move(self, dx: int, dy: int):
        if not self._pyauto_ok:
            return False
        try:
            x, y = pyautogui.position()
            duration = random.uniform(0.2, 0.7)
            pyautogui.moveTo(x + dx, y + dy, duration=duration,
                             tween=pyautogui.easeInOutQuad)
            return True
        except Exception as e:
            logger.debug(f"pyautogui move failed: {e}")
            return False

    def random_wander(self):
        if not CONFIG["human_movement_enabled"]:
            return
        if random.random() > CONFIG["mouse_wander_probability"]:
            return

        # 70% small nudges (realistic idle), 30% larger move
        if random.random() < CONFIG["micro_burst_probability"]:
            dx = random.randint(-80, 80)
            dy = random.randint(-80, 80)
        else:
            viewport = self.driver.execute_script(
                "return [window.innerWidth, window.innerHeight];"
            ) or [1200, 800]
            dx = random.randint(-int(viewport[0] * 0.3), int(viewport[0] * 0.3))
            dy = random.randint(-int(viewport[1] * 0.3), int(viewport[1] * 0.3))

        if not self._pyauto_relative_move(dx, dy):
            viewport = self.driver.execute_script(
                "return [window.innerWidth, window.innerHeight];"
            ) or [1200, 800]
            tx = max(50, min(viewport[0] - 50, self._last_pos[0] + dx))
            ty = max(50, min(viewport[1] - 50, self._last_pos[1] + dy))
            self.move_to(tx, ty)

        if random.random() < CONFIG["click_wander_probability"]:
            self._random_safe_click()

    def _random_safe_click(self):
        try:
            viewport = self.driver.execute_script(
                "return [window.innerWidth, window.innerHeight];"
            ) or [1200, 800]
            x = random.randint(int(viewport[0] * 0.3), int(viewport[0] * 0.7))
            y = random.randint(int(viewport[1] * 0.3), int(viewport[1] * 0.7))
            body = self.driver.find_element(By.TAG_NAME, "body")
            ActionChains(self.driver).move_to_element_with_offset(body, x, y).click().perform()
            logger.debug(f"Random safe click at ({x}, {y})")
            time.sleep(random.uniform(0.2, 0.5))
        except Exception as e:
            logger.debug(f"Random click failed: {e}")

    def scroll_randomly(self):
        if random.random() < 0.2:
            try:
                amount = random.randint(-100, 100)
                self.driver.execute_script(
                    "var el = document.querySelector('.chart-container') "
                    "|| document.scrollingElement; el.scrollTop += arguments[0];",
                    amount,
                )
                time.sleep(random.uniform(0.1, 0.3))
            except Exception:
                pass

    def right_click_context(self):
        try:
            viewport = self.driver.execute_script(
                "return [window.innerWidth, window.innerHeight];"
            ) or [1200, 800]
            x = random.randint(int(viewport[0] * 0.35), int(viewport[0] * 0.65))
            y = random.randint(int(viewport[1] * 0.35), int(viewport[1] * 0.65))
            body = self.driver.find_element(By.TAG_NAME, "body")
            ActionChains(self.driver).move_to_element_with_offset(
                body, x, y
            ).context_click().perform()
            time.sleep(random.uniform(0.3, 0.8))
            body.send_keys(Keys.ESCAPE)
        except Exception as e:
            logger.debug(f"Right-click context failed: {e}")

    def tab_focus_change(self):
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.send_keys(Keys.TAB)
            time.sleep(random.uniform(0.05, 0.15))
        except Exception:
            pass

    def zoom_nudge(self):
        try:
            zoom = random.choice(["105%", "95%", "100%"])
            self.driver.execute_script(f"document.body.style.zoom = '{zoom}'")
            time.sleep(random.uniform(0.2, 0.5))
        except Exception:
            pass


def human_type(element, text: str, min_delay: float = 0.05, max_delay: float = 0.15):
    """Human-like typing with burst typing, typos, and thinking pauses."""
    i = 0
    n = len(text)
    while i < n:
        burst = random.randint(2, 4)
        for _ in range(burst):
            if i >= n:
                break
            char = text[i]

            # 2% chance of typo on alpha chars
            if char.isalpha() and i < n - 1 and random.random() < 0.02:
                wrong = random.choice("abcdefghijklmnopqrstuvwxyz")
                element.send_keys(wrong)
                time.sleep(random.uniform(0.05, 0.15))
                element.send_keys(Keys.BACKSPACE)
                time.sleep(random.uniform(0.08, 0.20))

            element.send_keys(char)
            time.sleep(random.uniform(min_delay, max_delay))
            i += 1

        time.sleep(random.uniform(0.15, 0.45))

        if random.random() < 0.15:
            time.sleep(random.uniform(0.5, 1.2))


def human_wait_duration() -> float:
    """Log-normal-ish wait, clamped to configured bounds."""
    mu = CONFIG["human_wait_mu"]
    sigma = CONFIG["human_wait_sigma"]
    raw = random.lognormvariate(mu, sigma)
    lo = CONFIG["min_wait_between_tokens"]
    hi = CONFIG["max_wait_between_tokens"]
    return max(lo, min(hi, raw))


def human_like_idle(duration: Optional[float] = None):
    if duration is None:
        duration = random.uniform(1.0, 3.0)
    elapsed = 0.0
    while elapsed < duration and keep_running:
        chunk = min(random.uniform(0.3, 1.0), duration - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        if random.random() < 0.2:
            time.sleep(random.uniform(0.1, 0.3))


# ==================== PAGE-SIDE JS ====================
# [DISABLED] Legend scraping for RSI/RSI_MA50
FIND_CHART_LEGEND_JS = r"""
try {
    var hint = arguments[0];
    var candidates = document.querySelectorAll(
        '[class*="chart-gui-wrapper__legend"], [class*="legend-"][class*="noWrap-"]'
    );
    var out = [];
    for (var i = 0; i < candidates.length; i++) {
        var el = candidates[i];
        var t = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (t.indexOf(hint) < 0) continue;
        var anc = el;
        var isSettings = false;
        for (var d = 0; d < 6 && anc; d++) {
            if (anc.className && String(anc.className).indexOf('titlesWrapper') >= 0) {
                isSettings = true;
                break;
            }
            anc = anc.parentElement;
        }
        if (isSettings) continue;
        var tokens = [];
        function walk(node, depth) {
            if (!node || depth > 8) return;
            if (!node.children || node.children.length === 0) {
                var tt = (node.textContent || '').replace(/\s+/g, ' ').trim();
                if (tt) tokens.push(tt);
                return;
            }
            for (var k = 0; k < node.children.length; k++) walk(node.children[k], depth + 1);
        }
        walk(el, 0);
        out.push({
            cls: String(el.className || '').slice(0, 120),
            data_name: el.getAttribute ? el.getAttribute('data-name') : null,
            text: t.slice(0, 500),
            tokens: tokens
        });
        if (out.length >= 3) break;
    }
    return { ok: true, found: out.length, candidates: out };
} catch (e) {
    return { ok: false, error: String(e), stack: (e && e.stack) ? String(e.stack) : null };
}
"""


# ==================== BROWSER MANAGER ====================
class SystemFirefoxManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.driver: Optional[webdriver.Firefox] = None
        self.human_mouse: Optional[HumanMouse] = None

    def start(self):
        logger.info("Starting Firefox via geckodriver/Marionette...")
        prepare_profile_files()
        opts = Options()
        opts.binary_location = self.config["firefox_path"]
        opts.add_argument("-new-instance")
        opts.add_argument("-maximize")
        opts.page_load_strategy = "eager"
        opts.add_argument("-profile")
        opts.add_argument(self.config["user_data_dir"])
        driver_path = self.config["geckodriver_path"] or shutil.which("geckodriver")
        if not driver_path:
            for cand in ("/usr/local/bin/geckodriver", "/usr/bin/geckodriver",
                         "/usr/lib/firefox-geckodriver/geckodriver"):
                if os.path.isfile(cand):
                    driver_path = cand
                    break
        if not driver_path:
            raise FileNotFoundError(
                "geckodriver not found. sudo apt install firefox-geckodriver"
            )
        logger.info(f"Using geckodriver at: {driver_path}")
        self.driver = webdriver.Firefox(
            service=Service(executable_path=driver_path), options=opts
        )
        self.driver.maximize_window()
        self.driver.set_page_load_timeout(self.config["timeout"])
        self.human_mouse = HumanMouse(self.driver)
        logger.info("Firefox started.")
        return True

    def navigate_to_chart(self):
        try:
            logger.info(f"Loading: {self.config['url']}")
            self.driver.get(self.config["url"])
        except TimeoutException:
            logger.warning("Page load timed out (live sockets) - continuing anyway.")
        human_delay(1, 2)
        return True

    def _run_js(self, label: str, js: str, *args) -> Any:
        try:
            return self.driver.execute_script(js.strip(), *args)
        except WebDriverException as e:
            logger.warning(f"{label} failed: {e}")
            return None

    def wait_for_chart(self, timeout=90):
        deadline = time.time() + timeout
        last_diag = 0.0
        while time.time() < deadline and keep_running:
            state = self._run_js("wait_for_chart", """
                try {
                    var c = document.querySelector('div.chart-gui-wrapper');
                    var cv = document.querySelectorAll('canvas').length;
                    return { ready: document.readyState, canvas: cv, hasWrapper: !!c };
                } catch (e) { return { error: String(e) }; }
            """) or {}
            if state.get("hasWrapper") and (state.get("canvas") or 0) > 0:
                logger.info(f"Chart UI detected (canvas={state.get('canvas')}).")
                return True
            if time.time() - last_diag > 15:
                last_diag = time.time()
                logger.info(f"Waiting for chart UI: {state}")
            time.sleep(2)
        return False

    def _wait_for_chart_ready(self, timeout: float = 6.0) -> bool:
        """Wait until the chart canvas is present and not visually blank."""
        deadline = time.time() + timeout
        while time.time() < deadline and keep_running:
            state = self._run_js("chart_ready", """
                try {
                    var loading = document.querySelector(
                        '[class*="loading"], [data-name="loading"]'
                    );
                    if (loading && loading.offsetParent !== null) {
                        return { ready: false, reason: "loading-indicator" };
                    }
                    var c = document.querySelector('div.chart-gui-wrapper canvas');
                    if (!c) return { ready: false, reason: "no-canvas" };
                    var rect = c.getBoundingClientRect();
                    if (rect.width < 10 || rect.height < 10) {
                        return { ready: false, reason: "canvas-too-small" };
                    }
                    return { ready: true };
                } catch (e) {
                    return { ready: false, reason: "exception: " + String(e) };
                }
            """) or {}
            if state.get("ready"):
                return True
            time.sleep(0.4)
        logger.debug("Chart ready check timed out — proceeding anyway")
        return False

    # ---------------------------------------------------------------
    # [DISABLED] Legend lookup for RSI/RSI_MA50
    # ---------------------------------------------------------------
    # def find_chart_legend(self):
    #     return self._run_js("find_chart_legend", FIND_CHART_LEGEND_JS,
    #                         self.config["study_name"])

    def is_logged_in(self) -> bool:
        if not CONFIG.get("detect_login", True):
            return True
        result = self._run_js("is_logged_in", """
            try {
                if (document.querySelector('[data-name="header-toolbar-account"]')) return true;
                if (document.querySelector('[class*="avatar"]')) return true;
                if (document.querySelector('[data-name="header-user-menu-button"]')) return true;
                return false;
            } catch (e) { return false; }
        """)
        return bool(result)

    def get_symbol_interval(self) -> Dict[str, str]:
        data = self._run_js("get_symbol_interval", """
            var s = "", i = "";
            try {
                var e = document.querySelector('[data-name="toolbar-symbol-search"]');
                if (e) { s = e.textContent.trim(); }
            } catch (_) {}
            try {
                var b = document.querySelector('[data-name="toolbar-intervals"] button')
                        || document.querySelector('[data-name="toolbar-intervals"]');
                if (b) { i = b.textContent.trim(); }
            } catch (_) {}
            return {symbol: s, interval: i};
        """) or {}
        if not data.get("symbol"):
            try:
                m = re.search(r"[?&]symbol=([^&]+)", self.driver.current_url or "")
                if m:
                    data["symbol"] = m.group(1).replace("%3A", ":")
            except WebDriverException:
                pass
        return data

    def is_alive(self) -> bool:
        if not self.driver:
            return False
        try:
            _ = self.driver.current_url
            return True
        except (NoSuchWindowException, InvalidSessionIdException, WebDriverException):
            return False

    def cleanup(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except WebDriverException:
            pass
        logger.info("Browser cleaned up.")

    # ================================================================
    # KEYBOARD-FIRST INTERACTION
    # ================================================================

    def _focus_page(self):
        """Ensure the page body has keyboard focus, and no dialog is open."""
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            try:
                ActionChains(self.driver).move_to_element(body).click().perform()
            except Exception:
                pass
            try:
                body.send_keys(Keys.ESCAPE)
                time.sleep(0.15)
            except Exception:
                pass
            # Click on the chart canvas specifically — safest focus target
            try:
                canvas = self.driver.find_element(
                    By.CSS_SELECTOR, "div.chart-gui-wrapper canvas"
                )
                ActionChains(self.driver).move_to_element(canvas).click().perform()
                time.sleep(0.15)
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"_focus_page failed: {e}")

    def _read_interval_ui(self) -> str:
        """Read whatever the UI currently shows as the selected interval."""
        data = self._run_js("read_interval", """
            try {
                var act = document.querySelector(
                    '[data-name="toolbar-intervals"] button[aria-pressed="true"], ' +
                    '[data-name="toolbar-intervals"] button.active, ' +
                    '[data-name="toolbar-intervals"] button[data-active="true"]'
                );
                if (act) return (act.textContent || act.getAttribute('data-value') || '').trim();

                var pressed = document.querySelector('[data-name^="interval-"][aria-pressed="true"]');
                if (pressed) {
                    var dn = pressed.getAttribute('data-name') || '';
                    var m = dn.match(/interval-(.+)/);
                    if (m) return m[1];
                }

                var btn = document.querySelector('[data-name="toolbar-intervals"] button');
                if (btn) return (btn.textContent || '').trim();

                return "";
            } catch (e) { return ""; }
        """)
        return (data or "").strip()

    def _type_interval_keys(self, interval: str):
        """
        Type an interval string into the page as a human would.

        Handles:
          - Plain numeric intervals: "1", "5", "15", "60", "240"
          - Range intervals with 'R' suffix: "10R", "5R", etc.
          - Letter intervals: "D", "W"
        Each character is typed with a small human-like delay, then ENTER
        is pressed twice to commit.
        """
        body = self.driver.find_element(By.TAG_NAME, "body")
        for ch in str(interval):
            body.send_keys(ch)
            time.sleep(random.uniform(0.08, 0.18))
        time.sleep(random.uniform(0.25, 0.55))
        body.send_keys(Keys.RETURN)
        time.sleep(random.uniform(0.15, 0.30))
        body.send_keys(Keys.RETURN)

    def set_interval(self, interval: str = None) -> bool:
        """
        Set the chart interval by typing it exactly as a human would.
        TradingView accepts the interval keystroke at the document level.

        Examples:
          set_interval("10R")  -> 10-range bars
          set_interval("1")    -> 1-minute bars
          set_interval("60")   -> 1-hour bars
        """
        if interval is None:
            interval = CONFIG["default_interval"]

        logger.info(f"Setting interval to {interval} via keyboard...")

        self._focus_page()
        time.sleep(random.uniform(0.15, 0.35))

        try:
            self._type_interval_keys(interval)
            logger.info(f"Sent keyboard: '{interval}' + ENTER")
        except Exception as e:
            logger.warning(f"Keyboard interval input failed: {e}")
            STATS.intervals_set_fail += 1
            return False

        # Wait for the chart to settle at the new TF
        time.sleep(CONFIG["min_chart_settle_seconds"])
        self._wait_for_chart_ready(timeout=CONFIG["chart_ready_timeout"])

        # Soft verification — informational only
        ui_iv = self._read_interval_ui()
        if ui_iv and (ui_iv == interval
                      or ui_iv.upper() == interval.upper()):
            logger.info(f"Interval UI confirms: {ui_iv!r}")
        else:
            logger.warning(
                f"Interval UI shows {ui_iv!r} — keystrokes sent regardless. Continuing."
            )
        STATS.intervals_set_ok += 1
        return True

    def _wait_for_symbol(self, expected_pair: str, timeout=15.0,
                         min_wait=0.0) -> bool:
        """
        Soft check that the chart's active symbol matches `expected_pair`.
        Reads the URL and the toolbar. Returns True if either matches.
        """
        base = expected_pair.replace("USDT", "").upper()

        start = time.time()
        deadline = start + timeout
        confirmed_at = None

        while time.time() < deadline and keep_running:
            matched = False

            # URL check (?symbol=BINANCE:XXXUSDT)
            try:
                url = self.driver.current_url or ""
                m = re.search(r"[?&]symbol=([^&]+)", url)
                if m:
                    url_sym = m.group(1).replace("%3A", ":").upper()
                    if base in url_sym and "USDT" in url_sym:
                        matched = True
            except Exception:
                pass

            # Toolbar check
            if not matched:
                try:
                    data = self.get_symbol_interval()
                    sym = (data.get("symbol") or "").upper()
                    if base in sym and "USDT" in sym:
                        matched = True
                except Exception:
                    pass

            if matched:
                confirmed_at = time.time()
                break

            time.sleep(0.4)

        if confirmed_at is None:
            return False

        elapsed = confirmed_at - start
        if elapsed < min_wait:
            time.sleep(min_wait - elapsed)
        return True

    def search_and_select_token(self, token_pair: str) -> bool:
        """
        Select a token by typing the pair (e.g. 'BTCUSDT') directly into the
        page and pressing Enter. TradingView's global symbol input picks it
        up automatically — no prefix, no search box lookup.
        """
        logger.info(f"Searching for token: {token_pair}")

        # Clean focus state (dismiss overlays, focus the page)
        self._focus_page()
        time.sleep(random.uniform(0.15, 0.35))

        body = self.driver.find_element(By.TAG_NAME, "body")

        # Type the token pair exactly as it appears in tokens.txt
        try:
            human_type(body, token_pair, min_delay=0.03, max_delay=0.12)
        except Exception as e:
            logger.warning(f"Typing failed, retrying once: {e}")
            time.sleep(0.3)
            try:
                human_type(body, token_pair, min_delay=0.03, max_delay=0.12)
            except Exception as e2:
                logger.error(f"Typing failed twice: {e2}")
                STATS.add_failure(token_pair)
                return False

        # Human pause — "look at what appeared"
        time.sleep(random.uniform(0.8, 1.4))

        # Commit with ENTER (twice for safety)
        try:
            body.send_keys(Keys.RETURN)
            time.sleep(random.uniform(0.15, 0.30))
            body.send_keys(Keys.RETURN)
            logger.info(f"Sent keyboard: '{token_pair}' + ENTER")
        except Exception as e:
            logger.warning(f"ENTER commit failed: {e}")

        # Wait for chart settle (5s floor + DOM poll). Informational only.
        chart_ok = self._wait_for_symbol(
            token_pair,
            timeout=CONFIG["symbol_switch_timeout"],
            min_wait=CONFIG["min_chart_settle_seconds"],
        )
        if chart_ok:
            logger.info(f"Symbol confirmed: {token_pair}")
            self._wait_for_chart_ready(timeout=CONFIG["chart_ready_timeout"])
        else:
            logger.warning(
                f"Symbol DOM not confirmed as {token_pair}; continuing anyway."
            )
        return True

    def recycle_session(self):
        """Quit and restart the WebDriver session (for long-running stability)."""
        logger.info("Recycling WebDriver session...")
        try:
            self.cleanup()
        except Exception:
            pass
        time.sleep(2)
        self.start()
        self.navigate_to_chart()
        self.wait_for_chart(timeout=60)

    # ================================================================
    # RAINBOW PLAY
    # ================================================================

    def _send_interval_key(self, interval: str):
        """Focus the page, type an interval string, press Enter twice."""
        try:
            self._focus_page()
            time.sleep(random.uniform(0.15, 0.35))
            self._type_interval_keys(interval)
        except Exception as e:
            logger.debug(f"_send_interval_key({interval}) failed: {e}")

    def _send_ctrl_left(self, repeat: int):
        """Send Ctrl+Left `repeat` times with a random pause between each."""
        lo = CONFIG["rainbow_ctrl_left_min_pause"]
        hi = CONFIG["rainbow_ctrl_left_max_pause"]
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
        except Exception:
            return
        for i in range(repeat):
            if not keep_running:
                return
            try:
                ActionChains(self.driver).key_down(Keys.CONTROL).send_keys(Keys.LEFT).key_up(
                    Keys.CONTROL
                ).perform()
            except Exception as e:
                logger.debug(f"Ctrl+Left #{i + 1} failed: {e}")
            pause = random.uniform(lo, hi)
            console.print(f"[dim]  ctrl+left {i + 1}/{repeat} — pausing {pause:.1f}s[/dim]")
            time.sleep(pause)

    def rainbow_play(self, token: str):
        """
        Every Nth token, pick a random rainbow color. If 'orange' is drawn,
        perform the multi-timeframe sweep and Ctrl+Left navigation.
        """
        colors = CONFIG["rainbow_colors"]
        picked = random.choice(colors)
        console.print(Panel.fit(
            f"[bold]Rainbow play[/bold] on [cyan]{token}[/cyan]\n"
            f"[dim]Drew:[/dim] [bold magenta]{picked.upper()}[/bold magenta]",
            border_style="magenta",
            title="🎨 Rainbow Play",
        ))

        if picked != "orange":
            console.print("[dim]  Not orange — skipping sweep, continuing normally.[/dim]")
            return

        console.print(Panel.fit(
            f"[bold orange1]ORANGE drawn![/bold orange1]\n"
            f"[dim]Timeframe sweep: 60 → 45 → 30 → 15 → 5 → 3 → 1[/dim]",
            border_style="orange1",
            title="🎨 Orange Sweep",
        ))

        # 1) Random pre-sweep pause
        pre_pause = random.uniform(
            CONFIG["rainbow_orange_pause_min"],
            CONFIG["rainbow_orange_pause_max"],
        )
        console.print(f"[dim]  Pre-sweep pause: {pre_pause:.1f}s[/dim]")
        _sleep_with_heartbeat(pre_pause)

        # 2) Sweep intervals: 60, 45, 30, 15, 5, 3, 1
        for iv in CONFIG["rainbow_sweep_intervals"]:
            if not keep_running:
                return
            console.print(f"[cyan]  → Switching to {iv}m[/cyan]")
            self._send_interval_key(iv)
            time.sleep(CONFIG["min_chart_settle_seconds"])
            self._wait_for_chart_ready(timeout=CONFIG["chart_ready_timeout"])

        # 3) Ctrl+Left navigation, 3-4 times with 7-10s pauses
        repeats = random.randint(
            CONFIG["rainbow_ctrl_left_repeats_min"],
            CONFIG["rainbow_ctrl_left_repeats_max"],
        )
        console.print(
            f"[cyan]  → Ctrl+Left navigation × {repeats} "
            f"(pausing {CONFIG['rainbow_ctrl_left_min_pause']:.0f}-"
            f"{CONFIG['rainbow_ctrl_left_max_pause']:.0f}s)[/cyan]"
        )
        self._send_ctrl_left(repeats)

        console.print("[green]  ✓ Rainbow play complete — resuming normal cycle[/green]")


def _sleep_with_heartbeat(total: float, chunk_max: float = 5.0):
    """Sleep `total` seconds, printing a tiny heartbeat every few seconds."""
    elapsed = 0.0
    while elapsed < total and keep_running:
        chunk = min(chunk_max, total - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        remaining = total - elapsed
        if remaining > 0:
            console.print(f"[dim]    …{remaining:.0f}s remaining[/dim]")


# ==================== PARSING ====================
# ---------------------------------------------------------------
# [DISABLED] RSI / RSI_MA50 extraction
# ---------------------------------------------------------------
# _NUM_RE = re.compile(r"^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^-?\d+(?:\.\d+)?$")
#
# def _to_float(tok: str) -> Optional[float]:
#     try:
#         return float(tok.replace(",", ""))
#     except ValueError:
#         return None
#
# def parse_legend_tokens(tokens):
#     toks = [t.strip() for t in tokens if t and t.strip()]
#     nums = []
#     for t in toks:
#         if _NUM_RE.match(t):
#             v = _to_float(t)
#             if v is not None:
#                 nums.append(v)
#     rsi_val = ma_val = None
#     if len(nums) >= 3:
#         rsi_val, ma_val = nums[-3], nums[-2]
#     elif len(nums) == 2:
#         rsi_val, ma_val = nums[-2], nums[-1]
#     elif len(nums) == 1:
#         rsi_val = nums[-1]
#     return rsi_val, ma_val


# ==================== CONTEXT ====================
def new_ctx() -> Dict[str, Any]:
    return {
        "poll_count": 0,
        # [DISABLED] "rsi": None, "rsi_ma50": None,
        "verbose_until": 0.0,
        "legend_failure_logged": False,
        "last_tail": None,
        "last_full_tokens": None,
        "current_token": None,
        "tokens_processed": 0,
        "tokens_total": 0,
        "round_index": 0,
    }


def ts_to_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------
# [DISABLED] JSON output (RSI / RSI_MA50)
# ---------------------------------------------------------------
# def build_output(ctx, sym): ...
# def format_line(d): ...
# def write_json(path, data): ...


# ==================== LIVE STATUS UI ====================
def build_status_panel(ctx: Dict[str, Any]) -> Panel:
    """A live panel showing current progress + stats."""
    total = ctx.get("tokens_total") or 0
    done = ctx.get("tokens_processed") or 0
    current = ctx.get("current_token") or "—"

    stat_table = Table.grid(padding=(0, 2))
    stat_table.add_column(justify="right", style="dim", no_wrap=True)
    stat_table.add_column(justify="left", style="bold")

    stat_table.add_row("Round:", str(ctx.get("round_index", 0)))
    stat_table.add_row("Progress:", f"[cyan]{done}[/cyan] / [cyan]{total}[/cyan]")
    stat_table.add_row("Current:", f"[bold white]{current}[/bold white]")
    stat_table.add_row("Succeeded:", f"[green]{STATS.tokens_succeeded}[/green]")
    stat_table.add_row("Failed:", f"[red]{STATS.tokens_failed}[/red]")
    stat_table.add_row("Interval OK:", f"[green]{STATS.intervals_set_ok}[/green]")
    stat_table.add_row("Interval fail:", f"[red]{STATS.intervals_set_fail}[/red]")

    avg = STATS.avg_token_time
    stat_table.add_row("Avg/token:", f"{avg:.1f}s" if avg else "—")
    stat_table.add_row("Elapsed:", f"{STATS.round_elapsed:.0f}s")

    return Panel(
        Align.left(stat_table),
        title="[bold cyan]Live Status[/bold cyan]",
        border_style="cyan",
        box=box.ROUNDED,
    )


# ==================== TOKEN CYCLING ====================
def _micro_behavior_during_wait(mgr: SystemFirefoxManager, ctx: Dict[str, Any],
                                elapsed: float, total: float):
    """One 'tick' of stochastic user behavior during the token wait."""
    remaining = total - elapsed
    if remaining <= 0:
        return 0.0

    chunk = random.uniform(CONFIG["stare_min"], CONFIG["stare_max"])
    chunk = min(chunk, remaining)

    roll = random.random()
    if roll < 0.55:
        time.sleep(chunk)
    elif roll < 0.75:
        if mgr.human_mouse:
            mgr.human_mouse.random_wander()
        time.sleep(chunk * 0.6)
    elif roll < 0.85:
        if mgr.human_mouse:
            mgr.human_mouse.scroll_randomly()
        time.sleep(chunk * 0.5)
    elif roll < 0.92:
        if mgr.human_mouse:
            mgr.human_mouse.tab_focus_change()
        time.sleep(chunk * 0.4)
    elif roll < 0.97:
        if mgr.human_mouse:
            mgr.human_mouse.right_click_context()
        time.sleep(chunk * 0.6)
    else:
        if mgr.human_mouse:
            mgr.human_mouse.zoom_nudge()
        time.sleep(chunk * 0.5)

    return chunk


def cycle_tokens(mgr: SystemFirefoxManager, ctx: Dict[str, Any]) -> bool:
    while keep_running:
        ctx["round_index"] = ctx.get("round_index", 0) + 1
        STATS.reset_round()

        tokens = fetch_and_save_tokens_with_retry()
        if not tokens:
            console.print(
                "[bold red]Failed to fetch tokens after max retries. Will retry...[/bold red]"
            )
            time.sleep(30)
            continue

        # Randomize order every round
        random.shuffle(tokens)
        console.print(
            f"[dim]Shuffled {len(tokens)} tokens — order for this round is randomized.[/dim]"
        )

        if CONFIG["max_tokens_per_round"] > 0:
            tokens = tokens[: CONFIG["max_tokens_per_round"]]

        ctx["tokens_total"] = len(tokens)
        ctx["tokens_processed"] = 0

        default_iv = CONFIG["default_interval"]
        console.print(Panel.fit(
            f"[bold cyan]Setting chart interval to {default_iv} via keyboard...[/bold cyan]",
            border_style="cyan",
        ))
        mgr.set_interval(default_iv)
        human_delay(1.0, 2.0)

        console.print(Panel.fit(
            f"[bold green]Starting token cycle[/bold green]\n"
            f"[dim]Total tokens:[/dim] [cyan]{len(tokens)}[/cyan]\n"
            f"[dim]Default interval:[/dim] [cyan]{default_iv}[/cyan]\n"
            f"[dim]Wait between tokens:[/dim] "
            f"{CONFIG['min_wait_between_tokens']}-{CONFIG['max_wait_between_tokens']}s\n"
            f"[dim]Rainbow play:[/dim] every [cyan]{CONFIG['rainbow_play_every']}[/cyan] tokens "
            f"(colors: {', '.join(CONFIG['rainbow_colors'])}, sweep on 'orange')\n"
            f"[dim]PyAutoGUI:[/dim] "
            f"{'[green]enabled[/green]' if mgr.human_mouse and mgr.human_mouse._pyauto_ok else '[yellow]disabled[/yellow]'}",
            border_style="green",
            title=f"Token Cycle · Round {ctx['round_index']}",
        ))

        # --- Main token loop ---
        for idx, token in enumerate(tokens, 1):
            if not keep_running:
                return False

            ctx["current_token"] = token
            ctx["tokens_processed"] = idx
            STATS.add_attempt()

            console.rule(
                f"[bold cyan]Token {idx}/{len(tokens)} · {token}[/bold cyan]",
                style="cyan",
            )

            t_start = time.time()
            ok = mgr.search_and_select_token(token)
            if not ok:
                console.print(
                    f"[bold yellow]⚠ Could not select {token}, skipping...[/bold yellow]"
                )
                STATS.add_failure(token)
                continue

            wait_time = human_wait_duration()
            console.print(f"[dim]Watching chart for {wait_time:.1f}s...[/dim]")

            elapsed = 0.0
            while elapsed < wait_time and keep_running:
                elapsed += _micro_behavior_during_wait(mgr, ctx, elapsed, wait_time)

                # [DISABLED] Legend poll for RSI / RSI_MA50
                # if CONFIG["legend_polling_enabled"]:
                #     legend = mgr.find_chart_legend()
                #     if legend and legend.get("ok") and legend.get("found", 0) > 0:
                #         entry = legend["candidates"][0]
                #         toks = entry.get("tokens") or []
                #         rsi_val, ma_val = parse_legend_tokens(toks)
                #         if rsi_val is not None: ctx["rsi"] = rsi_val
                #         if ma_val is not None:  ctx["rsi_ma50"] = ma_val
                #         ctx["last_tail"] = toks[-11:] if len(toks) >= 11 else toks

                # [DISABLED] JSON write per token
                # sym = mgr.get_symbol_interval()
                # data = build_output(ctx, sym)
                # try: write_json(Path.cwd() / CONFIG["output_json"], data)
                # except Exception as e: logger.warning(f"JSON write failed: {e}")

                ctx["poll_count"] += 1
                if (CONFIG["recycle_every"] > 0
                        and ctx["poll_count"] % CONFIG["recycle_every"] == 0):
                    console.print("[yellow]Recycling WebDriver session...[/yellow]")
                    try:
                        mgr.recycle_session()
                        mgr.set_interval(default_iv)
                    except Exception as e:
                        logger.warning(f"Session recycle failed: {e}")

            STATS.add_success(time.time() - t_start)

            # --- Rainbow play: every Nth successfully viewed token ---
            every = CONFIG["rainbow_play_every"]
            if every > 0 and idx % every == 0 and keep_running:
                try:
                    mgr.rainbow_play(token)
                except Exception as e:
                    logger.warning(f"Rainbow play failed on {token}: {e}")
                # After the sweep, restore the default interval (e.g. 10R)
                try:
                    mgr.set_interval(default_iv)
                except Exception:
                    pass

        # --- Retry failed tokens once ---
        if STATS.failed_tokens:
            retry_list = list(STATS.failed_tokens)
            STATS.failed_tokens.clear()
            console.print(Panel.fit(
                f"[bold yellow]Retrying {len(retry_list)} failed token(s)...[/bold yellow]",
                border_style="yellow",
            ))
            for token in retry_list:
                if not keep_running:
                    return False
                ctx["current_token"] = token
                console.rule(f"[bold yellow]Retry · {token}[/bold yellow]", style="yellow")
                ok = mgr.search_and_select_token(token)
                if not ok:
                    STATS.add_failure(token)
                    continue
                wait_time = human_wait_duration()
                elapsed = 0.0
                while elapsed < wait_time and keep_running:
                    elapsed += _micro_behavior_during_wait(mgr, ctx, elapsed, wait_time)
                STATS.add_success(0)

        # --- Round summary ---
        _print_round_summary(ctx)

        console.print(Panel.fit(
            f"[bold green]✓ Completed round {ctx['round_index']}[/bold green]\n"
            f"[dim]Starting a fresh, re-shuffled pass over the token list...[/dim]",
            border_style="green",
        ))

        human_like_idle(random.uniform(2.0, 5.0))

    return True


def _print_round_summary(ctx: Dict[str, Any]):
    """Pretty summary table at the end of each round."""
    t = Table(
        title=f"Round {ctx['round_index']} Summary",
        box=box.ROUNDED,
        header_style="bold cyan",
        show_header=True,
    )
    t.add_column("Metric", style="dim", no_wrap=True)
    t.add_column("Value", style="bold")

    t.add_row("Tokens attempted", str(STATS.tokens_attempted))
    t.add_row("Tokens succeeded", f"[green]{STATS.tokens_succeeded}[/green]")
    t.add_row("Tokens failed", f"[red]{STATS.tokens_failed}[/red]")
    t.add_row("Interval set OK", f"[green]{STATS.intervals_set_ok}[/green]")
    t.add_row("Interval set fail", f"[red]{STATS.intervals_set_fail}[/red]")
    avg = STATS.avg_token_time
    t.add_row("Avg per token", f"{avg:.1f}s" if avg else "—")
    t.add_row("Round duration", f"{STATS.round_elapsed:.0f}s")
    t.add_row("Finished at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    console.print(t)


# ==================== MAIN AUTOMATION ====================
def automate(wait_for_login: bool = False):
    logger.info("=" * 60)
    logger.info("TradingView token cycler — starting up")
    logger.info(f"  firefox     : {CONFIG['firefox_path']}")
    logger.info(f"  profile     : {CONFIG['user_data_dir']}")
    logger.info(f"  tokens file : {CONFIG['tokens_file']} (primary source)")
    logger.info(f"  fallback    : Binance 24h gainers")
    logger.info(f"  default iv  : {CONFIG['default_interval']}")
    logger.info(f"  pyautogui   : {'yes' if _PYAUTOGUI_AVAILABLE else 'no'}")
    logger.info("=" * 60)
    setup_directories()

    if ARGS.dry_run:
        console.print(Panel.fit(
            "[bold yellow]DRY RUN[/bold yellow] — resolving tokens only, no browser.",
            border_style="yellow",
        ))
        tokens = fetch_and_save_tokens_with_retry()
        if not tokens:
            console.print("[red]No tokens available.[/red]")
            return {"success": False}
        console.print(Panel.fit(
            f"[bold green]Resolved {len(tokens)} tokens[/bold green]\n"
            f"[dim]Source:[/dim] {'local file' if Path(CONFIG['tokens_file']).exists() else 'Binance'}",
            border_style="green",
        ))
        # Show a sample
        sample = tokens[:50]
        for i, tok in enumerate(sample, 1):
            console.print(f"  {i:>4}. {tok}")
        if len(tokens) > len(sample):
            console.print(f"  [dim]… and {len(tokens) - len(sample)} more[/dim]")
        return {"success": True}

    ctx = new_ctx()

    while keep_running:
        mgr = SystemFirefoxManager(CONFIG)
        try:
            mgr.start()
            mgr.navigate_to_chart()

            if wait_for_login:
                console.print(Panel.fit(
                    "[bold yellow]Waiting for login...[/bold yellow]\n"
                    "[dim]Press Enter to continue once logged in and chart is visible.[/dim]",
                    border_style="yellow",
                ))
                try:
                    input()
                except EOFError:
                    pass
                human_delay(1, 2)

            if not mgr.wait_for_chart(timeout=90):
                logger.warning("Chart did not appear to load properly, continuing anyway...")

            if CONFIG.get("detect_login", True):
                if mgr.is_logged_in():
                    console.print("[green]✓ Logged in[/green]")
                else:
                    console.print(Panel.fit(
                        "[bold yellow]⚠ Not logged in?[/bold yellow]\n"
                        "[dim]Chart may be limited. Continuing anyway...[/dim]",
                        border_style="yellow",
                    ))

            if mgr.human_mouse:
                mgr.human_mouse.random_wander()

            cycle_tokens(mgr, ctx)

            mgr.cleanup()
            if not keep_running or not CONFIG["restart_on_crash"]:
                break
            logger.info(f"Restarting browser in {CONFIG['retry_delay']}s...")
            time.sleep(CONFIG["retry_delay"])
            ctx = new_ctx()

        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error(traceback.format_exc())
            mgr.cleanup()
            if not keep_running:
                break
            time.sleep(CONFIG["retry_delay"])

    return {"success": True}


def main():
    apply_cli_overrides()

    console.print(Panel.fit(
        "[bold cyan]TradingView Token Cycler[/bold cyan]\n"
        f"[dim]Local token_usdt.txt (primary) · Binance fallback · "
        f"{CONFIG['default_interval']} charts · rainbow play[/dim]",
        border_style="cyan",
        title="🤖 Automation",
    ))

    if ARGS.debug:
        console.print("[yellow]Debug mode enabled[/yellow]")
    if not _PYAUTOGUI_AVAILABLE:
        console.print(
            "[yellow]pyautogui not available — OS-level mouse wander disabled.\n"
            "Install with: pip install pyautogui[/yellow]"
        )

    try:
        automate(wait_for_login=ARGS.wait_login)
        return 0
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Interrupted by user[/bold yellow]")
        return 130


if __name__ == "__main__":
    sys.exit(main())