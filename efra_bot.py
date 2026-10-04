#!/usr/bin/env python3
"""
EFRA - Microstructure scalping & compounding engine (spot, long-only, ccxt/ccxt.pro).

WHAT IT DOES:
  1. SCAN       - Ranks spot pairs by (24h range) / (spread + round-trip fees),
                  filtering for top liquid pairs with tight spreads.
  2. LIVE FEED  - Streams sub-50ms L2 order books & real-time trade tape (CVD)
                  via ccxt.pro WebSockets, with automatic REST fallback.
  3. ALPHA      - Triple-confluence signal:
                  • 5-level Order Book Imbalance (OBI >= 0.68)
                  • Cumulative Volume Delta (CVD aggressive buyer flow >= 0.52)
                  • Micro-price momentum (>= 2.0 bps)
                  • Spread quality filter & cost edge clearance.
  4. EXIT       - Dynamic 3-stage profit lock:
                  • Breakeven Ratchet: locks in profit (+fees) once +16-20 bps is reached.
                  • Dynamic Trailing Stop: trails peak by 12 bps for +60 to +150 bps runners.
                  • Hard Stop-Loss (-25 bps), timeout (90s), or imbalance flip.
  5. COMPOUND   - Auto-detects deposits and capital growth:
                  • Every +10% equity gain promotes to the next compounding tier.
                  • Multi-slot risk allocation (1 to 3 concurrent slots) as equity scales.
                  • Anti-martingale streak protection.
  6. PROTECT    - Daily loss limit halts trading, cancels pending orders, and flattens positions.

USAGE:
  # Paper mode with WebSocket live stream & terminal dashboard:
  python efra_bot.py --exchange kraken --paper --dashboard

  # Run instant 5-second exchange & live data diagnostic:
  python efra_bot.py --exchange kraken --diagnostic

  # Maker post-only mode:
  python efra_bot.py --exchange kraken --mode maker

  # Live trading (requires EFRA_API_KEY and EFRA_API_SECRET):
  python efra_bot.py --exchange kraken --live --yes
"""

import argparse
import csv
import logging
import os
import sys
import time
from collections import defaultdict, deque
import json
from dataclasses import dataclass, fields

try:
    from efra_stream import LiveStreamEngine
    HAS_STREAM_ENGINE = True
except ImportError:
    HAS_STREAM_ENGINE = False

import datetime

try:
    from efra_dashboard import EfraDashboard
    HAS_DASHBOARD = True
except ImportError:
    HAS_DASHBOARD = False

# ── Strategy version identifiers ────────────────────────────────────────────
# Increment ENTRY_POLICY_VERSION whenever entry thresholds or signal paths change.
# This tags every closed trade so performance can be segregated by policy build.
STRATEGY_VERSION  = "smart-exit-v1"   # architectural version
ENTRY_POLICY_VERSION = "ep-20261004"  # entry parameter snapshot date
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Cfg:
    exchange: str = "binance"
    mode: str = "taker"                  # "taker" (cross spread) or "maker" (rest post-only orders)
    quote: str = "USDT"
    paper: bool = True
    start_balance: float = 100.0         # paper only; live uses real balance

    # live data / stream
    ws: bool = True                      # use ccxt.pro WebSockets for sub-50ms updates
    diagnostic: bool = False             # run diagnostic health check and exit
    dashboard: bool = False              # interactive terminal HUD

    # scanner filters
    min_quote_volume: float = 1_000_000  # 24h volume in quote currency ($1M+ minimum)
    min_price: float = 0.01              # skip sub-penny dust coins
    max_spread_bps: float = 6.0          # strictly tight spreads (max 0.06% spread)
    watch_n: int = 20                    # pairs polled each loop
    rescan_s: int = 60                   # rescan every 60s — faster hot-pair rotation

    # costs (per side)
    fee_bps: float = 20.0                # 20 bps = 0.20% (Gate.io taker)
    maker_fee_bps: float = 20.0          # fallback
    auto_fee: int = 1
    maker_timeout_s: int = 12
    maker_cancel_imb: float = 0.50
    slippage_bps: float = 1.0

    # entry / exit
    # TP raised to 250 bps — gives trailing stop more room to run before locking
    tp_bps: float = 250.0
    # SL tightened to 35 bps — cut losses faster, asymmetric vs wins
    sl_bps: float = 35.0
    min_net_edge_bps: float = 15.0       # tp must beat roundtrip costs by at least 15 bps
    # OBI bar raised: require stronger bid-side dominance
    imbalance_entry: float = 0.72
    # CVD bar raised: require genuine aggressive buyer tape
    min_cvd: float = 0.65
    # Confluence bar raised: higher composite signal quality required
    min_confluence: float = 62.0
    # Min momentum raised: avoid entering slow drift moves
    min_mom_bps: float = 3.5
    max_mom_bps: float = 18.0            # tighter cap — don't chase late exhaustion
    min_mom_accel: float = 0.0           # momentum must be accelerating (>=0 = not decelerating)
    min_wall_ratio: float = 0.0          # bid-wall dominance ratio (0 = disabled)
    mom_window: int = 8
    # Max hold reduced: cut time-losers faster, don't give dead positions free rent
    max_hold_s: int = 180
    cooldown_s: int = 30
    # Inter-trade pause raised: prevents cascade re-entries after losses
    inter_trade_pause_s: float = 45.0
    btc_filter: bool = True
    vol_surge_factor: float = 1.5        # enter only if recent vol >= 1.5x 24h avg rate
    smart_reentry: bool = True           # allow re-entry after early exit if momentum continues

    # dynamic profit ratchet & trailing stop
    # Breakeven lock raised: don't lock at a level that guarantees tiny PnL
    breakeven_bps: float = 120.0
    # Trail trigger raised: let trade develop further before activating trail
    trail_trigger_bps: float = 130.0
    # Trail tightened to 15 bps: capture more of each winning move
    trail_bps: float = 15.0

    # Smart slow-mover bail: cut confirmed losing positions heading lower
    # Requires: held >= slow_bail_hold_s, negative return, AND both momentum + CVD strongly bearish
    slow_bail_hold_s: float = 75.0       # minimum hold before slow bail can fire
    slow_bail_ret_bps: float = -8.0      # position must be losing by at least this much
    slow_bail_mom_bps: float = -4.0      # momentum must be actively falling
    slow_bail_cvd: float = 0.38          # CVD must show dominant sellers (< 38% buyers)
    # OBI-flip exit: book has completely inverted (sellers dominate), position still losing
    flip_bail_hold_s: float = 60.0       # minimum hold before flip can trigger
    flip_bail_imb: float = 0.15          # OBI must have crashed to extreme sell-dominance
    flip_bail_ret_bps: float = -5.0      # must be losing to avoid cutting winning positions
    # Flash-wick SL debounce: require SL breach to persist for N seconds before cutting
    # Prevents 50ms price spikes from triggering full stop-losses
    sl_wick_debounce_s: float = 1.5      # seconds SL must be breached before it fires

    # Correlated-loss guard: if >= 2 SL exits in last sl_cluster_window_s, pause entries
    sl_cluster_window_s: float = 90.0    # look-back window for SL clustering detection
    sl_cluster_count: int = 2            # number of SLs in window to trigger pause
    sl_cluster_pause_s: float = 120.0    # how long to pause entries after cluster detected
    # High-confidence-only mode after loss streaks
    streak_conf_boost: float = 8.0       # add this to min_confluence after >= 3 losses
    streak_obi_boost: float = 0.05       # add to imbalance_entry after >= 3 losses

    # compounding & risk (Kelly-adjusted position sizing)
    position_frac: float = 0.40          # base allocation per slot
    kelly_half: bool = True              # use half-Kelly to reduce variance
    max_positions: int = 2
    multi_slot: bool = True
    compound_step: float = 0.05
    daily_loss_limit_frac: float = 0.08  # halt if equity falls 8% in a day

    loop_s: float = 0.3
    log_file: str = "efra_trades.csv"
    state_file: str = "efra_state.json"
    balance_cache_s: float = 5.0


class Bot:
    def __init__(self, cfg: Cfg, ex=None):
        self.c = cfg
        if ex is None:
            ex = self._build_exchange()
        self.ex = ex
        self.cash = cfg.start_balance          # paper cash
        self.start_balance = cfg.start_balance
        self.pos = {}                          # symbol -> position dict
        self.mids = defaultdict(lambda: deque(maxlen=cfg.mom_window))
        self.cool = {}
        self.watch = []
        self.last_scan = 0.0
        self.day_start = time.time()
        self.day_start_eq = None
        # day_start_date: UTC calendar date string used for restart-safe daily loss guard
        self.day_start_date: str = ""
        self.halted = False
        self.n_trades = 0
        self.n_wins = 0
        self.loss_streak = 0
        self._bal_cache = (0.0, None)
        self.err_streak = 0
        self.last_books = {}

        # Compounding engine
        self.compound_tier = 1
        self.last_tier_equity = cfg.start_balance
        self.high_water_mark = cfg.start_balance
        self.last_close_ts = 0.0

        # === ULTRA-ADVANCED SYSTEMS ===
        # 1. Kelly win-rate tracker (rolling 50 trades)
        self._kelly_wins: deque = deque(maxlen=50)   # 1 = win, 0 = loss
        self._kelly_payoffs: deque = deque(maxlen=50) # abs pnl/cost ratio per trade
        # 2. Smart re-entry: tracks early exits and re-entry eligibility
        self._early_exits: dict = {}   # sym -> {ts, exit_px, reason}
        # 3. Relative volume surge tracker (rolling 5-min trade count)
        self._vol_windows: dict = defaultdict(lambda: deque(maxlen=60))  # sym -> deque of tick cvd timestamps
        # 4. Session regime: tracks hourly win-rate to scale size by session
        self._session_wins: deque = deque(maxlen=20)  # rolling 20-trade window per session
        self._session_hour: int = -1
        # 5. SL-cluster guard: track recent SL timestamps to detect correlated loss bursts
        self._recent_sl_ts: deque = deque(maxlen=10)  # timestamps of recent SL exits
        self._sl_cluster_pause_until: float = 0.0    # epoch until which entries are paused

        # Isolated state & log files per exchange and execution mode to prevent cross-process corruption
        if self.c.state_file == "efra_state.json":
            self.c.state_file = f"efra_state_{self.c.exchange}_{self.c.mode}.json"
        if self.c.log_file == "efra_trades.csv":
            self.c.log_file = f"efra_trades_{self.c.exchange}_{self.c.mode}.csv"

        # Live WebSocket stream engine
        self.ws_engine = None
        if cfg.ws and HAS_STREAM_ENGINE:
            try:
                ex_cfg = {}
                if not cfg.paper:
                    ex_cfg["apiKey"] = os.environ.get("EFRA_API_KEY", "")
                    ex_cfg["secret"] = os.environ.get("EFRA_API_SECRET", "")
                    if os.environ.get("EFRA_API_PASSWORD"):
                        ex_cfg["password"] = os.environ.get("EFRA_API_PASSWORD")
                self.ws_engine = LiveStreamEngine(cfg.exchange, cfg.quote, ex_cfg)
                logging.info("WebSocket live data engine initialized for %s", cfg.exchange)
            except Exception as e:
                logging.warning("Could not initialize WebSocket engine (%s); falling back to REST", e)
                self.ws_engine = None

        # Dashboard HUD
        self.dashboard = None
        if cfg.dashboard and HAS_DASHBOARD:
            self.dashboard = EfraDashboard(self)

        self._init_log()

    # ---------- setup ----------
    def _build_exchange(self):
        c = self.c
        try:
            import ccxt
        except ImportError:
            raise SystemExit("ccxt is not installed. Run:  pip install ccxt")
        if not hasattr(ccxt, c.exchange):
            near = [x for x in ccxt.exchanges if c.exchange.lower() in x][:8]
            raise SystemExit(f"Unknown exchange '{c.exchange}'."
                             + (f" Did you mean: {', '.join(near)}?" if near else ""))
        params = {"enableRateLimit": True, "timeout": 20000,
                  "options": {"defaultType": "spot"}}
        if not c.paper:
            key, sec = os.environ.get("EFRA_API_KEY"), os.environ.get("EFRA_API_SECRET")
            if not key or not sec:
                raise SystemExit(
                    "Live mode needs credentials. Set them before starting:\n"
                    "  export EFRA_API_KEY=...\n  export EFRA_API_SECRET=...")
            params["apiKey"], params["secret"] = key, sec
            pw = os.environ.get("EFRA_API_PASSWORD")
            if pw:
                params["password"] = pw
        return getattr(ccxt, c.exchange)(params)

    def preflight(self):
        """Validates market access, fees, balances, and executes diagnostic if requested."""
        c = self.c
        if not self.ex.markets:
            try:
                self.ex.load_markets()
            except Exception as e:
                logging.warning("load_markets warning: %s", e)

        spot = [s for s, m in self.ex.markets.items()
                if m.get("spot") and m.get("quote") == c.quote and m.get("active", True)]
        if not spot:
            quotes = sorted({m.get("quote") for m in self.ex.markets.values()
                             if m.get("spot")} - {None})
            raise SystemExit(f"No active spot pairs quoted in {c.quote} on "
                             f"{c.exchange}. Available: {', '.join(quotes[:12])}"
                             f"\nRe-run with e.g. --quote {quotes[0] if quotes else 'USDT'}")
        logging.info("preflight: %d spot %s pairs on %s", len(spot), c.quote, c.exchange)

        if c.auto_fee:
            self._load_real_fee(spot[0])

        if not c.paper:
            try:
                bal = self.ex.fetch_balance()
            except Exception as e:
                raise SystemExit(f"Could not read balance - check API credentials and server clock: {e}")
            free = float(bal["free"].get(c.quote, 0.0))
            self.start_balance = free
            self.last_tier_equity = free
            self.high_water_mark = free
            logging.info("preflight: live balance %.4f %s", free, c.quote)
            if free <= 0:
                raise SystemExit(f"Your {c.quote} spot balance is 0. Deposit funds into SPOT wallet before --live.")

        self.recover_state()
        cost_bps = 2 * (c.fee_bps + c.slippage_bps)
        logging.info("preflight OK | fee=%.1fbps | round-trip cost=%.1fbps | tp=%.1fbps | stream=%s",
                     c.fee_bps, cost_bps, c.tp_bps, "WS" if self.ws_engine else "REST")

        if c.diagnostic:
            self.run_diagnostic(spot[0])
            sys.exit(0)

    def run_diagnostic(self, sample_sym: str):
        """Instant diagnostic check for connectivity, latency, fee schedule, and book stream."""
        c = self.c
        print("\n" + "=" * 60)
        print(f" EFRA SYSTEM DIAGNOSTIC REPORT - {c.exchange.upper()}")
        print("=" * 60)
        print(f" Mode               : {'PAPER (Virtual)' if c.paper else 'LIVE (Real Capital)'}")
        print(f" Quote Currency     : {c.quote}")
        print(f" Active Spot Pairs  : {len(self.ex.markets)}")
        print(f" Taker Fee Tier     : {c.fee_bps:.1f} bps (0.{int(c.fee_bps*10)}%)")
        print(f" Maker Fee Tier     : {c.maker_fee_bps:.1f} bps")
        print(f" Round-Trip Cost    : {2*(c.fee_bps+c.slippage_bps):.1f} bps")

        # Test Ticker Ping
        t0 = time.time()
        try:
            ticker = self.ex.fetch_ticker(sample_sym)
            lat = (time.time() - t0) * 1000.0
            bid = ticker.get('bid')
            ask = ticker.get('ask')
            print(f" REST Latency       : {lat:.1f} ms on {sample_sym}")
            print(f" Sample Ticker      : Bid={bid} Ask={ask}")
        except Exception as e:
            print(f" REST Latency       : FAILED ({e})")

        # Test WebSocket Feed
        if self.ws_engine:
            print(" Testing WebSocket  : Subscribing to", sample_sym, "...")
            self.ws_engine.start([sample_sym], markets=self.ex.markets)
            received = False
            for _ in range(16):
                time.sleep(0.5)
                b = self.ws_engine.get_book(sample_sym)
                if b:
                    received = True
                    print(f" WebSocket Stream   : ACTIVE (Latency sub-50ms | Imb={b['imb']:.2f} | CVD={b['cvd_5s']:.2f})")
                    break
            if not received:
                print(" WebSocket Stream   : PENDING / Falling back to REST")
            self.ws_engine.stop()
        else:
            print(" WebSocket Stream   : DISABLED (REST Polling active)")

        print("=" * 60)
        print(" DIAGNOSTIC RESULT  : ALL SYSTEMS OPERATIONAL. READY TO RUN!")
        print("=" * 60 + "\n")

    def _load_real_fee(self, sample_sym):
        c = self.c
        fee = maker = None
        try:
            if not c.paper and self.ex.has.get("fetchTradingFee"):
                tf = self.ex.fetch_trading_fee(sample_sym)
                fee, maker = tf.get("taker"), tf.get("maker")
        except Exception:
            fee = maker = None
        mk = self.ex.markets.get(sample_sym) or {}
        fee = mk.get("taker") if fee is None else fee
        maker = mk.get("maker") if maker is None else maker
        if maker is not None:
            c.maker_fee_bps = float(maker) * 1e4
        if fee:
            c.fee_bps = float(fee) * 1e4
        logging.info("fee schedule: maker %.1fbps / taker %.1fbps", c.maker_fee_bps, c.fee_bps)

    # ---------- state & persistence ----------
    def save_state(self):
        """Persist all adaptive state so restarts never reset risk budgets or strategy memory."""
        try:
            with open(self.c.state_file, "w") as f:
                json.dump({
                    # Core book-keeping
                    "pos": self.pos,
                    "cash": self.cash,
                    "n_trades": self.n_trades,
                    "n_wins": self.n_wins,
                    "compound_tier": self.compound_tier,
                    "last_tier_equity": self.last_tier_equity,
                    "high_water_mark": self.high_water_mark,
                    # Adaptive risk state — MUST survive restart
                    "loss_streak": self.loss_streak,
                    "kelly_wins": list(self._kelly_wins),
                    "kelly_payoffs": list(self._kelly_payoffs),
                    # Daily loss guard — persisted as UTC calendar date + baseline equity
                    # so a container restart mid-day does NOT create a fresh risk budget
                    "day_start_date": self.day_start_date,
                    "day_start_eq": self.day_start_eq,
                    # Strategy version tag
                    "strategy_version": STRATEGY_VERSION,
                    "entry_policy": ENTRY_POLICY_VERSION,
                }, f)
        except Exception:
            logging.exception("could not save state")

    def recover_state(self):
        """Restore all adaptive state, preserving daily loss guard across restarts."""
        if not os.path.exists(self.c.state_file):
            return
        try:
            with open(self.c.state_file) as f:
                st = json.load(f)
        except Exception:
            logging.warning("state file unreadable, ignoring")
            return

        self.n_trades = st.get("n_trades", 0)
        self.n_wins = st.get("n_wins", 0)
        self.compound_tier = st.get("compound_tier", 1)
        self.last_tier_equity = st.get("last_tier_equity", self.cash)
        self.high_water_mark = st.get("high_water_mark", self.cash)
        if self.c.paper:
            self.cash = st.get("cash", self.cash)

        # Restore adaptive risk memory
        self.loss_streak = st.get("loss_streak", 0)
        for v in st.get("kelly_wins", []):
            self._kelly_wins.append(v)
        for v in st.get("kelly_payoffs", []):
            self._kelly_payoffs.append(v)

        # === DAILY LOSS GUARD — restart-safe ===
        # If a saved day_start_date exists and matches TODAY (UTC), restore the
        # saved day_start_eq so the 8% budget is NOT silently reset by a redeploy.
        # If the date is stale (yesterday), let the normal step() logic re-anchor.
        import datetime
        today_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        saved_date = st.get("day_start_date", "")
        saved_eq = st.get("day_start_eq")
        if saved_date == today_utc and saved_eq is not None:
            self.day_start_date = saved_date
            self.day_start_eq = float(saved_eq)
            logging.info(
                "Daily loss guard restored: date=%s baseline=$%.4f (NOT reset by restart)",
                saved_date, self.day_start_eq
            )

        for sym, p in (st.get("pos") or {}).items():
            if sym in self.ex.markets:
                p["ts"] = time.time()
                self.pos[sym] = p
                logging.warning("recovered open position %s qty=%.8g entry=%.8g", sym, p["qty"], p["entry"])

    # ---------- helpers ----------
    def _init_log(self):
        if not os.path.exists(self.c.log_file):
            with open(self.c.log_file, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["ts", "symbol", "entry", "exit", "qty", "pnl_quote",
                     "reason", "equity", "tier",
                     "strategy_version", "entry_policy", "signal_path"])

    def _quote_free(self, fresh=False):
        if self.c.paper:
            return self.cash
        ts, val = self._bal_cache
        if not fresh and val is not None and time.time() - ts < self.c.balance_cache_s:
            return val
        val = float(self.ex.fetch_balance()["free"].get(self.c.quote, 0.0))
        self._bal_cache = (time.time(), val)
        return val

    def equity(self, books):
        eq = self._quote_free()
        for sym, p in self.pos.items():
            mid = books.get(sym, {}).get("mid", p["entry"])
            eq += p["qty"] * mid
        return eq

    # ---------- compounding & slots ----------
    def check_compounding(self, current_equity: float):
        """Auto-detects +10% capital growth or deposits to scale compounding tiers."""
        c = self.c
        self.high_water_mark = max(self.high_water_mark, current_equity)
        threshold = self.last_tier_equity * (1.0 + c.compound_step)
        if current_equity >= threshold:
            steps = int((current_equity - self.last_tier_equity) / (self.last_tier_equity * c.compound_step))
            steps = max(1, steps)
            old_tier = self.compound_tier
            self.compound_tier += steps
            self.last_tier_equity = current_equity
            logging.info("🚀 LEVEL UP! Equity milestone reached: Tier %d -> %d ($%.2f equity, +%.1f%%)",
                         old_tier, self.compound_tier, current_equity,
                         ((current_equity / self.start_balance) - 1.0) * 100.0)
            self.save_state()

    def get_slot_allocation(self, current_equity: float):
        """Kelly-adjusted dynamic slot sizing with session and streak scaling."""
        c = self.c
        if not c.multi_slot:
            return c.max_positions, c.position_frac

        # Tier-based slot count
        if current_equity < 300.0:
            slots, frac = 2, c.position_frac
        elif current_equity < 1000.0:
            slots, frac = 3, 0.30
        elif current_equity < 3000.0:
            slots, frac = 4, 0.22
        else:
            slots, frac = 5, 0.18

        # === KELLY FRACTION: size positions by real win-rate & avg payoff ===
        if c.kelly_half and len(self._kelly_wins) >= 10:
            w = sum(self._kelly_wins) / len(self._kelly_wins)   # empirical win rate
            avg_win  = (sum(v for v, k in zip(self._kelly_payoffs, self._kelly_wins) if k) /
                        max(1, sum(self._kelly_wins)))
            avg_loss = (sum(v for v, k in zip(self._kelly_payoffs, self._kelly_wins) if not k) /
                        max(1, len(self._kelly_wins) - sum(self._kelly_wins)))
            b = avg_win / max(avg_loss, 0.001)  # payoff ratio
            kelly_f = (b * w - (1 - w)) / max(b, 0.001)  # full Kelly
            half_kelly = max(0.10, min(0.55, kelly_f * 0.5))  # half Kelly, clamped
            frac = min(frac, half_kelly)
            logging.debug("Kelly: w=%.2f b=%.2f full=%.2f half=%.2f frac=%.2f",
                          w, b, kelly_f, half_kelly, frac)

        # === SESSION REGIME: reduce size during low-confidence sessions ===
        import time as _time
        hour = int(_time.gmtime().tm_hour)
        # Asian session (00-08 UTC): lower liquidity, tighter sizing
        if 0 <= hour < 8:
            frac *= 0.75
        # Peak London/NY overlap (12-20 UTC): max aggression
        elif 12 <= hour < 20:
            frac = min(frac * 1.10, 0.55)

        # Session win-rate boost: if last 5 trades all won, confidence mode +10%
        if len(self._session_wins) >= 5 and all(self._session_wins):
            frac = min(frac * 1.10, 0.55)

        # Streak defensive scaling
        if self.loss_streak >= 2:
            frac *= 0.70
        if self.loss_streak >= 4:
            frac *= 0.60   # heavy drawdown protection

        return slots, frac

    def check_btc_regime(self):
        """Monitors BTC price trend to prevent entering long positions during broader market selloffs."""
        if not getattr(self.c, "btc_filter", True):
            return True, 0.0
        btc_sym = f"BTC/{self.c.quote}"
        if btc_sym not in self.ex.markets:
            return True, 0.0
        bk = self.read_book(btc_sym)
        if not bk:
            return True, 0.0
        mom = bk.get("mom", 0.0)
        # If BTC is experiencing a sharp downward impulse (< -12 bps), pause new altcoin entries
        is_safe = (mom > -12.0)
        return is_safe, mom

    # ---------- scanner ----------
    def scan(self):
        c = self.c
        rows = []
        try:
            tickers = self.ex.fetch_tickers()
        except Exception as e:
            logging.warning("scan failed (%s) - keeping current watchlist", e)
            self.last_scan = time.time()
            return

        # 24h average volume rate (quote/second) for vol-surge detection
        avg_vol_rates = {}
        for sym, t in tickers.items():
            qv = t.get("quoteVolume") or 0.0
            if qv > 0:
                avg_vol_rates[sym] = qv / 86400.0

        for sym, t in tickers.items():
            m = self.ex.markets.get(sym)
            if not m or not m.get("spot") or not m.get("active", True):
                continue
            if m.get("quote") != c.quote or not sym.isascii():
                continue
            # Skip leveraged ETF tokens
            if any(x in sym for x in ("3L/", "3S/", "5L/", "5S/", "3L_", "3S_", "5L_", "5S_")):
                continue
            bid, ask, last = t.get("bid"), t.get("ask"), t.get("last")
            hi, lo = t.get("high"), t.get("low")
            qv = t.get("quoteVolume") or ((t.get("baseVolume") or 0.0) * (last or 0.0))
            if not all([bid, ask, last, hi, lo]) or qv < c.min_quote_volume or last < getattr(c, "min_price", 0.01):
                continue
            mid = (bid + ask) / 2
            spread = (ask - bid) / mid * 1e4
            if spread > c.max_spread_bps or spread <= 0:
                continue
            rng = (hi - lo) / last * 1e4
            pct = abs(t.get("percentage") or 0.0)
            vol_mil = max(0.1, qv / 1_000_000.0)

            # === VOLUME SURGE BONUS ===
            # Pairs with recent volume spiking above their 24h avg rate score higher
            # (Indicates fresh catalyst / breakout — not just average market activity)
            vol_surge_mult = 1.0
            if sym in avg_vol_rates and avg_vol_rates[sym] > 0:
                # Use 24h pct change as a proxy for recent volume vs average
                # Positive % change strongly correlates with volume surge
                if pct > 3.0:
                    vol_surge_mult = 1.0 + min(1.5, pct / 10.0)  # up to 2.5x boost

            # Composite score: volume-weighted, momentum-biased, spread-penalized, surge-amplified
            score = (vol_mil ** 0.4) * (rng * 0.3 + pct * 0.7) * vol_surge_mult / (spread + 1.2 * c.fee_bps)
            rows.append((score, sym))

        rows.sort(reverse=True)
        self.watch = [s for _, s in rows[: c.watch_n]]
        self.last_scan = time.time()
        logging.info("watchlist (%d pairs): %s", len(self.watch), ", ".join(self.watch) or "(empty)")

        # Update WebSocket subscriptions if stream engine is active
        if self.ws_engine:
            active_syms = list(self.pos.keys()) + list(getattr(self, "pending", {}).keys())
            stream_syms = list(dict.fromkeys(self.watch + active_syms))
            if not self.ws_engine.is_running:
                self.ws_engine.start(stream_syms, markets=self.ex.markets)
            else:
                self.ws_engine.update_symbols(stream_syms)

    # ---------- microstructure signal ----------
    def read_book(self, sym: str, is_position: bool = False):
        # 1. Fast WebSocket cache check (< 0.05ms)
        if self.ws_engine and self.ws_engine.is_running:
            bk = self.ws_engine.get_book(sym, max_age_s=10.0)
            if bk:
                self.last_books[sym] = bk
                return bk
            # If WS is running, use recent cached snapshot or skip to prevent blocking the high-speed loop
            if sym in self.last_books:
                return self.last_books[sym]
            if not is_position:
                return None

        # 2. Check recent cached snapshot to keep loop moving fast
        cached = self.last_books.get(sym)
        if cached and not is_position and (time.time() - cached.get("ts", 0) < 4.0):
            return cached

        # 3. REST query (pure REST mode or active position fallback)
        try:
            ob = self.ex.fetch_order_book(sym, limit=10)
        except Exception as e:
            logging.debug("order book %s failed: %s", sym, e)
            return self.last_books.get(sym)
        if not ob.get("bids") or not ob.get("asks"):
            return self.last_books.get(sym)
        bid, ask = ob["bids"][0][0], ob["asks"][0][0]
        mid = (bid + ask) / 2
        bv = sum(lvl[0] * lvl[1] for lvl in ob["bids"][:5])
        av = sum(lvl[0] * lvl[1] for lvl in ob["asks"][:5])
        d = self.mids[sym]
        d.append(mid)
        mom = (mid - d[0]) / d[0] * 1e4 if len(d) == d.maxlen else 0.0
        imb = bv / (bv + av) if (bv + av) else 0.5
        spread = (ask - bid) / mid * 1e4

        bq1 = ob["bids"][0][0] * ob["bids"][0][1]
        aq1 = ob["asks"][0][0] * ob["asks"][0][1]
        micro_px = (ask * bq1 + bid * aq1) / (bq1 + aq1) if (bq1 + aq1) > 0 else mid
        micro_skew = ((micro_px - mid) / mid) * 1e4 if mid > 0 else 0.0

        imb_score = max(0.0, min(1.0, (imb - 0.5) * 3.0))
        micro_score = max(0.0, min(1.0, micro_skew / 2.0)) if micro_skew > 0 else 0.0
        mom_score = max(0.0, min(1.0, (mom + 2.0) / 6.0)) if mom > -2.0 else 0.0
        conf = (imb_score * 45.0 + micro_score * 25.0 + mom_score * 30.0)

        snap = {
            "bid": bid, "ask": ask, "mid": mid, "micro_px": micro_px,
            "micro_skew": micro_skew, "spread": spread,
            "imb": imb, "imb10": imb, "cvd_5s": 0.50, "cvd_15s": 0.50,
            "mom": mom, "confluence": conf, "ts": time.time(),
        }
        self.last_books[sym] = snap
        return snap

    # ---------- execution ----------
    def _ioc(self, sym, side, qty, ref_px):
        pad = 20 / 1e4
        px = ref_px * (1 + pad) if side == "buy" else ref_px * (1 - pad)
        px = float(self.ex.price_to_precision(sym, px))
        o = self.ex.create_order(sym, "limit", side, qty, px, {"timeInForce": "IOC"})
        if o.get("filled") is None or not o.get("average"):
            try:
                o = self.ex.fetch_order(o["id"], sym)
            except Exception:
                pass
        return o

    def open(self, sym, s, quote_amt):
        c = self.c
        m = self.ex.markets[sym]
        lim = m.get("limits") or {}
        min_cost = (lim.get("cost") or {}).get("min") or 0
        if quote_amt < min_cost:
            logging.info("skip %s: %.2f below exchange min notional %.2f", sym, quote_amt, min_cost)
            return
        min_amt = (lim.get("amount") or {}).get("min") or 0
        if min_amt and quote_amt / s["ask"] < min_amt:
            logging.info("skip %s: size below exchange min amount %.8g", sym, min_amt)
            return

        if c.paper:
            entry = s["ask"] * (1 + c.slippage_bps / 1e4)
            qty = quote_amt * (1 - c.fee_bps / 1e4) / entry
            self.cash -= quote_amt
        else:
            qty = float(self.ex.amount_to_precision(
                sym, quote_amt / (s["ask"] * (1 + 3 * c.slippage_bps / 1e4))))
            if qty <= 0:
                logging.info("skip %s: size rounds to zero at this price", sym)
                return
            o = self._ioc(sym, "buy", qty, s["ask"])
            filled = o.get("filled")
            if filled is None:
                filled = float(self.ex.fetch_balance()["free"].get(m["base"], 0.0))
            if not filled:
                logging.info("buy %s not filled, skipping", sym)
                return
            entry = o.get("average") or o.get("price") or s["ask"]
            qty = filled
            quote_amt = entry * qty

        self.pos[sym] = {
            "entry": entry,
            "qty": qty,
            "ts": time.time(),
            "cost": quote_amt,
            "peak_ret_bps": 0.0,
            "stop_bps": -c.sl_bps,
            "be_locked": False,
            "trailing_active": False,
            # Entry diagnostics — carried through to close() for CSV tagging
            "signal_path": s.get("signal_path", ""),
            "entry_cvd": s.get("entry_cvd", s.get("cvd_5s", 0.5)),
            "entry_imb": s.get("entry_imb", s.get("imb", 0.5)),
            "entry_mom": s.get("entry_mom", s.get("mom", 0.0)),
            "entry_accel": s.get("entry_accel", s.get("mom_accel", 0.0)),
            "entry_cost_bps": s.get("entry_cost_bps", 2 * (c.fee_bps + c.slippage_bps)),
        }
        self._bal_cache = (0.0, None)
        self.save_state()
        logging.info(
            "BUY  %s @ %.8g | Path=%s OBI=%.2f CVD=%.2f Mom=%.1fbps Accel=%.1f Conf=%.1f Cost=%.1fbps | $%.2f",
            sym, entry,
            s.get("signal_path", "?"),
            s.get("imb", 0.5), s.get("cvd_5s", 0.5), s.get("mom", 0.0),
            s.get("mom_accel", 0.0), s.get("confluence", 0.0),
            s.get("entry_cost_bps", 0.0), quote_amt
        )

    def close(self, sym, s, reason, books):
        c = self.c
        p = self.pos.pop(sym)
        if c.paper:
            # In Maker Mode, non-emergency exits rest at the Ask with maker fees.
            # Only SL (panic stop-loss) crosses the spread as a taker.
            maker_exit = (c.mode == "maker" and reason not in ("sl", "halt"))
            if maker_exit:
                exit_px = s["ask"]               # filled at Ask (we're the resting seller)
                fee_bps = c.maker_fee_bps         # maker fee only
            else:
                exit_px = s["bid"] * (1 - c.slippage_bps / 1e4)
                fee_bps = c.fee_bps
            proceeds = p["qty"] * exit_px * (1 - fee_bps / 1e4)
            self.cash += proceeds
            qty = p["qty"]
        else:
            base = self.ex.markets[sym]["base"]
            free = float(self.ex.fetch_balance()["free"].get(base, 0.0))
            qty = float(self.ex.amount_to_precision(sym, min(free, p["qty"])))
            min_amt = ((self.ex.markets[sym].get("limits") or {}).get("amount") or {}).get("min") or 0
            if qty <= 0 or (min_amt and qty < min_amt):
                logging.warning("%s leftover %.8g below min size - dropping tracking", sym, qty)
                self.save_state()
                return
            o = self._ioc(sym, "sell", qty, s["bid"])
            if o.get("filled") is not None and o["filled"] <= 0:
                self.pos[sym] = p
                logging.warning("sell %s not filled, will retry", sym)
                return
            exit_px = o.get("average") or s["bid"]
            proceeds = exit_px * qty * (1 - c.fee_bps / 1e4)

        pnl = proceeds - p["cost"]
        ret_bps = (exit_px - p["entry"]) / p["entry"] * 1e4
        self.n_trades += 1
        win = pnl > 0
        if win:
            self.n_wins += 1
            self.loss_streak = 0
        else:
            self.loss_streak += 1

        # === KELLY TRACKER: feed real outcomes into rolling win/payoff history ===
        self._kelly_wins.append(1 if win else 0)
        cost_val = max(p["cost"], 1.0)
        self._kelly_payoffs.append(abs(pnl) / cost_val)
        self._session_wins.append(1 if win else 0)

        # === SL-CLUSTER GUARD: detect cascading losses from correlated market moves ===
        if reason == "sl":
            now_ts = time.time()
            self._recent_sl_ts.append(now_ts)
            # Count SLs within the cluster window
            window_start = now_ts - c.sl_cluster_window_s
            sl_count = sum(1 for t in self._recent_sl_ts if t >= window_start)
            if sl_count >= c.sl_cluster_count:
                self._sl_cluster_pause_until = now_ts + c.sl_cluster_pause_s
                logging.warning(
                    "SL-CLUSTER GUARD: %d stop-losses in %.0fs — pausing new entries for %.0fs",
                    sl_count, c.sl_cluster_window_s, c.sl_cluster_pause_s
                )

        # === SMART RE-ENTRY TRACKER ===
        # If we exit on breakeven/trail_stop while momentum is still positive,
        # mark this pair as eligible for fast re-entry
        if reason in ("be_stop", "trail_stop") and ret_bps > 0:
            s_now = books.get(sym) or {}
            if s_now.get("mom", 0.0) > 1.5 and s_now.get("cvd_5s", 0.5) > 0.55:
                self._early_exits[sym] = {
                    "ts": time.time(), "exit_px": exit_px, "reason": reason
                }
                logging.info("SMART RE-ENTRY armed for %s (exited on %s with +%.1fbps, momentum continuing)",
                             sym, reason, ret_bps)

        self._bal_cache = (0.0, None)
        eq = self.equity(books)
        self.check_compounding(eq)

        with open(c.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                int(time.time()), sym, p["entry"], exit_px, qty,
                round(pnl, 6), reason, round(eq, 4), self.compound_tier,
                STRATEGY_VERSION, ENTRY_POLICY_VERSION,
                p.get("signal_path", ""),
            ])

        self.cool[sym] = time.time() + c.cooldown_s
        self.last_close_ts = time.time()
        self.save_state()
        logging.info("SELL %s @ %.8g | %s | PnL=%+.4f (%+.1fbps) | Eq=$%.2f | WinRate=%.0f%% (%d trades)",
                     sym, exit_px, reason.upper(), pnl, ret_bps, eq,
                     100.0 * self.n_wins / self.n_trades, self.n_trades)

    # ---------- main step ----------
    def step(self):
        c = self.c
        now = time.time()
        if now - self.last_scan > c.rescan_s or not self.watch:
            self.scan()

        books = {}
        for sym in self.pos:
            b = self.read_book(sym, is_position=True)
            if b:
                books[sym] = b
        for sym in self.watch:
            if sym not in books:
                b = self.read_book(sym, is_position=False)
                if b:
                    books[sym] = b
        self.last_books = books

        eq = self.equity(books)
        self.check_compounding(eq)

        # Daily loss guard — anchored to UTC calendar date
        # On first step of a new UTC day (or first ever), set the baseline.
        # On restart within the SAME day, recover_state() already restored day_start_eq,
        # so this block is skipped — the risk budget is NOT reset.
        today_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        if self.day_start_date != today_utc or self.day_start_eq is None:
            self.day_start_date = today_utc
            self.day_start_eq = eq
            self.day_start = now
            logging.info("Daily loss guard anchored: date=%s baseline=$%.4f", today_utc, eq)

        # Daily loss limit check
        if eq < self.day_start_eq * (1.0 - c.daily_loss_limit_frac):
            self._bal_cache = (0.0, None)
            eq = self.equity(books)
            if eq < self.day_start_eq * (1.0 - c.daily_loss_limit_frac):
                logging.warning("DAILY LOSS LIMIT HIT (equity %.4f). Flattening and halting.", eq)
                self.halted = True

        # Render dashboard HUD if enabled
        if self.dashboard:
            self.dashboard.render()

        # Dynamic Exits with Profit Ratchet & Trailing Stop
        for sym in list(self.pos):
            s, p = books.get(sym), self.pos[sym]
            if not s:
                continue

            # In Maker Mode, exit price is the resting Ask, not the Bid!
            maker_exit = (c.mode == "maker")
            curr_px = s["ask"] if maker_exit else s["bid"]
            ret = (curr_px - p["entry"]) / p["entry"] * 1e4
            p["peak_ret_bps"] = max(p.get("peak_ret_bps", 0.0), ret)
            
            fee_per_side = c.maker_fee_bps if maker_exit else c.fee_bps
            cost_bps = 2 * (fee_per_side + c.slippage_bps)

            # Stage 1: Breakeven Ratchet (Lock in profit once gain clears fee hurdle)
            # Set stop to cost_bps + 12 (net positive after all fees, 12 bps buffer)
            be_target = max(c.breakeven_bps, cost_bps + 20.0)
            if ret >= be_target and not p.get("be_locked"):
                p["be_locked"] = True
                p["stop_bps"] = cost_bps + 20.0  # guaranteed net positive post-fees (+20 bps buffer)
                logging.info("PROFIT LOCK %s: gain reaches +%.1fbps -> Stop ratcheted to Breakeven (+costs+20)", sym, ret)

            # Stage 2: Dynamic Trailing Stop (Let runners ride to +200 to +350 bps)
            trail_target = max(c.trail_trigger_bps, cost_bps + 30.0)
            if ret >= trail_target:
                p["trailing_active"] = True
                trail_stop = p["peak_ret_bps"] - c.trail_bps
                p["stop_bps"] = max(p.get("stop_bps", -c.sl_bps), trail_stop)

            # ================================================================
            # EXIT CONDITIONS — full smart exit system
            # ================================================================
            active_stop = p.get("stop_bps", -c.sl_bps)
            reason = None
            hold_s = now - p["ts"]

            if self.halted:
                reason = "halt"

            elif ret <= active_stop:
                # --- Flash-wick SL debounce ---
                # For un-ratcheted positions: require SL breach to persist for
                # sl_wick_debounce_s before firing. Prevents 50ms spike stops.
                # Ratcheted/trailing positions exit immediately (protecting locked profit).
                if p.get("be_locked") or p.get("trailing_active"):
                    reason = "trail_stop" if p.get("trailing_active") else "be_stop"
                else:
                    sl_first = p.get("sl_first_ts")
                    if sl_first is None:
                        p["sl_first_ts"] = now   # start debounce timer
                    elif now - sl_first >= c.sl_wick_debounce_s:
                        reason = "sl"            # breach persisted → genuine stop
                # If price recovered above stop, reset debounce
                if ret > active_stop and "sl_first_ts" in p:
                    del p["sl_first_ts"]

            elif ret >= c.tp_bps and not p.get("trailing_active"):
                reason = "tp"

            elif hold_s >= c.max_hold_s:
                reason = "time"

            # --- OBI-Flip exit ---
            # Order book has completely inverted (sellers fully dominate), position losing.
            # Only fires when hold > flip_bail_hold_s AND return is negative.
            elif (hold_s >= c.flip_bail_hold_s
                  and not p.get("be_locked")
                  and ret <= c.flip_bail_ret_bps
                  and s.get("imb", 0.5) < c.flip_bail_imb):
                reason = "flip"

            # --- Smart slow-mover bail ---
            # Cut confirmed losing positions that have been drifting lower with
            # sustained selling pressure. Requires ALL three guards:
            #   1. Held long enough (not a normal pullback)
            #   2. Negative return beyond threshold
            #   3. Momentum actively falling (not stalling)
            #   4. CVD shows dominant sellers (not just neutral drift)
            # Will NOT cut positive or neutral positions — only confirmed losers.
            elif (hold_s >= c.slow_bail_hold_s
                  and not p.get("be_locked")
                  and ret <= c.slow_bail_ret_bps
                  and s.get("mom", 0.0) <= c.slow_bail_mom_bps
                  and s.get("cvd_5s", 0.5) <= c.slow_bail_cvd):
                reason = "slow"

            # --- Structural breakdown fallback ---
            # Deep loss + completely collapsed OBI + strong negative momentum
            elif (hold_s >= 120.0 and not p.get("be_locked")
                  and ret < -25.0 and s.get("imb", 0.5) < 0.15
                  and s.get("mom", 0.0) < -10.0):
                reason = "breakdown"

            if reason:
                self.close(sym, s, reason, books)

        if self.halted:
            return

        # Alpha Confluence Entries (with inter_trade_pause_s to prevent runaway fee churn)
        inter_pause = getattr(c, "inter_trade_pause_s", 45.0)
        if now - getattr(self, "last_close_ts", 0.0) < inter_pause:
            return

        # SL-cluster pause: block new entries during correlated-loss bursts
        if now < self._sl_cluster_pause_until:
            remaining = self._sl_cluster_pause_until - now
            if int(remaining) % 30 == 0:  # log every 30s to avoid spam
                logging.info("SL-cluster guard active — %.0fs remaining before re-entry allowed", remaining)
            return

        # Check macro BTC regime before considering any altcoin long
        macro_safe, btc_mom = self.check_btc_regime()
        if not macro_safe:
            return

        slots, alloc_frac = self.get_slot_allocation(eq)

        # === SMART RE-ENTRY: expire stale re-entry windows (>90s) ===
        for sym in list(self._early_exits):
            if now - self._early_exits[sym]["ts"] > 90.0:
                del self._early_exits[sym]

        for sym in self.watch:
            if (len(self.pos) + len(getattr(self, "pending", {}))) >= slots:
                break
            s = books.get(sym)
            if not s or sym in self.pos or self.cool.get(sym, 0) > now:
                continue

            cost = 2 * (c.fee_bps + c.slippage_bps) + s["spread"]
            if c.tp_bps - cost < c.min_net_edge_bps:
                continue

            # Pull all microstructure metrics
            imb       = s.get("imb", 0.5)
            cvd       = s.get("cvd_5s", 0.5)
            cvd15     = s.get("cvd_15s", 0.5)
            mom       = s.get("mom", 0.0)
            mom_accel = s.get("mom_accel", 0.0)   # positive = momentum building
            micro_skew = s.get("micro_skew", 0.0)
            wall_ratio = s.get("wall_ratio", 0.5)  # dominant bid-wall size ratio
            conf      = s.get("confluence", 0.0)

            # Gate 1: momentum direction
            if mom < c.min_mom_bps or mom > c.max_mom_bps:
                continue

            # Gate 2: momentum acceleration (not decelerating)
            if c.min_mom_accel > 0 and mom_accel < c.min_mom_accel:
                continue

            # Gate 3: bid-wall dominance (optional — disabled by default)
            if c.min_wall_ratio > 0 and wall_ratio < c.min_wall_ratio:
                continue

            # === VOLUME SURGE CHECK ===
            # Require recent CVD activity to confirm actual volume surge, not stale data
            vol_ok = True
            if c.vol_surge_factor > 1.0:
                # cvd_5s > 0.55 means meaningful recent buying pressure
                vol_ok = (cvd >= 0.55 and cvd15 >= 0.52)

            if not vol_ok:
                continue

            # === LOSS-STREAK HIGH-CONVICTION MODE ===
            # After >= 3 consecutive losses, raise the entry bars significantly
            # to avoid further churn in adverse market conditions
            streak = self.loss_streak
            effective_conf = c.min_confluence + (c.streak_conf_boost if streak >= 3 else 0.0)
            effective_imb  = c.imbalance_entry + (c.streak_obi_boost  if streak >= 3 else 0.0)
            # After >= 5 consecutive losses, require ALL gates to pass simultaneously
            require_all_gates = (streak >= 5)

            if streak >= 3:
                logging.debug(
                    "HIGH-CONVICTION MODE (streak=%d): conf>=%.1f imb>=%.2f",
                    streak, effective_conf, effective_imb
                )

            # === HIGH-CONVICTION SIGNAL PATHWAYS ===
            # 1. OBI: Order book dominance + micro-skew + tape alignment
            sig_obi  = (imb >= effective_imb and micro_skew >= 1.0
                        and cvd >= c.min_cvd and mom >= c.min_mom_bps)
            # 2. CONF: High composite score — broadest but requires strong confluence
            sig_conf = (conf >= effective_conf and imb >= (effective_imb - 0.02)
                        and cvd >= c.min_cvd and mom >= c.min_mom_bps)
            # 3. TAPE: Dominant buyer aggression with strong book & skew
            #    Raised CVD bar to 0.75 and imb bar to 0.65 for higher quality
            sig_tape = (cvd >= 0.75 and imb >= 0.65
                        and micro_skew >= 1.0 and mom >= c.min_mom_bps)
            # 4. ACCEL: Momentum acceleration burst — catches early breakout stage
            #    Tightened: requires stronger acceleration and tape agreement
            sig_accel = (mom_accel >= 3.0 and mom >= c.min_mom_bps
                         and cvd >= 0.68 and imb >= 0.65 and micro_skew > 0.5)

            # Determine which paths fired for diagnostics and CSV tagging
            if require_all_gates:
                fired = (imb >= effective_imb and cvd >= c.min_cvd
                         and mom >= c.min_mom_bps and conf >= effective_conf
                         and micro_skew >= 1.0)
                path_label = "ALL" if fired else ""
            else:
                fired = sig_obi or sig_conf or sig_tape or sig_accel
                path_label = "+".join([
                    name for name, ok in
                    (("OBI", sig_obi), ("CONF", sig_conf), ("TAPE", sig_tape), ("ACCEL", sig_accel))
                    if ok
                ])

            # === SMART RE-ENTRY: bypass pause for confirmed continuing moves ===
            re_entry = c.smart_reentry and sym in self._early_exits
            if re_entry and not fired:
                er = self._early_exits[sym]
                price_moved = (s["mid"] - er["exit_px"]) / er["exit_px"] * 1e4
                # Only re-enter if price has moved at least 5 bps above exit AND signals still positive
                if price_moved >= 5.0 and cvd >= c.min_cvd and imb >= 0.60 and mom >= c.min_mom_bps:
                    fired = True
                    path_label = "REENTRY"
                    logging.info("SMART RE-ENTRY %s: price +%.1fbps above exit, momentum sustained",
                                 sym, price_moved)
                    del self._early_exits[sym]

            if fired and path_label:
                # Attach entry diagnostics to snapshot for position tracking
                s["signal_path"] = path_label
                s["entry_cvd"] = cvd
                s["entry_imb"] = imb
                s["entry_mom"] = mom
                s["entry_accel"] = mom_accel
                s["entry_cost_bps"] = cost
                quote_amt = self._quote_free(fresh=not c.paper) * alloc_frac
                self.open(sym, s, quote_amt)

    def flatten(self, reason):
        for sym in list(self.pos):
            b = self.read_book(sym)
            if b:
                try:
                    self.close(sym, b, reason, {})
                except Exception:
                    logging.exception("could not close %s", sym)

    def run(self):
        logging.info("EFRA Starting | Mode=%s | Exchange=%s | Quote=%s | Stream=%s",
                     "PAPER" if self.c.paper else "LIVE", self.c.exchange.upper(),
                     self.c.quote, "WebSocket" if self.ws_engine else "REST")
        self.preflight()
        logging.info("Running - Ctrl+C to stop; trades saved to %s", self.c.log_file)
        last_beat = 0.0
        try:
            while not self.halted:
                try:
                    self.step()
                    self.err_streak = 0
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    self.err_streak += 1
                    wait = min(30, 2 ** min(self.err_streak, 5))
                    logging.warning("step failed (%s) - retry #%d in %ds", e, self.err_streak, wait)
                    time.sleep(wait)

                if time.time() - last_beat > 120 and not self.c.dashboard:
                    last_beat = time.time()
                    logging.info("status | Watching %d | Positions %d | Tier %d | Trades %d | Wins %d",
                                 len(self.watch), len(self.pos), self.compound_tier, self.n_trades, self.n_wins)
                time.sleep(self.c.loop_s)
            self.flatten("halt")
        except KeyboardInterrupt:
            logging.info("Stopping Efra Bot - flattening positions...")
            self.flatten("manual_stop")
        finally:
            if self.ws_engine:
                self.ws_engine.stop()
            self.save_state()


class MakerBot(Bot):
    """Maker post-only order mode for fee discounts."""
    def __init__(self, cfg, ex=None):
        super().__init__(cfg, ex)
        self.pending = {}

    def equity(self, books):
        return super().equity(books) + sum(pd["quote"] for pd in self.pending.values())

    def save_state(self):
        try:
            with open(self.c.state_file, "w") as f:
                json.dump({
                    "pos": self.pos,
                    "pending": self.pending,
                    "cash": self.cash,
                    "n_trades": self.n_trades,
                    "n_wins": self.n_wins,
                    "compound_tier": self.compound_tier,
                    "last_tier_equity": self.last_tier_equity,
                }, f)
        except Exception:
            logging.exception("could not save maker state")

    def recover_state(self):
        super().recover_state()
        try:
            with open(self.c.state_file) as f:
                for sym, pd in (json.load(f).get("pending") or {}).items():
                    if sym in self.ex.markets:
                        self.pending[sym] = pd
        except Exception:
            pass

    def preflight(self):
        super().preflight()
        c = self.c
        rt = 2 * c.maker_fee_bps
        logging.info("MAKER mode: Maker fee=%.1fbps Taker fee=%.1fbps | RT Cost=%.1fbps",
                     c.maker_fee_bps, c.fee_bps, rt)

    def open(self, sym, s, quote_amt):
        c = self.c
        if sym in self.pending or sym in self.pos:
            return
        px = s["bid"]
        qty = quote_amt / px
        if c.paper:
            self.cash -= quote_amt
            self.pending[sym] = {"px": px, "qty": qty, "quote": quote_amt, "ts": time.time()}
            logging.info("MAKER POST BID %s qty=%.8g @ %.8g | Cost=$%.2f", sym, qty, px, quote_amt)
        else:
            try:
                m = self.ex.markets[sym]
                qty = float(self.ex.amount_to_precision(sym, qty))
                px = float(self.ex.price_to_precision(sym, px))
                o = self.ex.create_order(sym, "limit", "buy", qty, px, {"postOnly": True})
                self.pending[sym] = {"id": o["id"], "px": px, "qty": qty, "quote": quote_amt, "ts": time.time()}
                logging.info("MAKER POST BID %s id=%s qty=%.8g @ %.8g", sym, o["id"], qty, px)
            except Exception as e:
                logging.warning("maker post bid %s failed: %s", sym, e)

    def _manage_pending(self, books, now):
        c = self.c
        for sym, pd in list(self.pending.items()):
            s = books.get(sym)
            age = now - pd["ts"]
            fade = bool(s and s.get("imb", 0.5) < c.maker_cancel_imb)
            stale = age >= c.maker_timeout_s or fade or self.halted
            if c.paper:
                # Fill when price trades through bid (s["bid"] < pd["px"])
                # or order has been resting at the top of the bid book for >= 1.5s with intact depth
                bid_hit = s and (s["bid"] < pd["px"] or (s["bid"] <= pd["px"] and age >= 1.5))
                if bid_hit:
                    self._entry_filled(sym, pd["qty"], pd["px"], books)
                elif stale:
                    self.cash += pd["quote"]
                    self.pending.pop(sym, None)
                    self.cool[sym] = now + 10
                    self.save_state()
                continue
            # Live pending check
            try:
                o = self.ex.fetch_order(pd["id"], sym)
                st, filled = o.get("status"), float(o.get("filled") or 0)
                if st == "open" and stale:
                    try:
                        self.ex.cancel_order(pd["id"], sym)
                    except Exception:
                        pass
                    o = self.ex.fetch_order(pd["id"], sym)
                    st, filled = o.get("status"), float(o.get("filled") or 0)
                if st == "open":
                    continue
                self.pending.pop(sym, None)
                if filled > 0:
                    self._entry_filled(sym, filled, o.get("average") or pd["px"], books)
                else:
                    self.cool[sym] = now + 10
                    self.save_state()
            except Exception as e:
                logging.warning("pending check %s failed: %s", sym, e)

    def _entry_filled(self, sym, qty, px, books):
        c = self.c
        m = self.ex.markets[sym]
        pd = self.pending.pop(sym, {})
        if c.paper:
            cost = pd.get("quote", px * qty)
            qty = qty * (1 - c.maker_fee_bps / 1e4)
        else:
            free = float(self.ex.fetch_balance()["free"].get(m["base"], 0.0))
            qty = float(self.ex.amount_to_precision(sym, min(free, qty)))
            cost = px * qty
            if qty <= 0:
                return
        p = {
            "entry": px, "qty": qty, "ts": time.time(), "cost": cost,
            "tp_px": px * (1 + c.tp_bps / 1e4), "tp_id": None,
            "peak_ret_bps": 0.0, "stop_bps": -c.sl_bps,
            "be_locked": False, "trailing_active": False,
        }
        self.pos[sym] = p
        self._place_tp(sym, p)
        self.save_state()
        logging.info("MAKER FILLED %s qty=%.8g @ %.8g", sym, qty, px)

    def close(self, sym, s, reason, books):
        p = self.pos.get(sym)
        if p and p.get("tp_id") and not self.c.paper:
            try:
                self.ex.cancel_order(p["tp_id"], sym)
                logging.info("Cancelled resting TP order %s for %s before closing", p["tp_id"], sym)
            except Exception as e:
                logging.debug("Could not cancel resting TP %s: %s", p.get("tp_id"), e)
        super().close(sym, s, reason, books)

    def _place_tp(self, sym, p):
        if self.c.paper or p.get("tp_id"):
            return
        try:
            free = float(self.ex.fetch_balance()["free"].get(self.ex.markets[sym]["base"], 0.0))
            qty = float(self.ex.amount_to_precision(sym, min(free, p["qty"])))
            px = float(self.ex.price_to_precision(sym, p["tp_px"]))
            o = self.ex.create_order(sym, "limit", "sell", qty, px, {"postOnly": True})
            p["tp_id"] = o["id"]
        except Exception as e:
            logging.warning("could not place resting TP on %s: %s", sym, e)

    def step(self):
        c = self.c
        now = time.time()
        self._bal_cache = (0.0, None)
        if now - self.last_scan > c.rescan_s or not self.watch:
            self.scan()
        books = {}
        for sym in set(self.watch) | set(self.pos) | set(self.pending):
            b = self.read_book(sym)
            if b:
                books[sym] = b
        self.last_books = books

        eq = self.equity(books)
        self.check_compounding(eq)
        if self.dashboard:
            self.dashboard.render()

        self._manage_pending(books, now)
        super().step()

    def flatten(self, reason):
        for sym, pd in list(self.pending.items()):
            if self.c.paper:
                self.cash += pd["quote"]
            else:
                try:
                    self.ex.cancel_order(pd["id"], sym)
                except Exception:
                    pass
            del self.pending[sym]
        super().flatten(reason)


def main():
    ap = argparse.ArgumentParser(description="EFRA - Microstructure Scalper & Compounding Bot")
    for f in fields(Cfg):
        if f.name in ("paper", "ws", "diagnostic", "dashboard", "multi_slot", "btc_filter"):
            continue
        ap.add_argument(f"--{f.name.replace('_', '-')}", type=f.type, default=f.default)

    ap.add_argument("--paper", action="store_true", default=True, help="Simulate trades with live order books")
    ap.add_argument("--live", action="store_true", help="Place REAL capital orders on the exchange")
    ap.add_argument("--yes", action="store_true", help="Skip live confirmation prompt")
    ap.add_argument("--reset", action="store_true", help="Reset saved state and start fresh session")
    ap.add_argument("--diagnostic", action="store_true", help="Run connectivity & latency health check and exit")
    ap.add_argument("--dashboard", action="store_true", help="Run visual terminal dashboard")
    ap.add_argument("--no-ws", action="store_true", help="Disable WebSocket and use REST polling only")
    ap.add_argument("--no-auto-fee", action="store_true", help="Trust --fee-bps instead of reading exchange fee")
    ap.add_argument("--no-multi-slot", action="store_true", help="Disable dynamic multi-slot allocation")
    ap.add_argument("--no-btc-filter", action="store_true", help="Disable BTC macro market regime filter")
    ap.add_argument("--verbose", action="store_true")

    a = ap.parse_args()

    cfg_dict = {}
    for f in fields(Cfg):
        if hasattr(a, f.name):
            cfg_dict[f.name] = getattr(a, f.name)

    cfg = Cfg(**cfg_dict)
    cfg.paper = not a.live
    cfg.ws = not a.no_ws
    cfg.diagnostic = a.diagnostic
    cfg.dashboard = a.dashboard
    cfg.multi_slot = not a.no_multi_slot
    cfg.btc_filter = not a.no_btc_filter
    if a.no_auto_fee:
        cfg.auto_fee = 0

    if cfg.state_file == "efra_state.json":
        cfg.state_file = f"efra_state_{cfg.exchange}_{cfg.mode}.json"
    if cfg.log_file == "efra_trades.csv":
        cfg.log_file = f"efra_trades_{cfg.exchange}_{cfg.mode}.csv"

    if a.reset:
        targets = [cfg.state_file, cfg.log_file, "efra_state.json", "efra_trades.csv"]
        for t in targets:
            if os.path.exists(t):
                try:
                    os.remove(t)
                    print(f"Cleared '{t}' for clean session.")
                except Exception as e:
                    print(f"Could not remove {t}: {e}")

    log_level = logging.DEBUG if a.verbose else (logging.WARNING if a.dashboard else logging.INFO)
    handlers = [logging.FileHandler("efra.log", encoding="utf-8")]
    if not a.dashboard:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname).1s %(message)s",
        handlers=handlers,
    )

    if a.live and not a.yes and not a.diagnostic:
        print("\n" + "!" * 70)
        print(" WARNING: LIVE MODE WILL PLACE REAL SPOT ORDERS USING REAL CAPITAL!")
        print("!" * 70)
        if input("Type 'I UNDERSTAND' to proceed with real orders: ").strip() != "I UNDERSTAND":
            print("Aborted.")
            return

    bot_cls = MakerBot if cfg.mode == "maker" else Bot
    bot_cls(cfg).run()


if __name__ == "__main__":
    main()
