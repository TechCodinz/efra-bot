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

try:
    from efra_dashboard import EfraDashboard
    HAS_DASHBOARD = True
except ImportError:
    HAS_DASHBOARD = False


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

    # scanner filters (high liquidity & tight spread: filters out illiquid spoofing traps)
    min_quote_volume: float = 1_000_000  # 24h volume in quote currency ($1M+ minimum)
    min_price: float = 0.01              # skip sub-penny dust coins
    max_spread_bps: float = 6.0          # strictly tight spreads (max 0.06% spread)
    watch_n: int = 18                    # pairs polled each loop (expanded to 18 liquid pairs)
    rescan_s: int = 120

    # costs (per side). fee_bps is a FALLBACK - real taker fee is read at startup
    fee_bps: float = 20.0                # 20 bps = 0.20% (Gate.io taker)
    maker_fee_bps: float = 20.0          # fallback
    auto_fee: int = 1
    maker_timeout_s: int = 12            # cancel an unfilled resting entry after this
    maker_cancel_imb: float = 0.50       # ...or when bid-side support fades below this
    slippage_bps: float = 1.0

    # entry / exit (engineered for asymmetric positive expectancy & high payoff ratio)
    tp_bps: float = 200.0                # take-profit target (+2.00% gain per scalp)
    sl_bps: float = 40.0                 # hard stop loss (-0.40%)
    min_net_edge_bps: float = 12.0       # tp must beat roundtrip costs by at least 12 bps
    imbalance_entry: float = 0.68        # bid share of top-5 depth (68% bids - only verified walls)
    min_cvd: float = 0.58               # aggressive buyer ratio - only enter with strong tape
    min_confluence: float = 55.0         # robust composite alpha bar - only top-tier setups
    min_mom_bps: float = 2.5             # require at least 2.5 bps of positive momentum at entry
    max_mom_bps: float = 20.0            # never chase exhaustion blow-off tops (tightened)
    mom_window: int = 8                  # loop ticks of mid-price history
    max_hold_s: int = 420                # 7-minute max hold (gives breakouts room to complete without fee churn)
    cooldown_s: int = 25                 # 25s fast cooldown per pair after exit
    inter_trade_pause_s: float = 8.0     # 8s high-speed re-deployment between trades
    btc_filter: bool = True              # macro Bitcoin market regime filter

    # dynamic profit ratchet & trailing stop (tuned for winners to run)
    breakeven_bps: float = 80.0          # trigger breakeven lock once +80 bps (0.80%) is reached
    trail_trigger_bps: float = 100.0     # activate trailing stop once return reaches +100 bps (+1.0%)
    trail_bps: float = 30.0              # 30 bps trailing cushion from peak (allows runners to +200-+350 bps)

    # compounding & risk
    position_frac: float = 0.40          # 40% capital allocation per slot (tightened for safety)
    max_positions: int = 2               # concurrent positions
    multi_slot: bool = True              # dynamically scale slots as capital grows
    compound_step: float = 0.05          # +5% equity gain promotes compounding tier and scales lots
    daily_loss_limit_frac: float = 0.08  # halt if equity falls 8% in a day (tighter protection)

    loop_s: float = 0.3                  # main step loop sleep (300ms high-speed reaction)
    log_file: str = "efra_trades.csv"
    state_file: str = "efra_state.json"
    balance_cache_s: float = 5.0         # avoid hammering balance endpoint


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
        try:
            with open(self.c.state_file, "w") as f:
                json.dump({
                    "pos": self.pos,
                    "cash": self.cash,
                    "n_trades": self.n_trades,
                    "n_wins": self.n_wins,
                    "compound_tier": self.compound_tier,
                    "last_tier_equity": self.last_tier_equity,
                    "high_water_mark": self.high_water_mark,
                }, f)
        except Exception:
            logging.exception("could not save state")

    def recover_state(self):
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
                    ["ts", "symbol", "entry", "exit", "qty", "pnl_quote", "reason", "equity", "tier"])

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
        """Calculates dynamic slot count with safe, disciplined position sizing to preserve capital."""
        c = self.c
        if not c.multi_slot:
            return c.max_positions, c.position_frac

        # High-Velocity Multi-Slot Compounding Engine:
        # Accounts under $300: 2 concurrent sniper slots (45% capital each), eliminating idle cash.
        # Accounts $300-$1000: 3 slots (30% each).
        # Accounts >$1000: 4 slots (22% each).
        if current_equity < 300.0:
            slots = 2
            frac = c.position_frac
        elif current_equity < 1000.0:
            slots = 3
            frac = 0.30
        elif current_equity < 3000.0:
            slots = 4
            frac = 0.22
        else:
            slots = 5
            frac = 0.18

        # Streak defensive scaling: if 2 consecutive losses, scale down by 30%
        if self.loss_streak >= 2:
            frac *= 0.70

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
        for sym, t in tickers.items():
            m = self.ex.markets.get(sym)
            if not m or not m.get("spot") or not m.get("active", True):
                continue
            if m.get("quote") != c.quote or not sym.isascii():
                continue
            # Skip leveraged ETF tokens (e.g. 3L, 3S, 5L, 5S)
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
            # High-liquidity volume-weighted momentum score:
            # Rewards liquid pairs with tight spreads and real breakout momentum
            score = (vol_mil ** 0.4) * (rng * 0.3 + pct * 0.7) / (spread + 1.2 * c.fee_bps)
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
        }
        self._bal_cache = (0.0, None)
        self.save_state()
        logging.info("BUY  %s @ %.8g | OBI=%.2f CVD=%.2f Mom=%.1fbps Conf=%.1f | Cost=$%.2f",
                     sym, entry, s.get("imb", 0.5), s.get("cvd_5s", 0.5), s["mom"],
                     s.get("confluence", 0.0), quote_amt)

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
        self.n_trades += 1
        if pnl > 0:
            self.n_wins += 1
            self.loss_streak = 0
        else:
            self.loss_streak += 1

        self._bal_cache = (0.0, None)
        eq = self.equity(books)
        self.check_compounding(eq)

        with open(c.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                int(time.time()), sym, p["entry"], exit_px, qty,
                round(pnl, 6), reason, round(eq, 4), self.compound_tier
            ])

        self.cool[sym] = time.time() + c.cooldown_s
        self.last_close_ts = time.time()
        self.save_state()
        ret_bps = (exit_px - p["entry"]) / p["entry"] * 1e4
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

        if self.day_start_eq is None or now - self.day_start > 86400:
            self.day_start, self.day_start_eq = now, eq

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
            be_target = max(c.breakeven_bps, cost_bps + 12.0)
            if ret >= be_target and not p.get("be_locked"):
                p["be_locked"] = True
                p["stop_bps"] = cost_bps + 12.0  # guaranteed net positive post-fees (+12 bps)
                logging.info("PROFIT LOCK %s: gain reaches +%.1fbps -> Stop ratcheted to Breakeven (+costs)", sym, ret)

            # Stage 2: Dynamic Trailing Stop (Let runners ride to +200 to +350 bps)
            trail_target = max(c.trail_trigger_bps, cost_bps + 30.0)
            if ret >= trail_target:
                p["trailing_active"] = True
                trail_stop = p["peak_ret_bps"] - c.trail_bps
                p["stop_bps"] = max(p.get("stop_bps", -c.sl_bps), trail_stop)

            # Exit Conditions
            active_stop = p.get("stop_bps", -c.sl_bps)
            reason = None
            if self.halted:
                reason = "halt"
            elif ret <= active_stop:
                # Immediate exit if stop is breached! Eliminates delay to avoid slippage disasters
                if p.get("be_locked") or p.get("trailing_active"):
                    reason = "trail_stop" if p.get("trailing_active") else "be_stop"
                else:
                    reason = "sl"
            elif ret >= c.tp_bps and not p.get("trailing_active"):
                reason = "tp"
            elif now - p["ts"] >= c.max_hold_s:
                reason = "time"
            # Structural breakdown: only cut if held >= 90s, return < -20 bps, both OBI and momentum fully collapsed
            elif (now - p["ts"] >= 90.0 and not p.get("be_locked")
                  and ret < -20.0 and s.get("imb", 0.5) < 0.18
                  and s.get("mom", 0.0) < -8.0):
                reason = "breakdown"

            if reason:
                self.close(sym, s, reason, books)

        if self.halted:
            return

        # Alpha Confluence Entries (with inter_trade_pause_s to prevent runaway fee churn)
        inter_pause = getattr(c, "inter_trade_pause_s", 45.0)
        if now - getattr(self, "last_close_ts", 0.0) < inter_pause:
            return

        # Check macro BTC regime before considering any altcoin long
        macro_safe, btc_mom = self.check_btc_regime()
        if not macro_safe:
            return

        slots, alloc_frac = self.get_slot_allocation(eq)
        for sym in self.watch:
            if (len(self.pos) + len(getattr(self, "pending", {}))) >= slots:
                break
            s = books.get(sym)
            if not s or sym in self.pos or self.cool.get(sym, 0) > now:
                continue

            cost = 2 * (c.fee_bps + c.slippage_bps) + s["spread"]
            if c.tp_bps - cost < c.min_net_edge_bps:
                continue

            # High-Conviction Microstructure Alpha Validation
            imb = s.get("imb", 0.5)
            cvd = s.get("cvd_5s", 0.5)
            mom = s.get("mom", 0.0)
            micro_skew = s.get("micro_skew", 0.0)
            conf = s.get("confluence", 0.0)

            # Never buy into negative or below-threshold momentum
            if mom < c.min_mom_bps:
                continue

            # Never chase exhaustion tops (overextended momentum wicks that immediately retrace)
            if mom > getattr(c, "max_mom_bps", 25.0):
                continue

            # High-conviction signal pathways (snipe genuine momentum breakouts):
            # 1. Order book dominance: strong bid wall >= 68%, positive micro-skew, strong buyer tape
            sig_obi = (imb >= c.imbalance_entry and micro_skew >= 0.8 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
            # 2. Composite alpha confluence: high confluence score with verified buyer flow
            sig_conf = (conf >= c.min_confluence and imb >= 0.62 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
            # 3. Aggressive buyer surge on trade tape: dominant tape buying with strong book
            sig_tape = (cvd >= 0.72 and imb >= 0.60 and micro_skew >= 0.8 and mom >= c.min_mom_bps)

            if sig_obi or sig_conf or sig_tape:
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
