#!/usr/bin/env python3
"""
EFRA RESEARCH v2 -- rich live recorder with real trade tape, CVD, and event analysis.

WHY v2 EXISTS:
  The v1 recorder (efra_research.py) uses REST-polled L2 only.
  The edge audit showed L2 signals produce only ~2 bps gross forward move
  vs ~43 bps round-trip cost. That gap cannot be closed by L2 tuning.
  This recorder captures real trade aggressor flow (CVD) via WebSocket
  so TAPE and ACCEL signal paths can be properly validated.

WHAT IT RECORDS (per 1-second snapshot, per symbol):
  - L2 imbalance at 5 and 10 levels
  - Micro-price skew
  - Wall ratio (production formula from efra_stream.py)
  - Momentum at 5s / 15s / 30s
  - Momentum acceleration (5s derivative)
  - Spread and spread change vs 5s ago
  - REAL CVD at 5s / 15s / 30s from live trade tape
  - Buy/sell notional and trade count per window
  - Realized volatility (30s)
  - All EFRA signal paths that would fire (OBI/CONF/TAPE/ACCEL)
  - Round-trip cost estimate
  - Forward mid-price returns at 5/15/30/60/120/180s (filled async)
  - MFE / MAE to 60s

USAGE:
  # Record (run on VPS, alongside existing research sidecar):
  python efra_research_v2.py record \\
      --exchange gate --quote USDT --hours 96 \\
      --db /opt/app-platform/state/efra-research/efra_v2.db

  # Analyze (run any time, even while recording):
  python efra_research_v2.py analyze \\
      --db /opt/app-platform/state/efra-research/efra_v2.db \\
      --fee-bps 20 --slippage-bps 1

  # Check DB progress:
  python efra_research_v2.py status \\
      --db /opt/app-platform/state/efra-research/efra_v2.db

DO NOT:
  - Change efra_bot.py thresholds.
  - Re-enable PAPER execution before an OOS candidate passes.
  - Touch VPS/Docker/Caddy/UI infrastructure.
"""

import argparse
import asyncio
import bisect
import logging
import math
import sqlite3
import statistics
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

# --------------------------------------------------------------------------
# PRODUCTION EFRA THRESHOLDS -- used for signal classification only
# Do NOT change these without bumping ENTRY_POLICY_VERSION
# --------------------------------------------------------------------------
EFRA_OBI_MIN   = 0.72
EFRA_MOM_MIN   = 3.5
EFRA_MOM_MAX   = 18.0
EFRA_MICRO_MIN = 1.0
EFRA_CONF_MIN  = 62.0
EFRA_CVD_MIN   = 0.65

FEE_BPS        = 20.0   # Gate taker fee per side (bps)
SLIP_BPS       = 1.0    # Estimated slippage per side (bps)
MAKER_FEE_BPS  = 20.0   # Gate maker fee per side (bps)

HORIZONS       = (5, 15, 30, 60, 120, 180)
MIN_SAMPLES        = 50
TSTAT_MIN          = 2.0
PROMOTION_MIN_OOS  = 200
PROMOTION_MIN_SYMS = 2
MIN_SYM_OOS        = 20

# --------------------------------------------------------------------------
# DB SCHEMA
# --------------------------------------------------------------------------
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS snaps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL    NOT NULL,
    sym             TEXT    NOT NULL,

    -- Top-of-book
    bid             REAL,
    ask             REAL,
    mid             REAL,
    spread_bps      REAL,

    -- L2 depth (5 and 10 levels)
    bq1             REAL,
    aq1             REAL,
    bv5             REAL,
    av5             REAL,
    bv10            REAL,
    av10            REAL,
    imb5            REAL,
    imb10           REAL,
    micro_skew      REAL,
    wall_ratio      REAL,

    -- Momentum (bps)
    mom_5s          REAL,
    mom_15s         REAL,
    mom_30s         REAL,
    mom_accel       REAL,

    -- Spread dynamics
    spread_change_5s REAL,

    -- CVD from real trade tape
    cvd_5s          REAL,
    cvd_15s         REAL,
    cvd_30s         REAL,
    buy_vol_5s      REAL,
    sell_vol_5s     REAL,
    n_trades_5s     INTEGER,
    n_trades_30s    INTEGER,

    -- Realized volatility proxy (30s rolling)
    rvol_30s        REAL,

    -- Signal paths (1=fires, 0=does not)
    sig_obi         INTEGER,
    sig_conf        INTEGER,
    sig_tape        INTEGER,
    sig_accel       INTEGER,
    sig_any         INTEGER,

    -- Cost
    cost_bps        REAL,

    -- Forward mid-price returns (bps) -- filled retroactively
    fwd_5s          REAL,
    fwd_15s         REAL,
    fwd_30s         REAL,
    fwd_60s         REAL,
    fwd_120s        REAL,
    fwd_180s        REAL,
    mfe_60s         REAL,
    mae_60s         REAL,
    filled          INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sym_ts   ON snaps(sym, ts);
CREATE INDEX IF NOT EXISTS idx_unfilled ON snaps(filled, ts);
CREATE INDEX IF NOT EXISTS idx_ts       ON snaps(ts);
"""

# --------------------------------------------------------------------------
# RECORDER
# --------------------------------------------------------------------------

class ResearchRecorderV2:
    """
    Records rich microstructure snapshots with real trade tape to SQLite.
    Architecture mirrors LiveStreamEngine: asyncio WS loop in background
    thread, snapshot writes from main thread, fill loop in fill thread.
    """

    def __init__(self, args):
        self.args     = args
        self.db_path  = args.db
        self._syms    = []

        # Shared state -- protected by _lock
        self._lock    = threading.Lock()
        self._books   = {}   # sym -> latest L2 snapshot dict
        self._trades  = defaultdict(lambda: deque())   # sym -> (ts, side, notional, price)
        self._mids    = defaultdict(lambda: deque())   # sym -> (ts, mid)

        # Threading
        self._ws_thread   = None
        self._fill_thread = None
        self._loop        = None
        self._stop        = threading.Event()
        self._ex          = None

        # Batch insert buffer
        self._pending = []
        self._pending_lock = threading.Lock()

        self._db = None

    # ------------------------------------------------------------------
    # DB setup
    # ------------------------------------------------------------------

    def _init_db(self):
        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.executescript(SCHEMA)
        self._db.commit()
        logging.info("DB ready: %s", self.db_path)

    # ------------------------------------------------------------------
    # Symbol selection (from efra_research.py pick_symbols logic)
    # ------------------------------------------------------------------

    def _pick_syms(self, ex):
        try:
            tickers = ex.fetch_tickers()
        except Exception as e:
            logging.error("fetch_tickers failed: %s", e)
            return []

        rows = []
        q = self.args.quote
        for sym, t in tickers.items():
            m = ex.markets.get(sym)
            if not m or not m.get("spot") or m.get("quote") != q:
                continue
            if not m.get("active", True):
                continue
            bid, ask = t.get("bid"), t.get("ask")
            qv = t.get("quoteVolume") or 0
            hi, lo, last = t.get("high"), t.get("low"), t.get("last")
            if not all([bid, ask, hi, lo, last, qv]):
                continue
            if qv < self.args.min_vol:
                continue
            spread = (ask - bid) / ((ask + bid) / 2) * 1e4
            if spread > self.args.max_spread_bps:
                continue
            score = (hi - lo) / last * 1e4 / (spread + 1.0)
            rows.append((score, sym))

        rows.sort(reverse=True)
        syms = [s for _, s in rows[:self.args.pairs]]
        logging.info("Selected %d pairs: %s", len(syms), ", ".join(syms))
        return syms

    # ------------------------------------------------------------------
    # CVD calculation (extended: 5s / 15s / 30s)
    # ------------------------------------------------------------------

    def _calc_cvd(self, sym, now):
        """Returns (cvd_5s, cvd_15s, cvd_30s, buy_5s, sell_5s, n5, n30).
        All values computed from live trade tape inside lock (call with lock held)."""
        tape = self._trades[sym]
        t5, t15, t30 = now - 5.0, now - 15.0, now - 30.0

        buy5 = sell5 = buy15 = sell15 = buy30 = sell30 = 0.0
        n5 = n30 = 0

        for tr_ts, side, notional, _ in tape:
            if tr_ts < t30:
                continue
            if side == "buy":
                buy30 += notional
            else:
                sell30 += notional
            n30 += 1
            if tr_ts >= t15:
                if side == "buy": buy15 += notional
                else: sell15 += notional
            if tr_ts >= t5:
                if side == "buy": buy5 += notional
                else: sell5 += notional
                n5 += 1

        tot5  = buy5 + sell5
        tot15 = buy15 + sell15
        tot30 = buy30 + sell30

        return (
            buy5 / tot5   if tot5  > 0 else 0.5,
            buy15 / tot15 if tot15 > 0 else 0.5,
            buy30 / tot30 if tot30 > 0 else 0.5,
            buy5, sell5, n5, n30,
        )

    # ------------------------------------------------------------------
    # Compute full snapshot metrics
    # ------------------------------------------------------------------

    def _compute_snap(self, sym, now):
        """Reads current shared state and computes all metrics for one snapshot."""
        with self._lock:
            book = self._books.get(sym)
            if not book:
                return None
            age = now - book.get("_ts", 0)
            if age > 5.0:
                return None  # stale

            bid = book["bid"]; ask = book["ask"]; mid = book["mid"]
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            spread_bps = book["spread_bps"]
            bq1 = book["bq1"]; aq1 = book["aq1"]
            bv5 = book["bv5"]; av5 = book["av5"]
            bv10 = book["bv10"]; av10 = book["av10"]
            imb5 = book["imb5"]; imb10 = book["imb10"]
            micro_skew = book["micro_skew"]
            wall_ratio = book["wall_ratio"]

            # CVD
            cvd_5s, cvd_15s, cvd_30s, buy_5s, sell_5s, n5, n30 = self._calc_cvd(sym, now)

            # Momentum from mid history
            mids_hist = list(self._mids[sym])
            mom_5s = mom_15s = mom_30s = mom_accel = 0.0
            rvol_30s = 0.0

            if mids_hist:
                t5  = now - 5.0
                t15 = now - 15.0
                t30 = now - 30.0

                j5  = next((i for i, (t, _) in enumerate(mids_hist) if t >= t5),  None)
                j15 = next((i for i, (t, _) in enumerate(mids_hist) if t >= t15), None)
                j30 = next((i for i, (t, _) in enumerate(mids_hist) if t >= t30), None)

                if j5 is not None and j5 > 0:
                    m0 = mids_hist[j5-1][1]
                    mom_5s = (mid / m0 - 1) * 1e4 if m0 > 0 else 0.0
                if j15 is not None and j15 > 0:
                    m0 = mids_hist[j15-1][1]
                    mom_15s = (mid / m0 - 1) * 1e4 if m0 > 0 else 0.0
                if j30 is not None and j30 > 0:
                    m0 = mids_hist[j30-1][1]
                    mom_30s = (mid / m0 - 1) * 1e4 if m0 > 0 else 0.0

                    # Realized vol: std of 1s mid-price log returns over 30s window
                    window_mids = [m for t, m in mids_hist if t >= t30]
                    if len(window_mids) >= 5:
                        rets = [(window_mids[i] / window_mids[i-1] - 1) * 1e4
                                for i in range(1, len(window_mids))]
                        rvol_30s = statistics.pstdev(rets) if len(rets) > 1 else 0.0

                # Momentum acceleration: mom_5s now vs mom_5s ~5s ago
                t10 = now - 10.0
                j10 = next((i for i, (t, _) in enumerate(mids_hist) if t >= t10), None)
                if j10 is not None and j10 > 0 and j5 is not None and j5 > 0:
                    # mom 5s ago: mid_5s_ago vs mid_10s_ago
                    mid_5s_ago = mids_hist[j5-1][1] if j5 > 0 else mid
                    mid_10s_ago = mids_hist[j10-1][1] if j10 > 0 else mid_5s_ago
                    prev_mom_5s = (mid_5s_ago / mid_10s_ago - 1) * 1e4 if mid_10s_ago > 0 else 0.0
                    mom_accel = mom_5s - prev_mom_5s

            # Spread change vs 5s ago
            spread_change_5s = 0.0
            t5_ = now - 5.0
            spreads_hist = book.get("_spreads_hist", [])
            if spreads_hist:
                old_spread = next((s for t, s in spreads_hist if t >= t5_), None)
                if old_spread is not None:
                    spread_change_5s = spread_bps - old_spread

        # Signal classification (computed outside lock)
        conf_score = _confluence(imb5, cvd_5s, micro_skew, mom_5s)

        sig_obi  = int(imb5 >= EFRA_OBI_MIN and micro_skew >= EFRA_MICRO_MIN
                       and EFRA_MOM_MIN <= mom_5s <= EFRA_MOM_MAX)
        sig_conf = int(conf_score >= EFRA_CONF_MIN and imb5 >= (EFRA_OBI_MIN - 0.02)
                       and EFRA_MOM_MIN <= mom_5s <= EFRA_MOM_MAX)
        sig_tape = int(cvd_5s >= 0.75 and imb5 >= 0.65
                       and micro_skew >= EFRA_MICRO_MIN and EFRA_MOM_MIN <= mom_5s <= EFRA_MOM_MAX)
        sig_accel = int(mom_accel >= 3.0 and EFRA_MOM_MIN <= mom_5s <= EFRA_MOM_MAX
                        and cvd_5s >= 0.68 and imb5 >= 0.65 and micro_skew > 0.5)
        sig_any  = int(bool(sig_obi or sig_conf or sig_tape or sig_accel))
        cost_bps = spread_bps + 2 * FEE_BPS + 2 * SLIP_BPS

        return {
            "ts": now, "sym": sym,
            "bid": bid, "ask": ask, "mid": mid, "spread_bps": spread_bps,
            "bq1": bq1, "aq1": aq1,
            "bv5": bv5, "av5": av5, "bv10": bv10, "av10": av10,
            "imb5": imb5, "imb10": imb10,
            "micro_skew": micro_skew, "wall_ratio": wall_ratio,
            "mom_5s": mom_5s, "mom_15s": mom_15s, "mom_30s": mom_30s,
            "mom_accel": mom_accel, "spread_change_5s": spread_change_5s,
            "cvd_5s": cvd_5s, "cvd_15s": cvd_15s, "cvd_30s": cvd_30s,
            "buy_vol_5s": buy_5s, "sell_vol_5s": sell_5s,
            "n_trades_5s": n5, "n_trades_30s": n30,
            "rvol_30s": rvol_30s,
            "sig_obi": sig_obi, "sig_conf": sig_conf,
            "sig_tape": sig_tape, "sig_accel": sig_accel,
            "sig_any": sig_any, "cost_bps": cost_bps,
        }

    # ------------------------------------------------------------------
    # Snapshot loop (main thread: fires every 1s)
    # ------------------------------------------------------------------

    def _snapshot_loop(self):
        insert_sql = """
        INSERT INTO snaps (
            ts, sym, bid, ask, mid, spread_bps,
            bq1, aq1, bv5, av5, bv10, av10, imb5, imb10,
            micro_skew, wall_ratio,
            mom_5s, mom_15s, mom_30s, mom_accel, spread_change_5s,
            cvd_5s, cvd_15s, cvd_30s,
            buy_vol_5s, sell_vol_5s, n_trades_5s, n_trades_30s,
            rvol_30s,
            sig_obi, sig_conf, sig_tape, sig_accel, sig_any, cost_bps
        ) VALUES (
            :ts, :sym, :bid, :ask, :mid, :spread_bps,
            :bq1, :aq1, :bv5, :av5, :bv10, :av10, :imb5, :imb10,
            :micro_skew, :wall_ratio,
            :mom_5s, :mom_15s, :mom_30s, :mom_accel, :spread_change_5s,
            :cvd_5s, :cvd_15s, :cvd_30s,
            :buy_vol_5s, :sell_vol_5s, :n_trades_5s, :n_trades_30s,
            :rvol_30s,
            :sig_obi, :sig_conf, :sig_tape, :sig_accel, :sig_any, :cost_bps
        )"""

        batch = []
        last_commit  = time.time()
        last_log     = time.time()
        n_snaps      = 0
        n_with_cvd   = 0
        n_signal_any = 0
        end_ts = time.time() + self.args.hours * 3600

        logging.info("Snapshot loop started. Will record until %s UTC",
                     datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"))

        while not self._stop.is_set() and time.time() < end_ts:
            t0 = time.time()
            for sym in self._syms:
                snap = self._compute_snap(sym, t0)
                if snap is None:
                    continue
                batch.append(snap)
                n_snaps += 1
                if snap["cvd_5s"] != 0.5 or snap["cvd_15s"] != 0.5:
                    n_with_cvd += 1
                if snap["sig_any"]:
                    n_signal_any += 1

            # Commit every 10s
            if batch and (time.time() - last_commit >= 10.0):
                try:
                    self._db.executemany(insert_sql, batch)
                    self._db.commit()
                    batch.clear()
                    last_commit = time.time()
                except Exception as e:
                    logging.warning("DB insert error: %s", e)

            # Status log every 60s
            if time.time() - last_log >= 60.0:
                cvd_pct = 100.0 * n_with_cvd / max(n_snaps, 1)
                sig_pct = 100.0 * n_signal_any / max(n_snaps, 1)
                logging.info(
                    "Recorded %d snaps | CVD_active=%.0f%% | Signal_any=%.0f%% | "
                    "Pairs=%d | %.1fh remaining",
                    n_snaps, cvd_pct, sig_pct, len(self._syms),
                    (end_ts - time.time()) / 3600
                )
                last_log = time.time()

            # Trim trade tape (keep only 60s of data per symbol)
            cutoff = t0 - 62.0
            with self._lock:
                for sym in self._syms:
                    tape = self._trades[sym]
                    while tape and tape[0][0] < cutoff:
                        tape.popleft()
                    # Trim mid history (keep 65s)
                    mids = self._mids[sym]
                    while mids and mids[0][0] < t0 - 65.0:
                        mids.popleft()

            elapsed = time.time() - t0
            time.sleep(max(0.0, 1.0 - elapsed))

        # Final flush
        if batch:
            try:
                self._db.executemany(insert_sql, batch)
                self._db.commit()
            except Exception:
                pass
        logging.info("Snapshot loop ended. Total: %d snaps", n_snaps)

    # ------------------------------------------------------------------
    # Forward fill loop (background thread: fills fwd_* for mature rows)
    # ------------------------------------------------------------------

    def _fill_loop(self):
        """Retroactively fills forward returns for rows older than 185s."""
        logging.info("Fill loop started.")
        while not self._stop.is_set():
            time.sleep(60.0)   # run fill every 60s
            if self._stop.is_set():
                break
            try:
                self._fill_pass()
            except Exception as e:
                logging.warning("Fill loop error: %s", e)

        # Final fill pass
        try:
            self._fill_pass()
        except Exception:
            pass
        logging.info("Fill loop ended.")

    def _fill_pass(self):
        """One fill pass: for each unfilled row older than 185s, look up future prices."""
        cutoff = time.time() - 185.0
        # Use a separate DB connection for fill (WAL mode allows concurrent r/w)
        fill_db = sqlite3.connect(self.db_path)
        fill_db.execute("PRAGMA journal_mode=WAL;")

        rows = fill_db.execute(
            "SELECT id, sym, ts, mid FROM snaps WHERE filled=0 AND ts < ? ORDER BY sym, ts LIMIT 5000",
            (cutoff,)
        ).fetchall()

        if not rows:
            fill_db.close()
            return

        updates = []
        for snap_id, sym, ts, mid0 in rows:
            if not mid0 or mid0 <= 0:
                fill_db.execute("UPDATE snaps SET filled=-1 WHERE id=?", (snap_id,))
                continue

            fwd_vals = {}
            mfe = mae = None

            for h in HORIZONS:
                # Find nearest snapshot at ts+h
                near = fill_db.execute(
                    """SELECT mid FROM snaps
                       WHERE sym=? AND ts BETWEEN ? AND ? AND mid IS NOT NULL
                       ORDER BY ABS(ts - ?) LIMIT 1""",
                    (sym, ts + h - max(5.0, h * 0.3), ts + h + max(5.0, h * 0.3),
                     ts + h)
                ).fetchone()
                fwd_vals[h] = (near[0] / mid0 - 1) * 1e4 if near else None

            # MFE/MAE: use all snapshots in [ts, ts+60]
            window_rows = fill_db.execute(
                "SELECT mid FROM snaps WHERE sym=? AND ts BETWEEN ? AND ? AND mid IS NOT NULL",
                (sym, ts + 0.5, ts + 62.0)
            ).fetchall()
            if window_rows:
                mids_w = [r[0] for r in window_rows]
                mfe = max((m / mid0 - 1) * 1e4 for m in mids_w)
                mae = min((m / mid0 - 1) * 1e4 for m in mids_w)

            updates.append((
                fwd_vals.get(5),  fwd_vals.get(15), fwd_vals.get(30),
                fwd_vals.get(60), fwd_vals.get(120), fwd_vals.get(180),
                mfe, mae, snap_id
            ))

        if updates:
            fill_db.executemany(
                """UPDATE snaps SET
                   fwd_5s=?, fwd_15s=?, fwd_30s=?, fwd_60s=?, fwd_120s=?, fwd_180s=?,
                   mfe_60s=?, mae_60s=?, filled=1
                   WHERE id=?""",
                updates
            )
            fill_db.commit()
            logging.info("Filled %d rows with forward returns.", len(updates))

        fill_db.close()

    # ------------------------------------------------------------------
    # WebSocket async loop (background thread)
    # ------------------------------------------------------------------

    def _run_ws_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._ws_async())
        finally:
            self._loop.close()

    async def _ws_async(self):
        try:
            import ccxt.pro as ccxtpro
        except ImportError:
            logging.critical("ccxt.pro not installed. Run: pip install ccxt")
            self._stop.set()
            return

        exc_cls = getattr(ccxtpro, self.args.exchange, None)
        if not exc_cls:
            logging.critical("No ccxt.pro exchange: %s", self.args.exchange)
            self._stop.set()
            return

        self._ex = exc_cls({
            "enableRateLimit": True,
            "timeout": 20000,
            "options": {"defaultType": "spot"},
        })
        try:
            await asyncio.wait_for(self._ex.load_markets(), timeout=30.0)
            syms = self._syms

            tasks = []
            for sym in syms:
                tasks.append(asyncio.create_task(self._watch_book(sym)))
                tasks.append(asyncio.create_task(self._watch_trades(sym)))

            # Keep running until stop
            while not self._stop.is_set():
                await asyncio.sleep(0.5)

        except Exception as e:
            logging.error("WS async main error: %s", e)
        finally:
            if self._ex:
                await self._ex.close()

    async def _watch_book(self, sym):
        limit = 20
        err = 0
        while not self._stop.is_set():
            try:
                ob = await asyncio.wait_for(self._ex.watch_order_book(sym, limit=limit), timeout=8.0)
                now = time.time()
                bids = ob.get("bids") or []
                asks = ob.get("asks") or []
                if not bids or not asks:
                    continue

                best_bid = bids[0][0]; best_ask = asks[0][0]
                mid = (best_bid + best_ask) / 2.0
                spread_bps = (best_ask - best_bid) / mid * 1e4

                bq1 = bids[0][0] * bids[0][1]
                aq1 = asks[0][0] * asks[0][1]
                bv5  = sum(p * q for p, q in bids[:5])
                av5  = sum(p * q for p, q in asks[:5])
                bv10 = sum(p * q for p, q in bids[:10])
                av10 = sum(p * q for p, q in asks[:10])

                imb5  = bv5  / (bv5  + av5)  if (bv5  + av5)  > 0 else 0.5
                imb10 = bv10 / (bv10 + av10) if (bv10 + av10) > 0 else 0.5

                micro_px = (best_ask * bq1 + best_bid * aq1) / (bq1 + aq1) if (bq1 + aq1) > 0 else mid
                micro_skew = (micro_px - mid) / mid * 1e4 if mid > 0 else 0.0

                # Wall ratio (exact production formula from efra_stream.py)
                if bids and av5 > 0:
                    max_bid_not = max(p * q for p, q in bids[:10])
                    avg_ask_not = av5 / max(len(asks[:5]), 1)
                    wall_ratio  = min(1.0, max_bid_not / (avg_ask_not + max_bid_not))
                else:
                    wall_ratio  = 0.5

                with self._lock:
                    self._mids[sym].append((now, mid))
                    self._books[sym] = {
                        "_ts": now,
                        "bid": best_bid, "ask": best_ask, "mid": mid,
                        "spread_bps": spread_bps,
                        "bq1": bq1, "aq1": aq1,
                        "bv5": bv5, "av5": av5, "bv10": bv10, "av10": av10,
                        "imb5": imb5, "imb10": imb10,
                        "micro_skew": micro_skew, "wall_ratio": wall_ratio,
                        "bids": bids, "asks": asks,
                    }
                err = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                err += 1
                await asyncio.sleep(min(5.0, 0.2 * (2 ** min(err, 5))))

    async def _watch_trades(self, sym):
        """Watches trade tape -- same Gate.io side-inference as efra_stream.py."""
        err = 0
        while not self._stop.is_set():
            try:
                trades = await asyncio.wait_for(self._ex.watch_trades(sym, limit=50), timeout=8.0)
                now = time.time()
                with self._lock:
                    cur_mid = self._books.get(sym, {}).get("mid", 0.0)
                    tape = self._trades[sym]
                    for tr in trades:
                        # Multi-field side detection (Gate.io quirk)
                        side = (
                            tr.get("side") or tr.get("takerSide") or tr.get("taker_side") or
                            tr.get("info", {}).get("side", "") or tr.get("info", {}).get("type", "")
                        )
                        side = str(side).lower().strip()
                        if side in ("b", "bid", "buy", "1"):      side = "buy"
                        elif side in ("s", "ask", "sell", "0", "-1"): side = "sell"
                        else:
                            px_raw = float(tr.get("price") or 0.0)
                            side   = "buy" if (cur_mid > 0 and px_raw >= cur_mid) else "sell"

                        amt = float(tr.get("amount") or 0.0)
                        px  = float(tr.get("price")  or 0.0)
                        tr_ts = float(tr.get("timestamp") or (now * 1000.0)) / 1000.0
                        if amt > 0 and px > 0:
                            tape.append((tr_ts, side, amt * px, px))
                err = 0
            except asyncio.TimeoutError:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception:
                err += 1
                await asyncio.sleep(min(5.0, 0.5 * err))

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    def start(self):
        import ccxt
        ex = getattr(ccxt, self.args.exchange, None)
        if not ex:
            sys.exit(f"Unknown exchange: {self.args.exchange}")
        ex_inst = ex({"enableRateLimit": True, "timeout": 20000,
                      "options": {"defaultType": "spot"}})
        ex_inst.load_markets()

        if self.args.symbols:
            self._syms = [s.strip() for s in self.args.symbols.split(",")]
        else:
            self._syms = self._pick_syms(ex_inst)

        if not self._syms:
            sys.exit("No symbols selected. Lower --min-vol or check --exchange.")

        self._init_db()

        self._ws_thread = threading.Thread(target=self._run_ws_loop, daemon=True, name="ws-loop")
        self._ws_thread.start()
        logging.info("WS thread started. Waiting 5s for first book updates...")
        time.sleep(5.0)

        self._fill_thread = threading.Thread(target=self._fill_loop, daemon=True, name="fill-loop")
        self._fill_thread.start()

        try:
            self._snapshot_loop()
        except KeyboardInterrupt:
            logging.info("Interrupted by user.")
        finally:
            self._stop.set()
            logging.info("Waiting for threads to stop...")
            self._fill_thread.join(timeout=10)
            self._ws_thread.join(timeout=5)
            if self._db:
                self._db.close()
            logging.info("Recorder stopped.")


# --------------------------------------------------------------------------
# SIGNAL HELPER
# --------------------------------------------------------------------------

def _confluence(imb5, cvd_5s, micro_skew, mom_5s):
    imb_s  = max(0.0, min(1.0, (imb5   - 0.5) * 3.0))
    cvd_s  = max(0.0, min(1.0, (cvd_5s - 0.5) * 3.0))
    mic_s  = max(0.0, min(1.0,  micro_skew / 2.0)) if micro_skew > 0 else 0.0
    mom_s  = max(0.0, min(1.0, (mom_5s  + 2.0) / 6.0)) if mom_5s > -2 else 0.0
    return imb_s * 40.0 + cvd_s * 30.0 + mic_s * 20.0 + mom_s * 10.0


# --------------------------------------------------------------------------
# ANALYZER
# --------------------------------------------------------------------------

def tstat(xs):
    n = len(xs)
    if n < 3: return 0.0
    sd = statistics.pstdev(xs)
    return statistics.fmean(xs) / (sd / math.sqrt(n)) if sd > 0 else 0.0


def ci90(xs):
    n = len(xs)
    if n < 4: return (None, None)
    sd = statistics.pstdev(xs)
    m  = statistics.fmean(xs)
    mg = 1.645 * sd / math.sqrt(n)
    return (m - mg, m + mg)


def med(xs):
    return statistics.median(xs) if xs else 0.0


def analyze(args):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

    db = sqlite3.connect(args.db)
    db.execute("PRAGMA query_only=ON;")

    # DB summary
    total = db.execute("SELECT COUNT(*) FROM snaps").fetchone()[0]
    filled = db.execute("SELECT COUNT(*) FROM snaps WHERE filled=1").fetchone()[0]
    pairs = db.execute("SELECT COUNT(DISTINCT sym) FROM snaps").fetchone()[0]
    t_min = db.execute("SELECT MIN(ts) FROM snaps").fetchone()[0] or 0
    t_max = db.execute("SELECT MAX(ts) FROM snaps").fetchone()[0] or 0
    hours = (t_max - t_min) / 3600 if t_max > t_min else 0

    if filled < MIN_SAMPLES * 3:
        print(f"\nINSUFFICIENT FILLED DATA: {filled} filled rows (need >={MIN_SAMPLES*3}).")
        print(f"Total rows: {total:,} | Pairs: {pairs} | Hours: {hours:.1f}h")
        print("Let the recorder run longer and re-run analyze.")
        db.close()
        return

    fee  = args.fee_bps
    slip = args.slippage_bps
    mfee = args.maker_fee_bps

    # Chronological 3-way split
    s1 = t_min + (t_max - t_min) / 3
    s2 = t_min + 2 * (t_max - t_min) / 3

    print(f"\n{'='*72}")
    print("  EFRA EDGE AUDIT v2 -- with real CVD from trade tape")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"  DB: {args.db}")
    print(f"  {total:,} total snaps | {filled:,} filled ({100*filled/max(total,1):.0f}%) | "
          f"{pairs} pairs | {hours:.1f}h")
    print(f"  Split: IS<{datetime.fromtimestamp(s1,tz=timezone.utc):%H:%M}  "
          f"VAL<{datetime.fromtimestamp(s2,tz=timezone.utc):%H:%M}  OOS:rest")
    print(f"  Taker: {fee}bps/side + {slip}bps slip | Maker: {mfee}bps/side + {slip}bps slip")
    rtc_approx = 5.0 + 2*fee + 2*slip  # approximate avg spread
    print(f"  Break-even (approx): ~{rtc_approx:.0f} bps gross forward move required")
    print(f"{'='*72}")

    # Candidate event definitions
    # Each is (label, WHERE_clause)
    # They query the filled snaps table with forward return columns.
    # The WHERE clauses use actual CVD from the tape.

    candidates = [
        # ------ BASELINE (no filter) ------
        ("ALL_SNAPS",        "1=1"),
        # ------ L2 only (same as v1, baseline comparison) ------
        ("L2_OBI>=0.72",     "imb5>=0.72"),
        ("L2_OBI+MOM",       "imb5>=0.72 AND mom_5s>=3.5 AND mom_5s<=18"),
        # ------ Real CVD single threshold ------
        ("CVD5s>0.65",       "cvd_5s>0.65 AND n_trades_5s>=3"),
        ("CVD5s>0.70",       "cvd_5s>0.70 AND n_trades_5s>=3"),
        ("CVD5s>0.75",       "cvd_5s>0.75 AND n_trades_5s>=3"),
        ("CVD5s>0.80",       "cvd_5s>0.80 AND n_trades_5s>=3"),
        ("CVD30s>0.65",      "cvd_30s>0.65 AND n_trades_30s>=10"),
        ("CVD30s>0.70",      "cvd_30s>0.70 AND n_trades_30s>=10"),
        ("CVD30s>0.75",      "cvd_30s>0.75 AND n_trades_30s>=10"),
        # ------ Volume/trade burst ------
        ("N_trades_5s>=5",   "n_trades_5s>=5"),
        ("N_trades_5s>=10",  "n_trades_5s>=10"),
        # ------ Momentum acceleration ------
        ("ACCEL>2+CVD",      "mom_accel>2.0 AND cvd_5s>0.65 AND n_trades_5s>=3"),
        ("ACCEL>3+CVD",      "mom_accel>3.0 AND cvd_5s>0.68 AND n_trades_5s>=3"),
        # ------ TAPE signal path (now with real CVD) ------
        ("TAPE",             "cvd_5s>0.75 AND imb5>0.65 AND micro_skew>1.0 AND mom_5s>3.5"),
        ("TAPE_tight",       "cvd_5s>0.80 AND imb5>0.70 AND micro_skew>1.5 AND mom_5s>5.0"),
        # ------ ACCEL path (full) ------
        ("ACCEL_full",       "mom_accel>3.0 AND mom_5s>3.5 AND cvd_5s>0.68 AND imb5>0.65 AND micro_skew>0.5"),
        # ------ OBI + CVD combined ------
        ("OBI+CVD",          "imb5>0.72 AND cvd_5s>0.65 AND mom_5s>3.5 AND mom_5s<=18"),
        ("OBI+CVD_tight",    "imb5>0.75 AND cvd_5s>0.70 AND mom_5s>5.0 AND mom_5s<=18"),
        # ------ Full signal (all paths) ------
        ("sig_any",          "sig_any=1"),
        ("sig_tape",         "sig_tape=1"),
        ("sig_accel",        "sig_accel=1"),
        ("sig_obi",          "sig_obi=1"),
        ("sig_conf",         "sig_conf=1"),
        # ------ Liquidity vacuum / breakout ------
        ("liq_vac",          "spread_change_5s>1.5 AND cvd_5s>0.75 AND n_trades_5s>=3"),
        ("mom30s>8bps",      "mom_30s>8.0"),
        ("mom30s>8+CVD",     "mom_30s>8.0 AND cvd_30s>0.65"),
        # ------ Persistence: CVD stable over multiple windows ------
        ("CVD_persistent",   "cvd_5s>0.70 AND cvd_15s>0.65 AND cvd_30s>0.60 AND n_trades_30s>=10"),
        ("CVD_persistent_tight", "cvd_5s>0.75 AND cvd_15s>0.70 AND cvd_30s>0.65 AND n_trades_30s>=10"),
        # ------ Tight spread only ------
        ("tight_sp+CVD",     "spread_bps<3 AND cvd_5s>0.70"),
        ("tight_sp+TAPE",    "spread_bps<3 AND cvd_5s>0.75 AND imb5>0.65"),
        # ------ Maker simulation (same events, but cost is 2*mfee not taker) ------
        ("MAKER_TAPE",       "sig_tape=1"),
        ("MAKER_CVD30_pers", "cvd_5s>0.75 AND cvd_15s>0.70 AND cvd_30s>0.65"),
    ]

    maker_labels = {"MAKER_TAPE", "MAKER_CVD30_pers"}

    col_hdr = ("  {:<30} {:>3}s  n={:<6} net={:>+8.2f}  med={:>+8.2f}  "
               "gross={:>+8.2f}  hit={:>5.1%}  MFE={:>+7.1f}  MAE={:>+7.1f}  "
               "t={:>+6.2f}  OOS={}")

    def epoch(ts):
        if ts < s1: return 0
        if ts < s2: return 1
        return 2

    def query_segment(where, h, is_maker=False):
        """Query horizon-thinned net forward returns for a segment.

        Rows are sampled no more than once per horizon per symbol so a 1-second
        recorder does not turn one 60-second price move into ~60 correlated
        "independent" observations and inflate n/t-stat.
        """
        fwd_col = f"fwd_{h}s"

        rows_all = db.execute(
            f"""SELECT ts, sym, {fwd_col}, cost_bps, mfe_60s, mae_60s
                FROM snaps
                WHERE filled=1 AND {fwd_col} IS NOT NULL AND ({where})
                ORDER BY sym, ts""",
        ).fetchall()

        by_ep = {0: [], 1: [], 2: []}
        last_keep = {}
        for ts, sym, fwd, cost, mfe, mae in rows_all:
            if fwd is None:
                continue
            ep = epoch(ts)
            key = (ep, sym)
            if ts < last_keep.get(key, float("-inf")):
                continue
            last_keep[key] = ts + h
            if is_maker:
                net = fwd - (2 * mfee + 2 * slip)
            else:
                net = fwd - (cost if cost else 2*fee+2*slip+5)
            by_ep[ep].append((net, fwd, mfe or 0, mae or 0))

        return by_ep

    def oos_symbol_support(where, h, is_maker=False):
        """Count symbols with positive OOS mean net EV after horizon thinning."""
        fwd_col = f"fwd_{h}s"
        rows = db.execute(
            f"""SELECT ts, sym, {fwd_col}, cost_bps
                FROM snaps
                WHERE filled=1 AND ts>=? AND {fwd_col} IS NOT NULL AND ({where})
                ORDER BY sym, ts""",
            (s2,),
        ).fetchall()

        by_sym = defaultdict(list)
        last_keep = {}
        for ts, sym, fwd, cost in rows:
            if fwd is None:
                continue
            if ts < last_keep.get(sym, float("-inf")):
                continue
            last_keep[sym] = ts + h
            net = (
                fwd - (2 * mfee + 2 * slip)
                if is_maker
                else fwd - (cost if cost else 2*fee+2*slip+5)
            )
            by_sym[sym].append(net)

        positive = []
        for sym, nets in by_sym.items():
            if len(nets) >= MIN_SYM_OOS and statistics.fmean(nets) > 0:
                positive.append(sym)
        return positive

    def summarize_ep(obs_list):
        if len(obs_list) < MIN_SAMPLES:
            return None
        nets   = [x[0] for x in obs_list]
        grosss = [x[1] for x in obs_list]
        mfes   = [x[2] for x in obs_list]
        maes   = [x[3] for x in obs_list]
        ci = ci90(nets)
        return {
            "n": len(nets),
            "mean_net":   statistics.fmean(nets),
            "med_net":    med(nets),
            "mean_gross": statistics.fmean(grosss),
            "hit":        sum(1 for x in nets if x > 0) / len(nets),
            "mfe":        med(mfes),
            "mae":        med(maes),
            "t":          tstat(nets),
            "ci":         ci,
        }

    def is_positive(by_ep_summary):
        """Exploratory positive flag; not sufficient for promotion."""
        valid = [v for v in by_ep_summary.values() if v]
        return bool(valid and all(v["mean_net"] > 0 and v["t"] >= TSTAT_MIN for v in valid))

    def is_promotable(where, h, by_ep_summary, is_maker=False):
        """Hard PAPER-promotion gate: OOS EV/t/n plus multi-symbol support."""
        oos = by_ep_summary.get(2)
        if not oos:
            return False, []
        support = oos_symbol_support(where, h, is_maker=is_maker)
        ok = (
            oos["mean_net"] > 0
            and oos["t"] >= TSTAT_MIN
            and oos["n"] >= PROMOTION_MIN_OOS
            and len(support) >= PROMOTION_MIN_SYMS
        )
        return ok, support

    def print_section(title, subset_candidates, h_list=None):
        print(f"\n{'-'*72}")
        print(f"  {title}")
        print(f"{'-'*72}")
        h_targets = h_list or [60]

        for label, where in subset_candidates:
            is_mk = label in maker_labels
            for h in h_targets:
                by_ep_raw = query_segment(where, h, is_maker=is_mk)
                by_ep_sum = {ep: summarize_ep(obs) for ep, obs in by_ep_raw.items()}

                # Use OOS if available, else VAL, else IS
                ref = by_ep_sum.get(2) or by_ep_sum.get(1) or by_ep_sum.get(0)
                if not ref:
                    if h == h_targets[0]:
                        print(f"  {label:<30}  h={h}s  n<{MIN_SAMPLES}")
                    continue

                ci = ref["ci"]
                ci_s = f"[{ci[0]:+.1f},{ci[1]:+.1f}]" if ci[0] is not None else "N/A"
                promotable, support = is_promotable(where, h, by_ep_sum, is_maker=is_mk)
                oos = by_ep_sum.get(2)
                oos_s = (
                    "[OK]" if promotable
                    else ("[~]" if oos and oos["mean_net"] > 0 else "[NO]")
                )
                pos = (
                    f" <-- PROMOTION GATE PASSED ({len(support)} positive OOS symbols)"
                    if promotable else ""
                )

                print(col_hdr.format(
                    label[:30], h, ref["n"], ref["mean_net"], ref["med_net"],
                    ref["mean_gross"], ref["hit"], ref["mfe"], ref["mae"], ref["t"],
                    oos_s
                ) + pos)

    # Section 1: Baseline -- does the signal predict ANY gross move?
    print_section(
        "SECTION 1 -- Baseline forward gross move (no filter, all snaps)",
        [("ALL_SNAPS", "1=1"), ("L2_OBI>=0.72", "imb5>=0.72"),
         ("L2_OBI+MOM", "imb5>=0.72 AND mom_5s>=3.5 AND mom_5s<=18")],
        h_list=list(HORIZONS)
    )

    # Section 2: CVD burst events -- does persistent tape predict forward move?
    print_section(
        "SECTION 2 -- CVD burst events (real trade tape, all horizons)",
        [c for c in candidates if c[0].startswith("CVD")],
        h_list=list(HORIZONS)
    )

    # Section 3: Combined signals at 60s
    print_section(
        "SECTION 3 -- Combined signal events at 60s horizon",
        [c for c in candidates if c[0] not in
         ("ALL_SNAPS","L2_OBI>=0.72","L2_OBI+MOM") and not c[0].startswith("CVD")],
        h_list=[60]
    )

    # Section 4: Best candidates at all horizons
    print(f"\n{'-'*72}")
    print("  SECTION 4 -- Best candidates: CVD_persistent and TAPE at all horizons")
    print(f"{'-'*72}")
    for label in ["CVD_persistent", "CVD_persistent_tight", "TAPE", "TAPE_tight",
                  "OBI+CVD", "OBI+CVD_tight", "ACCEL_full"]:
        c = next((x for x in candidates if x[0] == label), None)
        if not c:
            continue
        for h in HORIZONS:
            by_ep_raw = query_segment(c[1], h, is_maker=(label in maker_labels))
            by_ep_sum = {ep: summarize_ep(obs) for ep, obs in by_ep_raw.items()}
            ref = by_ep_sum.get(2) or by_ep_sum.get(1) or by_ep_sum.get(0)
            if not ref:
                print(f"  {label:<30}  h={h}s  n<{MIN_SAMPLES}")
                continue
            ci = ref["ci"]
            ci_s = f"[{ci[0]:+.1f},{ci[1]:+.1f}]" if ci[0] is not None else "N/A"
            promotable, support = is_promotable(c[1], h, by_ep_sum, is_maker=(label in maker_labels))
            oos = by_ep_sum.get(2)
            oos_s = (
                "[OK]" if promotable
                else ("[~]" if oos and oos["mean_net"] > 0 else "[NO]")
            )
            pos = (
                f" <-- PROMOTION GATE PASSED ({len(support)} positive OOS symbols)"
                if promotable else ""
            )
            print(col_hdr.format(
                label[:30], h, ref["n"], ref["mean_net"], ref["med_net"],
                ref["mean_gross"], ref["hit"], ref["mfe"], ref["mae"], ref["t"],
                oos_s
            ) + pos)

    # Section 5: Maker execution
    print_section(
        "SECTION 5 -- Maker execution analysis (cost = 2*maker_fee, no spread)",
        [c for c in candidates if c[0].startswith("MAKER")],
        h_list=list(HORIZONS)
    )

    # Section 6: Per-symbol
    print(f"\n{'-'*72}")
    print("  SECTION 6 -- Per-symbol CVD_persistent expectancy (60s, OOS)")
    print(f"{'-'*72}")
    syms = [r[0] for r in db.execute("SELECT DISTINCT sym FROM snaps ORDER BY sym").fetchall()]
    sym_results = []
    for sym in syms:
        where = f"sym='{sym}' AND cvd_5s>0.70 AND cvd_15s>0.65 AND n_trades_30s>=10"
        by_ep_raw = query_segment(where, 60)
        by_ep_sum = {ep: summarize_ep(obs) for ep, obs in by_ep_raw.items()}
        ref = by_ep_sum.get(2) or by_ep_sum.get(1) or by_ep_sum.get(0)
        if ref:
            sym_results.append((sym, ref["mean_net"], ref["n"], ref["hit"], ref["t"]))
    sym_results.sort(key=lambda x: -x[1])
    print(f"  {'Symbol':<20} {'net_ev':>10} {'n':>6} {'hit%':>7} {'t':>6}")
    for sym, mn, n, hr, ts in sym_results:
        flag = " <-- AVOID" if mn < -5 else (" <-- POSITIVE" if mn > 0 else "")
        print(f"  {sym:<20} {mn:>+10.2f} {n:>6} {hr:>7.1%} {ts:>+6.2f}{flag}")

    # Section 7: Spread coverage
    print(f"\n{'-'*72}")
    print("  SECTION 7 -- CVD quality check (are we getting real tape?)")
    print(f"{'-'*72}")
    for sym in syms[:10]:
        r = db.execute(
            "SELECT COUNT(*), AVG(n_trades_5s), AVG(cvd_5s), AVG(spread_bps) "
            "FROM snaps WHERE sym=? AND filled=1",
            (sym,)
        ).fetchone()
        if r and r[0] > 0:
            print(f"  {sym:<20} n={r[0]:>6}  avg_trades_5s={r[1]:>5.1f}  "
                  f"avg_cvd_5s={r[2]:>5.3f}  avg_spread={r[3]:>5.2f}bps")

    # Verdict
    print(f"\n{'='*72}")
    print("  VERDICT")
    print(f"{'='*72}")

    promotable_candidates = []
    exploratory_positive = []
    for label, where in candidates:
        is_mk = label in maker_labels
        by_ep_raw = query_segment(where, 60, is_maker=is_mk)
        by_ep_sum = {ep: summarize_ep(obs) for ep, obs in by_ep_raw.items()}
        oos = by_ep_sum.get(2)
        if oos and oos["mean_net"] > 0 and oos["t"] >= TSTAT_MIN:
            exploratory_positive.append((label, oos["mean_net"], oos["n"], oos["t"]))
        ok, support = is_promotable(where, 60, by_ep_sum, is_maker=is_mk)
        if ok and oos:
            promotable_candidates.append(
                (label, oos["mean_net"], oos["n"], oos["t"], support)
            )

    if promotable_candidates:
        promotable_candidates.sort(key=lambda x: -x[1])
        print(f"\n  PROMOTION-GATE candidates at 60s:")
        for label, ev, n, ts, support in promotable_candidates:
            print(
                f"    {label:<30} OOS_net={ev:>+8.2f}bps  n={n}  t={ts:>+.2f}  "
                f"positive_symbols={len(support)}"
            )
        print(f"\n  ACTION: Promote the top candidate to SHADOW only.")
        print(f"  Confirm at 30s and 15s horizons before PAPER.")
    else:
        if exploratory_positive:
            print(f"\n  Some OOS means are positive but NONE passes the hard promotion gate:")
            for label, ev, n, ts in sorted(exploratory_positive, key=lambda x: -x[1])[:10]:
                print(f"    {label:<30} OOS_net={ev:>+8.2f}bps  n={n}  t={ts:>+.2f}")
        print(
            f"\n  Hard gate requires OOS mean_net>0, t>={TSTAT_MIN:.1f}, "
            f"n>={PROMOTION_MIN_OOS}, and >={PROMOTION_MIN_SYMS} positive symbols."
        )
        print(f"\n  No candidate shows positive OOS net EV at 60s.")
        print(f"  Check Section 4 for shorter horizons (5s, 15s, 30s).")
        print(f"  If gross moves are positive but net is negative, the issue is costs:")
        print(f"    -> Consider maker-only execution (Section 5)")
        print(f"    -> Consider shorter hold (smaller TP, faster exit)")
        print(f"    -> Consider pairs with lower spread only")
        print(f"  If gross is also near zero, the signal has no directional content.")

    print(f"\n{'='*72}\n")
    db.close()


# --------------------------------------------------------------------------
# STATUS
# --------------------------------------------------------------------------

def status(args):
    db = sqlite3.connect(args.db)
    total   = db.execute("SELECT COUNT(*) FROM snaps").fetchone()[0]
    filled  = db.execute("SELECT COUNT(*) FROM snaps WHERE filled=1").fetchone()[0]
    unfill  = db.execute("SELECT COUNT(*) FROM snaps WHERE filled=0").fetchone()[0]
    pairs   = db.execute("SELECT COUNT(DISTINCT sym) FROM snaps").fetchone()[0]
    t_min   = db.execute("SELECT MIN(ts) FROM snaps").fetchone()[0] or 0
    t_max   = db.execute("SELECT MAX(ts) FROM snaps").fetchone()[0] or 0
    hours   = (t_max - t_min) / 3600 if t_max > t_min else 0
    sig_any = db.execute("SELECT COUNT(*) FROM snaps WHERE sig_any=1 AND filled=1").fetchone()[0]
    cvd_ok  = db.execute(
        "SELECT COUNT(*) FROM snaps WHERE filled=1 AND (cvd_5s!=0.5 OR n_trades_5s>0)"
    ).fetchone()[0]

    print(f"\n=== EFRA Research v2 DB Status ===")
    print(f"  DB           : {args.db}")
    print(f"  Total snaps  : {total:,}")
    print(f"  Filled       : {filled:,} ({100*filled/max(total,1):.0f}%)")
    print(f"  Unfilled     : {unfill:,}")
    print(f"  Pairs        : {pairs}")
    print(f"  Time range   : {hours:.1f}h")
    if t_max > 0:
        print(f"  Last snap    : {datetime.fromtimestamp(t_max, tz=timezone.utc):%Y-%m-%d %H:%M} UTC")
    print(f"  CVD-active   : {cvd_ok:,} ({100*cvd_ok/max(filled,1):.0f}% of filled have real tape)")
    print(f"  Signal fired : {sig_any:,} ({100*sig_any/max(filled,1):.1f}% of filled)")

    # Per-pair breakdown
    print(f"\n  {'Symbol':<22} {'snaps':>8} {'filled':>8} {'avg_cvd5':>10} {'avg_n5':>8}")
    for r in db.execute(
        "SELECT sym, COUNT(*), SUM(filled), AVG(cvd_5s), AVG(n_trades_5s) "
        "FROM snaps GROUP BY sym ORDER BY COUNT(*) DESC LIMIT 20"
    ):
        print(f"  {r[0]:<22} {r[1]:>8,} {r[2] or 0:>8,} {r[3] or 0:>10.3f} {r[4] or 0:>8.1f}")
    db.close()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ap = argparse.ArgumentParser(description="EFRA Research v2 -- rich microstructure recorder")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # record
    rec = sub.add_parser("record")
    rec.add_argument("--exchange",       default="gate")
    rec.add_argument("--quote",          default="USDT")
    rec.add_argument("--pairs",          type=int,   default=20)
    rec.add_argument("--symbols",        default="", help="comma list, overrides auto-pick")
    rec.add_argument("--hours",          type=float, default=96.0)
    rec.add_argument("--db",             default="efra_v2.db")
    rec.add_argument("--min-vol",        type=float, default=1_000_000)
    rec.add_argument("--max-spread-bps", type=float, default=10.0)

    # analyze
    ana = sub.add_parser("analyze")
    ana.add_argument("--db",             default="efra_v2.db")
    ana.add_argument("--fee-bps",        type=float, default=FEE_BPS)
    ana.add_argument("--slippage-bps",   type=float, default=SLIP_BPS)
    ana.add_argument("--maker-fee-bps",  type=float, default=MAKER_FEE_BPS)

    # status
    sta = sub.add_parser("status")
    sta.add_argument("--db", default="efra_v2.db")

    args = ap.parse_args()

    if args.cmd == "record":
        r = ResearchRecorderV2(args)
        r.start()
    elif args.cmd == "analyze":
        analyze(args)
    elif args.cmd == "status":
        status(args)


if __name__ == "__main__":
    main()
