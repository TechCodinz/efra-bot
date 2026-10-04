#!/usr/bin/env python3
"""EFRA Sniper v2 web runtime.

Runs the existing EFRA trading engine in a background thread and exposes
read-only telemetry plus a browser dashboard. Trading logic stays in
efra_bot.py; this module is only the production web/observability shell.
"""

import csv
import json
import logging
import os
import sqlite3
import threading
import urllib.request
import urllib.error
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from efra_bot import Bot, MakerBot, Cfg
from efra_store import PostgresStore

logging.basicConfig(
    level=getattr(logging, os.getenv("EFRA_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    force=True,
)
log = logging.getLogger("efra.web")

_state_lock = threading.Lock()
_bot: Bot | None = None
_engine_thread: threading.Thread | None = None
_engine_error: str | None = None
_engine_started_at: float | None = None
_store: PostgresStore | None = None
_lease_status = "disabled"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def build_cfg() -> Cfg:
    c = Cfg()
    c.exchange = os.getenv("EFRA_EXCHANGE", "gateio")
    c.mode = os.getenv("EFRA_MODE", "taker").strip().lower()
    c.quote = os.getenv("EFRA_QUOTE", "USDT")
    c.paper = _env_bool("EFRA_PAPER", True)
    c.start_balance = _env_float("EFRA_START_BALANCE", 100.0)
    c.state_file = os.getenv("EFRA_STATE_FILE", c.state_file)
    c.log_file = os.getenv("EFRA_LOG_FILE", c.log_file)

    # Keep strategy defaults aligned with the current Sniper v2 configuration,
    # while allowing deployment-time overrides without modifying strategy code.
    c.tp_bps = _env_float("EFRA_TP_BPS", c.tp_bps)
    c.sl_bps = _env_float("EFRA_SL_BPS", c.sl_bps)
    c.breakeven_bps = _env_float("EFRA_BREAKEVEN_BPS", c.breakeven_bps)
    c.trail_trigger_bps = _env_float("EFRA_TRAIL_TRIGGER_BPS", c.trail_trigger_bps)
    c.trail_bps = _env_float("EFRA_TRAIL_BPS", c.trail_bps)
    c.min_confluence = _env_float("EFRA_MIN_CONFLUENCE", c.min_confluence)
    c.min_cvd = _env_float("EFRA_MIN_CVD", c.min_cvd)
    c.imbalance_entry = _env_float("EFRA_IMBALANCE_ENTRY", c.imbalance_entry)
    c.min_mom_bps = _env_float("EFRA_MIN_MOM_BPS", c.min_mom_bps)
    c.max_mom_bps = _env_float("EFRA_MAX_MOM_BPS", c.max_mom_bps)
    c.min_mom_accel = _env_float("EFRA_MIN_MOM_ACCEL", c.min_mom_accel)
    c.min_wall_ratio = _env_float("EFRA_MIN_WALL_RATIO", c.min_wall_ratio)
    c.vol_surge_factor = _env_float("EFRA_VOL_SURGE_FACTOR", c.vol_surge_factor)
    c.smart_reentry = _env_bool("EFRA_SMART_REENTRY", c.smart_reentry)
    c.slow_bail_hold_s = _env_float("EFRA_SLOW_BAIL_HOLD_S", c.slow_bail_hold_s)
    c.slow_bail_ret_bps = _env_float("EFRA_SLOW_BAIL_RET_BPS", c.slow_bail_ret_bps)
    c.slow_bail_mom_bps = _env_float("EFRA_SLOW_BAIL_MOM_BPS", c.slow_bail_mom_bps)
    c.slow_bail_cvd = _env_float("EFRA_SLOW_BAIL_CVD", c.slow_bail_cvd)
    c.flip_bail_hold_s = _env_float("EFRA_FLIP_BAIL_HOLD_S", c.flip_bail_hold_s)
    c.flip_bail_imb = _env_float("EFRA_FLIP_BAIL_IMB", c.flip_bail_imb)
    c.flip_bail_ret_bps = _env_float("EFRA_FLIP_BAIL_RET_BPS", c.flip_bail_ret_bps)
    c.sl_wick_debounce_s = _env_float("EFRA_SL_WICK_DEBOUNCE_S", c.sl_wick_debounce_s)
    c.sl_cluster_window_s = _env_float("EFRA_SL_CLUSTER_WINDOW_S", c.sl_cluster_window_s)
    c.sl_cluster_count = _env_int("EFRA_SL_CLUSTER_COUNT", c.sl_cluster_count)
    c.sl_cluster_pause_s = _env_float("EFRA_SL_CLUSTER_PAUSE_S", c.sl_cluster_pause_s)
    c.streak_conf_boost = _env_float("EFRA_STREAK_CONF_BOOST", c.streak_conf_boost)
    c.streak_obi_boost = _env_float("EFRA_STREAK_OBI_BOOST", c.streak_obi_boost)
    c.inter_trade_pause_s = _env_float("EFRA_INTER_TRADE_PAUSE_S", c.inter_trade_pause_s)
    c.position_frac = _env_float("EFRA_POSITION_FRAC", c.position_frac)
    c.daily_loss_limit_frac = _env_float("EFRA_DAILY_LOSS_LIMIT_FRAC", c.daily_loss_limit_frac)
    c.watch_n = _env_int("EFRA_WATCH_N", c.watch_n)
    c.max_hold_s = _env_int("EFRA_MAX_HOLD_S", c.max_hold_s)
    c.loop_s = _env_float("EFRA_LOOP_S", c.loop_s)
    c.ws = _env_bool("EFRA_WS", True)
    c.dashboard = False
    return c


def _lease_guard(bot: Bot, store: PostgresStore) -> None:
    global _lease_status, _engine_error
    while not bot.halted:
        time.sleep(5)
        if not store.lease_alive():
            log.critical("execution lease lost; halting engine fail-closed")
            _lease_status = "lost"
            _engine_error = "PostgreSQL execution lease lost"
            bot.halted = True
            return


def _engine_main() -> None:
    global _bot, _engine_error, _engine_started_at, _store, _lease_status
    store = None
    try:
        cfg = build_cfg()
        db_url = os.getenv("EFRA_DATABASE_URL", "").strip()
        if db_url:
            state_key = os.getenv(
                "EFRA_STATE_KEY",
                f"{cfg.exchange}:{cfg.mode}:{'paper' if cfg.paper else 'live'}:{cfg.quote}",
            )
            store = PostgresStore(db_url, state_key)
            _store = store
            store.ensure_schema()
            _lease_status = "standby"
            while not store.try_acquire_lease():
                log.info("execution lease busy | state=%s | waiting as STANDBY", state_key)
                time.sleep(3)
            _lease_status = "primary"
            log.info("execution lease acquired | state=%s | role=PRIMARY", state_key)
        else:
            _lease_status = "disabled"

        bot_cls = MakerBot if cfg.mode == "maker" else Bot
        log.info(
            "engine boot | exchange=%s mode=%s capital_mode=%s start_balance=%.2f",
            cfg.exchange, cfg.mode, "PAPER" if cfg.paper else "LIVE", cfg.start_balance,
        )
        bot = bot_cls(cfg)
        if store is not None:
            bot.state_store = store
            threading.Thread(
                target=_lease_guard,
                args=(bot, store),
                name="efra-execution-lease-guard",
                daemon=True,
            ).start()
        log.info(
            "engine constructed | exchange=%s websocket=%s markets=%d",
            bot.c.exchange, bool(bot.ws_engine), len(getattr(bot.ex, "markets", {}) or {}),
        )
        with _state_lock:
            _bot = bot
            _engine_started_at = time.time()
            _engine_error = None
        bot.run()
    except BaseException as exc:
        log.exception("EFRA engine stopped")
        with _state_lock:
            _engine_error = f"{type(exc).__name__}: {exc}"
    finally:
        if store is not None:
            store.close()


def start_engine_once() -> None:
    global _engine_thread
    with _state_lock:
        if _engine_thread and _engine_thread.is_alive():
            return
        _engine_thread = threading.Thread(
            target=_engine_main,
            name="efra-sniper-v2",
            daemon=True,
        )
        _engine_thread.start()


def _recent_trades(bot: Bot, limit: int = 20) -> list[dict[str, Any]]:
    store = getattr(bot, "state_store", None)
    if store is not None:
        try:
            rows = store.recent_trades(limit)
            if rows:
                return rows
        except Exception:
            log.exception("could not read PostgreSQL trade ledger; falling back to CSV")

    path = Path(bot.c.log_file)
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))[-limit:]
    except (OSError, csv.Error):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            out.append({
                "ts": int(float(row.get("ts") or 0)),
                "symbol": row.get("symbol") or "",
                "entry": float(row.get("entry") or 0),
                "exit": float(row.get("exit") or 0),
                "qty": float(row.get("qty") or 0),
                "pnl_quote": float(row.get("pnl_quote") or 0),
                "reason": row.get("reason") or "",
                "equity": float(row.get("equity") or 0),
                "tier": int(float(row.get("tier") or 0)),
                "strategy_version": row.get("strategy_version") or "",
                "entry_policy": row.get("entry_policy") or "",
                "signal_path": row.get("signal_path") or "",
            })
        except (TypeError, ValueError):
            continue
    return out


def _research_v2_status() -> dict[str, Any]:
    """Read-only freshness probe for the host Research v2 recorder."""
    root = Path(os.getenv("EFRA_RESEARCH_DIR", "/research"))
    db_path = root / "efra_v2.db"
    wal_path = root / "efra_v2.db-wal"
    candidates = [p for p in (db_path, wal_path) if p.exists()]
    if not candidates:
        return {
            "version": "v2",
            "connected": False,
            "db_present": False,
            "age_s": None,
            "last_snap_at": None,
            "pairs": 0,
        }

    newest_mtime = max(p.stat().st_mtime for p in candidates)
    age_s = max(0.0, time.time() - newest_mtime)
    connected = age_s <= 45.0
    last_snap_at = None
    pairs = 0

    # Keep this bounded: only two scalar queries, read-only, and fail open to
    # file-freshness status if SQLite is momentarily busy.
    if db_path.exists():
        try:
            con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=0.2)
            try:
                row = con.execute("SELECT MAX(ts) FROM snaps").fetchone()
                last_snap_at = float(row[0]) if row and row[0] is not None else None
                row = con.execute("SELECT COUNT(DISTINCT sym) FROM snaps").fetchone()
                pairs = int(row[0]) if row and row[0] is not None else 0
            finally:
                con.close()
        except Exception:
            pass

    if last_snap_at is not None:
        age_s = max(0.0, time.time() - last_snap_at)
        connected = age_s <= 45.0

    return {
        "version": "v2",
        "connected": connected,
        "db_present": True,
        "age_s": age_s,
        "last_snap_at": last_snap_at,
        "pairs": pairs,
        "feed": "gate-websocket-tape",
    }


def _paused_snapshot() -> dict[str, Any]:
    """Serve last persisted PAPER state while execution is intentionally disabled."""
    cfg = build_cfg()
    research = _research_v2_status()
    state_path = Path(os.getenv("EFRA_STATE_FILE", "/data/efra_state.json"))
    log_path = Path(os.getenv("EFRA_LOG_FILE", "/data/efra_trades.csv"))
    state: dict[str, Any] = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            log.exception("could not read paused EFRA state")

    trades: list[dict[str, Any]] = []
    if log_path.exists():
        try:
            with log_path.open("r", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))[-200:]
            for row in rows:
                try:
                    trades.append({
                        "ts": int(float(row.get("ts") or 0)),
                        "symbol": row.get("symbol") or "",
                        "entry": float(row.get("entry") or 0),
                        "exit": float(row.get("exit") or 0),
                        "qty": float(row.get("qty") or 0),
                        "pnl_quote": float(row.get("pnl_quote") or 0),
                        "reason": row.get("reason") or "",
                        "equity": float(row.get("equity") or 0),
                        "tier": int(float(row.get("tier") or 0)),
                        "strategy_version": row.get("strategy_version") or "",
                        "entry_policy": row.get("entry_policy") or "",
                        "signal_path": row.get("signal_path") or "",
                    })
                except (TypeError, ValueError):
                    continue
        except (OSError, csv.Error):
            log.exception("could not read paused EFRA trade log")

    cash = float(state.get("cash", cfg.start_balance) or cfg.start_balance)

    # PAPER cash excludes capital allocated to open simulated positions.
    # In UI-only mode there is intentionally no live market feed, so value
    # persisted positions at their entry prices instead of incorrectly showing
    # cash alone as total equity.
    frozen_position_value = 0.0
    for _sym, _p in (state.get("pos") or {}).items():
        try:
            frozen_position_value += float(_p.get("entry", 0)) * float(_p.get("qty", 0))
        except (TypeError, ValueError):
            continue
    equity = cash + frozen_position_value

    n_trades = int(state.get("n_trades", len(trades)) or 0)
    n_wins = int(state.get("n_wins", sum(1 for t in trades if t["pnl_quote"] > 0)) or 0)
    hwm = float(state.get("high_water_mark", max(equity, cfg.start_balance)) or equity)
    day_eq = float(state.get("day_start_eq", equity) or equity)
    realized_pnl = sum(float(t.get("pnl_quote", 0)) for t in trades)
    gross_profit = sum(max(0.0, float(t.get("pnl_quote", 0))) for t in trades)
    gross_loss = sum(max(0.0, -float(t.get("pnl_quote", 0))) for t in trades)
    profit_factor = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (None if gross_profit <= 0 else 9.9)
    )
    net_bps_samples = [
        (float(t["pnl_quote"]) / (float(t["entry"]) * float(t["qty"]))) * 1e4
        for t in trades
        if float(t.get("entry", 0)) > 0 and float(t.get("qty", 0)) > 0
    ]
    avg_net_bps = (
        sum(net_bps_samples) / len(net_bps_samples)
        if net_bps_samples else 0.0
    )

    persisted_positions = []
    for sym, p in (state.get("pos") or {}).items():
        try:
            persisted_positions.append({
                "symbol": sym,
                "entry": float(p.get("entry", 0)),
                "current": float(p.get("entry", 0)),
                "qty": float(p.get("qty", 0)),
                "ret_bps": 0.0,
                "pnl_quote": 0.0,
                "peak_ret_bps": float(p.get("peak_ret_bps", 0)),
                "stop_bps": float(p.get("stop_bps", -cfg.sl_bps)),
                "be_locked": bool(p.get("be_locked")),
                "trailing_active": bool(p.get("trailing_active")),
                "age_s": 0.0,
                "signal_path": p.get("signal_path", "unknown"),
                "entry_cvd": float(p.get("entry_cvd", 0.5)),
                "entry_imb": float(p.get("entry_imb", 0.5)),
                "entry_mom_bps": float(p.get("entry_mom", p.get("entry_mom_bps", 0.0))),
                "entry_accel_bps": float(p.get("entry_accel", 0.0)),
                "entry_confidence": float(p.get("entry_confidence", 0.0)),
                "entry_cost_bps": float(p.get("entry_cost_bps", 0.0)),
            })
        except (TypeError, ValueError):
            continue

    return {
        "ok": True,
        "engine": "paused_research",
        "execution_paused": True,
        "error": None,
        "thread_alive": False,
        "lease_status": "disabled",
        "persistence": "local_persistent",
        "started_at": None,
        "uptime_s": 0.0,
        "mode": "paper" if cfg.paper else "live",
        "exchange": cfg.exchange,
        "execution": cfg.mode,
        "quote": cfg.quote,
        "feed": "research-only",
        "research": research,
        "equity": equity,
        "cash": cash,
        "frozen_position_value": frozen_position_value,
        "start_equity": float(cfg.start_balance),
        "total_pnl": equity - float(cfg.start_balance),
        "total_pnl_pct": ((equity / cfg.start_balance) - 1.0) * 100 if cfg.start_balance else 0.0,
        "day_pnl": equity - day_eq,
        "day_pnl_pct": ((equity / day_eq) - 1.0) * 100 if day_eq else 0.0,
        "drawdown_pct": ((hwm - equity) / hwm) * 100 if hwm else 0.0,
        "trades": n_trades,
        "wins": n_wins,
        "realized_pnl": realized_pnl,
        "profit_factor": profit_factor,
        "average_net_bps_per_trade": avg_net_bps,
        "win_rate": (n_wins / n_trades * 100.0) if n_trades else 0.0,
        "compound_tier": int(state.get("compound_tier", 1) or 1),
        "loss_streak": int(state.get("loss_streak", 0) or 0),
        "slots": int(cfg.max_positions),
        "slot_allocation_pct": float(cfg.position_frac) * 100.0,
        "btc_regime_safe": True,
        "btc_momentum_bps": 0.0,
        "redeploy_pause_s": 0.0,
        "positions": persisted_positions,
        "pending": [],
        "radar": [],
        "recent_trades": trades[-20:],
        "strategy_version": state.get("strategy_version", ""),
        "entry_policy": state.get("entry_policy", ""),
        "config": {
            "tp_bps": cfg.tp_bps,
            "sl_bps": cfg.sl_bps,
            "breakeven_bps": cfg.breakeven_bps,
            "trail_trigger_bps": cfg.trail_trigger_bps,
            "trail_bps": cfg.trail_bps,
            "min_confluence": cfg.min_confluence,
            "imbalance_entry": cfg.imbalance_entry,
            "min_cvd": cfg.min_cvd,
            "min_mom_bps": cfg.min_mom_bps,
            "max_mom_bps": cfg.max_mom_bps,
            "min_mom_accel": cfg.min_mom_accel,
            "min_wall_ratio": cfg.min_wall_ratio,
            "vol_surge_factor": cfg.vol_surge_factor,
            "smart_reentry": 1 if cfg.smart_reentry else 0,
            "daily_loss_limit_pct": cfg.daily_loss_limit_frac * 100.0,
            "loop_s": cfg.loop_s,
        },
    }


def snapshot() -> dict[str, Any]:
    with _state_lock:
        bot = _bot
        thread = _engine_thread
        err = _engine_error
        started = _engine_started_at
        lease_status = _lease_status

    if bot is None:
        if not engine_enabled():
            return _paused_snapshot()
        return {
            "ok": err is None,
            "engine": "starting" if err is None else "failed",
            "error": err,
            "thread_alive": bool(thread and thread.is_alive()),
            "lease_status": lease_status,
            "persistence": "postgres" if _store is not None else "local_ephemeral",
        }

    try:
        books = dict(getattr(bot, "last_books", {}) or {})
        eq = float(bot.equity(books))
        start_eq = float(getattr(bot, "start_balance", bot.c.start_balance) or bot.c.start_balance)
        day_eq = float(getattr(bot, "day_start_eq", 0) or eq)
        hwm = max(float(getattr(bot, "high_water_mark", eq) or eq), eq)
        positions = []
        for sym, p in list(bot.pos.items()):
            b = books.get(sym, {}) or {}
            current = float(
                b.get("ask", p["entry"]) if bot.c.mode == "maker"
                else b.get("bid", b.get("mid", p["entry"]))
            )
            ret_bps = ((current - float(p["entry"])) / float(p["entry"]) * 1e4) if p.get("entry") else 0.0
            positions.append({
                "symbol": sym,
                "entry": float(p.get("entry", 0)),
                "current": current,
                "qty": float(p.get("qty", 0)),
                "ret_bps": ret_bps,
                "pnl_quote": (current - float(p.get("entry", 0))) * float(p.get("qty", 0)),
                "peak_ret_bps": float(p.get("peak_ret_bps", 0)),
                "stop_bps": float(p.get("stop_bps", -bot.c.sl_bps)),
                "be_locked": bool(p.get("be_locked")),
                "trailing_active": bool(p.get("trailing_active")),
                "age_s": max(0.0, time.time() - float(p.get("ts", time.time()))),
                "signal_path": p.get("signal_path", "unknown"),
                "entry_cvd": float(p.get("entry_cvd", 0.5)),
                "entry_imb": float(p.get("entry_imb", 0.5)),
                "entry_mom_bps": float(p.get("entry_mom", p.get("entry_mom_bps", 0.0))),
                "entry_accel_bps": float(p.get("entry_accel", 0.0)),
                "entry_confidence": float(p.get("entry_confidence", 0.0)),
                "entry_cost_bps": float(p.get("entry_cost_bps", 0.0)),
            })

        pending_map = getattr(bot, "pending", {}) or {}
        pending = [
            {
                "symbol": sym,
                "price": float(p.get("px", 0)),
                "qty": float(p.get("qty", 0)),
                "quote": float(p.get("quote", 0)),
                "age_s": max(0.0, time.time() - float(p.get("ts", time.time()))),
            }
            for sym, p in list(pending_map.items())
        ]

        radar = []
        for sym in list(getattr(bot, "watch", []) or [])[:18]:
            b = books.get(sym)
            if not b:
                radar.append({"symbol": sym, "status": "awaiting_feed"})
                continue
            spread = float(b.get("spread", 0))
            imb = float(b.get("imb", 0.5))
            cvd = float(b.get("cvd_5s", 0.5))
            skew = float(b.get("micro_skew", 0))
            mom = float(b.get("mom", 0))
            conf = float(b.get("confluence", 0))
            cvd15 = float(b.get("cvd_15s", 0.5))
            mom_accel = float(b.get("mom_accel", 0))
            wall_ratio = float(b.get("wall_ratio", 0.5))
            cost = 2 * (bot.c.fee_bps + bot.c.slippage_bps) + spread
            net_edge = bot.c.tp_bps - cost
            edge_ok = net_edge >= bot.c.min_net_edge_bps
            mom_ok = bot.c.min_mom_bps <= mom <= bot.c.max_mom_bps
            accel_ok = bot.c.min_mom_accel <= 0 or mom_accel >= bot.c.min_mom_accel
            wall_ok = bot.c.min_wall_ratio <= 0 or wall_ratio >= bot.c.min_wall_ratio
            volume_ok = (
                True
                if bot.c.vol_surge_factor <= 1.0
                else (cvd >= 0.55 and cvd15 >= 0.52)
            )
            streak = int(getattr(bot, "loss_streak", 0))
            effective_conf = bot.c.min_confluence + (
                bot.c.streak_conf_boost if streak >= 3 else 0.0
            )
            effective_imb = bot.c.imbalance_entry + (
                bot.c.streak_obi_boost if streak >= 3 else 0.0
            )
            sig_obi = (
                imb >= effective_imb
                and skew >= 1.0
                and cvd >= bot.c.min_cvd
                and mom >= bot.c.min_mom_bps
            )
            sig_conf = (
                conf >= effective_conf
                and imb >= (effective_imb - 0.02)
                and cvd >= bot.c.min_cvd
                and mom >= bot.c.min_mom_bps
            )
            sig_tape = (
                cvd >= 0.75
                and imb >= 0.65
                and skew >= 1.0
                and mom >= bot.c.min_mom_bps
            )
            sig_accel = (
                mom_accel >= 3.0
                and mom >= bot.c.min_mom_bps
                and cvd >= 0.68
                and imb >= 0.65
                and skew > 0.5
            )
            paths = [
                name
                for name, passed in (
                    ("OBI", sig_obi),
                    ("CONF", sig_conf),
                    ("TAPE", sig_tape),
                    ("ACCEL", sig_accel),
                )
                if passed
            ]
            require_all = streak >= 5
            fired = (
                imb >= effective_imb
                and cvd >= bot.c.min_cvd
                and mom >= bot.c.min_mom_bps
                and conf >= effective_conf
                and skew >= 1.0
            ) if require_all else bool(paths)
            ready = edge_ok and mom_ok and accel_ok and wall_ok and volume_ok and fired
            radar.append({
                "symbol": sym,
                "mid": float(b.get("mid", 0)),
                "spread_bps": spread,
                "obi": imb,
                "cvd": cvd,
                "micro_skew_bps": skew,
                "momentum_bps": mom,
                "confluence": conf,
                "net_edge_bps": net_edge,
                "cvd_15s": cvd15,
                "momentum_accel_bps": mom_accel,
                "wall_ratio": wall_ratio,
                "effective_confidence": effective_conf,
                "effective_imbalance": effective_imb,
                "path": "+".join(paths) if paths else ("ALL" if require_all and fired else "—"),
                "path_count": len(paths),
                "path_required": 4 if require_all else 1,
                "confidence_required": effective_conf,
                "ready": ready,
                "status": "position" if sym in bot.pos else "pending" if sym in pending_map else "signal_ready" if ready else "monitoring",
            })

        trade_history = _recent_trades(bot, 200)
        realized_pnl = sum(float(t.get("pnl_quote", 0)) for t in trade_history)
        gross_profit = sum(max(0.0, float(t.get("pnl_quote", 0))) for t in trade_history)
        gross_loss = sum(max(0.0, -float(t.get("pnl_quote", 0))) for t in trade_history)
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (None if gross_profit <= 0 else 9.9)
        net_bps_samples = [
            (float(t.get("pnl_quote", 0)) / (float(t.get("entry", 0)) * float(t.get("qty", 0)))) * 1e4
            for t in trade_history
            if float(t.get("entry", 0)) > 0 and float(t.get("qty", 0)) > 0
        ]
        average_net_bps = (
            sum(net_bps_samples) / len(net_bps_samples)
            if net_bps_samples else 0.0
        )
        slots, alloc = bot.get_slot_allocation(eq)
        persistence_mode = (
            "postgres"
            if getattr(bot, "state_store", None) is not None
            else "local_persistent"
            if os.getenv("EFRA_STATE_FILE", "").startswith("/data/")
            else "local_ephemeral"
        )
        return {
            "ok": err is None,
            "engine": "halted" if bot.halted else "active",
            "error": err,
            "thread_alive": bool(thread and thread.is_alive()),
            "lease_status": lease_status,
            "persistence": persistence_mode,
            "started_at": started,
            "uptime_s": max(0.0, time.time() - started) if started else 0.0,
            "mode": "paper" if bot.c.paper else "live",
            "exchange": bot.c.exchange,
            "execution": bot.c.mode,
            "quote": bot.c.quote,
            "feed": "websocket" if bot.ws_engine else "rest",
            "equity": eq,
            "cash": float(bot._quote_free()),
            "start_equity": start_eq,
            "total_pnl": eq - start_eq,
            "total_pnl_pct": ((eq / start_eq) - 1.0) * 100 if start_eq else 0.0,
            "day_pnl": eq - day_eq,
            "day_pnl_pct": ((eq / day_eq) - 1.0) * 100 if day_eq else 0.0,
            "drawdown_pct": ((hwm - eq) / hwm) * 100 if hwm else 0.0,
            "trades": int(bot.n_trades),
            "wins": int(bot.n_wins),
            "realized_pnl": realized_pnl,
            "profit_factor": profit_factor,
            "average_net_bps_per_trade": average_net_bps,
            "win_rate": (bot.n_wins / bot.n_trades * 100.0) if bot.n_trades else 0.0,
            "compound_tier": int(bot.compound_tier),
            "loss_streak": int(bot.loss_streak),
            "slots": int(slots),
            "slot_allocation_pct": float(alloc) * 100.0,
            "btc_regime_safe": bool(getattr(bot, "last_macro_safe", True)),
            "btc_momentum_bps": float(getattr(bot, "last_btc_mom", 0.0)),
            "redeploy_pause_s": float(getattr(bot, "last_inter_trade_pause_remaining", 0.0)),
            "positions": positions,
            "pending": pending,
            "radar": radar,
            "recent_trades": trade_history[-20:],
            "config": {
                "tp_bps": bot.c.tp_bps,
                "sl_bps": bot.c.sl_bps,
                "breakeven_bps": bot.c.breakeven_bps,
                "trail_trigger_bps": bot.c.trail_trigger_bps,
                "trail_bps": bot.c.trail_bps,
                "min_confluence": bot.c.min_confluence,
                "imbalance_entry": bot.c.imbalance_entry,
                "min_cvd": bot.c.min_cvd,
                "min_mom_bps": bot.c.min_mom_bps,
                "max_mom_bps": bot.c.max_mom_bps,
                "min_mom_accel": bot.c.min_mom_accel,
                "min_wall_ratio": bot.c.min_wall_ratio,
                "vol_surge_factor": bot.c.vol_surge_factor,
                "smart_reentry": 1 if bot.c.smart_reentry else 0,
                "slow_bail_hold_s": bot.c.slow_bail_hold_s,
                "slow_bail_ret_bps": bot.c.slow_bail_ret_bps,
                "slow_bail_mom_bps": bot.c.slow_bail_mom_bps,
                "slow_bail_cvd": bot.c.slow_bail_cvd,
                "flip_bail_hold_s": bot.c.flip_bail_hold_s,
                "flip_bail_imb": bot.c.flip_bail_imb,
                "flip_bail_ret_bps": bot.c.flip_bail_ret_bps,
                "sl_wick_debounce_s": bot.c.sl_wick_debounce_s,
                "sl_cluster_window_s": bot.c.sl_cluster_window_s,
                "sl_cluster_count": bot.c.sl_cluster_count,
                "sl_cluster_pause_s": bot.c.sl_cluster_pause_s,
                "streak_conf_boost": bot.c.streak_conf_boost,
                "streak_obi_boost": bot.c.streak_obi_boost,
                "daily_loss_limit_pct": bot.c.daily_loss_limit_frac * 100.0,
                "loop_s": bot.c.loop_s,
            },
        }
    except Exception as exc:
        log.exception("telemetry snapshot failed")
        return {
            "ok": False,
            "engine": "telemetry_error",
            "error": f"{type(exc).__name__}: {exc}",
            "thread_alive": bool(thread and thread.is_alive()),
        }



LEGACY_DASHBOARD = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EFRA Ultra-Precision Sniper v2</title>
<style>
:root{
  color-scheme:dark;
  --bg:#050608;--panel:#07090d;--panel2:#0a0d12;--line:#1a1f28;
  --text:#f3f5f7;--muted:#8792a3;--green:#38d39f;--red:#ff696f;
  --amber:#e7b85c;--blue:#9db8ff;
}
*{box-sizing:border-box}html,body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif}
body{min-height:100vh}.mono{font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace}
.shell{min-height:100vh;background:linear-gradient(180deg,#050608 0%,#06080b 100%)}
header{padding:14px 20px 12px;border-bottom:1px solid var(--line);background:#050608;position:sticky;top:0;z-index:10}
.top{display:flex;justify-content:space-between;align-items:flex-start;gap:16px}
.brand{display:flex;gap:12px}.bar{width:4px;border-radius:4px;background:#d9dde5;height:38px}
.brand h1{font-size:16px;letter-spacing:.28em;margin:0 0 4px;font-weight:700}.brand p{margin:0;color:var(--muted);font-size:13px}
.account{text-align:right}.equity{font-size:20px}.pnl{font-size:12px;margin-top:2px}.good{color:var(--green)}.bad{color:var(--red)}.warn{color:var(--amber)}
.badges{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.badge{border:1px solid var(--line);border-radius:999px;padding:5px 10px;background:#11151c;color:#dbe2ea;font-size:12px}
.badge.good{background:rgba(56,211,159,.10);color:var(--green)}.badge.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:1.1fr 1fr .85fr;min-height:calc(100vh - 108px)}
.panel{border-right:1px solid var(--line);min-width:0;background:#050608}.panel:last-child{border-right:0}
.section-head{padding:12px 20px;border-bottom:1px solid var(--line);font-size:14px;font-weight:600}
.subtle{color:var(--muted);font-size:11px}
table{width:100%;border-collapse:collapse}.radar th,.radar td{padding:12px 12px;border-bottom:1px solid #11151b;text-align:right;font-size:12px}
.radar th:first-child,.radar td:first-child{text-align:left;padding-left:20px}.radar th{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em}
.radar tr{cursor:pointer}.radar tr:hover,.radar tr.active{background:#0a0d12}
.pair{font-size:15px;font-weight:700}.state{font-size:10px;color:var(--muted);margin-top:3px}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#5a606a;margin-right:8px}.dot.ready{background:var(--green)}.dot.position{background:var(--amber)}
.center{padding:0 20px 24px}.pair-top{display:flex;justify-content:space-between;align-items:flex-start;padding:16px 0}.pair-top h2{margin:0;font-size:20px}.price{text-align:right;font-size:20px}
.stats{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--line);border-radius:14px;overflow:hidden}
.stat{padding:14px;border-right:1px solid var(--line);border-bottom:1px solid var(--line);min-height:72px}.stat:nth-child(3n){border-right:0}.stat:nth-last-child(-n+3){border-bottom:0}
.k{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.08em}.v{font-size:18px;margin-top:7px}
.signal{margin-top:18px;border:1px solid #202630;border-radius:14px;padding:16px;background:#080b10}.signal-title{font-size:12px;letter-spacing:.08em;text-transform:uppercase}
.signal p{font-size:12px;color:#9da8b7;line-height:1.65;margin:12px 0 0}
.rules{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:18px}.rule .k{margin-bottom:5px}.rule .v{font-size:15px}
.book{padding:14px 20px}.book-eq{font-size:42px;font-weight:700;line-height:1.05}.book-pnl{font-size:16px;margin-top:8px}.policy{color:var(--muted);font-size:11px;margin-top:10px}
.chart{height:150px;margin:10px -6px 0}.chart svg{width:100%;height:100%}
.book-grid{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--line);border-radius:10px;overflow:hidden;margin-top:12px}.book-cell{padding:14px;border-right:1px solid var(--line);border-bottom:1px solid var(--line)}.book-cell:nth-child(2n){border-right:0}.book-cell:nth-last-child(-n+2){border-bottom:0}
.server-book{margin-top:18px;border-top:1px solid var(--line);padding-top:14px}.server-title{display:flex;justify-content:space-between;align-items:center;font-size:12px;letter-spacing:.08em}
.position-row{margin-top:10px;border:1px solid var(--line);border-radius:10px;padding:10px 12px}.position-row .row{display:flex;justify-content:space-between;gap:10px}.position-row small{color:var(--muted)}
.bottom{grid-column:1/-1;border-top:1px solid var(--line);display:grid;grid-template-columns:1.4fr 1fr;background:#050608}
.ledger,.runtime{padding:14px 20px}.ledger{border-right:1px solid var(--line)}.ledger table th,.ledger table td{padding:9px 8px;border-bottom:1px solid #11151b;font-size:11px;text-align:right}.ledger table th:first-child,.ledger table td:first-child{text-align:left}
.empty{color:var(--muted);font-size:12px;padding:18px 0}.runtime-list{display:grid;grid-template-columns:1fr 1fr;gap:8px}.runtime-item{border:1px solid var(--line);border-radius:9px;padding:10px}
@media(max-width:1050px){.grid{grid-template-columns:1fr}.panel{border-right:0;border-bottom:1px solid var(--line)}.bottom{grid-template-columns:1fr}.ledger{border-right:0;border-bottom:1px solid var(--line)}}
</style>
</head>
<body>
<div class="shell">
<header>
  <div class="top">
    <div class="brand"><div class="bar"></div><div><h1>EFRA</h1><p>Ultra-Precision Sniper v2</p></div></div>
    <div class="account"><div id="topEq" class="equity mono">$0.00</div><div id="topPnl" class="pnl mono">—</div></div>
  </div>
  <div class="badges" id="badges"></div>
</header>

<div class="grid">
  <section class="panel">
    <div class="section-head">Gate · live microstructure confluence <span id="watched" class="subtle mono"></span></div>
    <table class="radar">
      <thead><tr><th>Pair</th><th>Mid</th><th>OBI/CVD</th><th>Mom</th><th>Conf</th><th>Path</th></tr></thead>
      <tbody id="radarBody"></tbody>
    </table>
  </section>

  <section class="panel center">
    <div class="pair-top">
      <div><div class="subtle">SNIPER v2 MICROSTRUCTURE</div><h2 id="pairName" class="mono">—</h2></div>
      <div><div id="pairPrice" class="price mono">—</div><div id="pairMom" class="subtle mono">—</div></div>
    </div>
    <div class="stats">
      <div class="stat"><div class="k">Spread</div><div class="v mono" id="mSpread">—</div></div>
      <div class="stat"><div class="k">OBI</div><div class="v mono" id="mObi">—</div></div>
      <div class="stat"><div class="k">CVD</div><div class="v mono" id="mCvd">—</div></div>
      <div class="stat"><div class="k">Micro skew</div><div class="v mono" id="mSkew">—</div></div>
      <div class="stat"><div class="k">Confluence</div><div class="v mono" id="mConf">—</div></div>
      <div class="stat"><div class="k">Net edge</div><div class="v mono" id="mEdge">—</div></div>
    </div>
    <div class="signal">
      <div id="signalTitle" class="signal-title mono">● MONITORING · path —</div>
      <p>Entry gate combines OBI, CVD, micro-skew, confluence or tape with the cost-aware momentum floor, required path agreement and modeled post-cost edge. BTC regime and redeploy pause remain enforced by the server engine.</p>
    </div>
    <div class="rules">
      <div class="rule"><div class="k">TP</div><div class="v mono" id="rTp">—</div></div>
      <div class="rule"><div class="k">Hard SL</div><div class="v mono" id="rSl">—</div></div>
      <div class="rule"><div class="k">Profit lock</div><div class="v mono" id="rBe">—</div></div>
      <div class="rule"><div class="k">Trail trigger</div><div class="v mono" id="rTrail">—</div></div>
      <div class="rule"><div class="k">Mom gate</div><div class="v mono" id="rMom">—</div></div>
      <div class="rule"><div class="k">Path agreement</div><div class="v mono" id="rPaths">—</div></div>
    </div>
  </section>

  <aside class="panel book">
    <div class="section-head" style="padding:0 0 14px;border:0">Gate autonomous book</div>
    <div id="bookEq" class="book-eq mono">$0.00</div>
    <div id="bookPnl" class="book-pnl mono">—</div>
    <div id="policy" class="policy">policy sniper-v2</div>
    <div class="chart"><svg id="eqChart" viewBox="0 0 400 150" preserveAspectRatio="none"></svg></div>
    <div class="book-grid">
      <div class="book-cell"><div class="k">Cash</div><div id="cash" class="v mono">—</div></div>
      <div class="book-cell"><div class="k">Drawdown</div><div id="dd" class="v mono">—</div></div>
      <div class="book-cell"><div class="k">Win rate</div><div id="wr" class="v mono">—</div></div>
      <div class="book-cell"><div class="k">Profit factor</div><div id="pf" class="v mono">—</div></div>
    </div>
    <div class="server-book">
      <div class="server-title"><span>SERVER BOOK</span><span id="bookCounts" class="mono subtle">0 open · 0 resting</span></div>
      <div id="bookRows"></div>
    </div>
  </aside>

  <div class="bottom">
    <section class="ledger">
      <div class="section-head" style="padding:0 0 10px;border:0">Realized trade ledger</div>
      <table><thead><tr><th>Pair</th><th>P&amp;L</th><th>Reason</th><th>Equity</th><th>Tier</th></tr></thead><tbody id="ledgerBody"></tbody></table>
    </section>
    <section class="runtime">
      <div class="section-head" style="padding:0 0 10px;border:0">Runtime / risk</div>
      <div class="runtime-list" id="runtimeList"></div>
    </section>
  </div>
</div>
</div>
<script>
let state=null, selected=null;
const $=id=>document.getElementById(id);
const money=x=>'$'+Number(x||0).toFixed(2);
const num=(x,d=1)=>Number(x||0).toFixed(d);
const pct=x=>Number(x||0).toFixed(1)+'%';
const cls=x=>Number(x||0)>=0?'good':'bad';
function fmtPx(x){x=Number(x||0);return x>=1000?x.toLocaleString(undefined,{maximumFractionDigits:2}):x>=1?x.toFixed(4):x.toFixed(6)}
function drawChart(s){
  const svg=$('eqChart'), trades=s.recent_trades||[], start=Number(s.start_equity||100);
  const vals=[start]; for(const t of trades) vals.push(Number(t.equity||vals[vals.length-1]));
  const min=Math.min(...vals), max=Math.max(...vals), span=Math.max(.01,max-min);
  const pts=vals.map((v,i)=>[(i/(Math.max(1,vals.length-1)))*390+5,140-((v-min)/span)*120]);
  const d=pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');
  svg.innerHTML='<path d="'+d+'" fill="none" stroke="#38d39f" stroke-width="2"/>';
}
function renderPair(){
  const rows=state?.radar||[]; if(!rows.length)return;
  if(!selected || !rows.some(r=>r.symbol===selected)) selected=rows[0].symbol;
  const r=rows.find(x=>x.symbol===selected)||rows[0], c=state.config||{};
  $('pairName').textContent=(r.symbol||'—').replace('/',' / ');
  $('pairPrice').textContent=fmtPx(r.mid);
  $('pairMom').textContent='mom '+(r.momentum_bps>=0?'+':'')+num(r.momentum_bps)+' bps';
  $('pairMom').className='subtle mono '+cls(r.momentum_bps);
  $('mSpread').textContent=num(r.spread_bps,2)+' bps';
  $('mObi').textContent=num((r.obi||0)*100,1)+'%'; $('mObi').className='v mono '+((r.obi||0)>=.5?'good':'bad');
  $('mCvd').textContent=num((r.cvd||0)*100,1)+'%'; $('mCvd').className='v mono '+((r.cvd||0)>=.5?'good':'bad');
  $('mSkew').textContent=num(r.micro_skew_bps,2)+' bps'; $('mSkew').className='v mono '+cls(r.micro_skew_bps);
  $('mConf').textContent=num(r.confluence,1);
  $('mEdge').textContent=(r.net_edge_bps>=0?'+':'')+num(r.net_edge_bps,1)+' bps'; $('mEdge').className='v mono '+cls(r.net_edge_bps);
  $('signalTitle').textContent='● '+(r.ready?'SIGNAL READY':String(r.status||'MONITORING').toUpperCase())+' · path '+(r.path||'—');
  $('signalTitle').className='signal-title mono '+(r.ready?'good':'');
  $('rTp').textContent=num(c.tp_bps,0)+' bps'; $('rSl').textContent=num(c.sl_bps,0)+' bps'; $('rBe').textContent=num(c.breakeven_bps,0)+' bps';
  $('rTrail').textContent=num(c.trail_trigger_bps,0)+' / '+num(c.trail_bps,0)+' bps';
  $('rMom').textContent='max('+num(c.min_mom_bps,1)+', '+num((c.min_impulse_cost_ratio||.3)*100,0)+'% cost)';
  $('rPaths').textContent=num(c.min_signal_paths||2,0)+' of 3 · conf '+num(c.min_entry_confidence||65,0)+'+';
}
function render(){
  const s=state;if(!s)return;
  $('topEq').textContent=money(s.equity); $('topPnl').textContent=(s.total_pnl>=0?'+':'')+money(s.total_pnl)+' · '+(s.total_pnl_pct>=0?'+':'')+num(s.total_pnl_pct,2)+'%';
  $('topPnl').className='pnl mono '+cls(s.total_pnl);
  $('badges').innerHTML=[
    '<span class="badge good">● Autonomous Gate · '+String(s.engine||'—')+'</span>',
    '<span class="badge good">Gate · '+String(s.mode||'—').toUpperCase()+' · '+String(s.feed||'—').toUpperCase()+'</span>',
    '<span class="badge muted">'+String(s.persistence||'—').replaceAll('_',' ')+'</span>',
    '<span class="badge muted">sniper-v2</span>'
  ].join('');
  const radar=s.radar||[]; $('watched').textContent=radar.length+' watched';
  $('radarBody').innerHTML=radar.map(r=>'<tr data-sym="'+r.symbol+'" class="'+(r.symbol===selected?'active':'')+'"><td><div class="pair mono"><span class="dot '+(r.status==='position'?'position':r.ready?'ready':'')+'"></span>'+r.symbol.replace('/USDT','/')+'</div><div class="state">'+(r.status||'monitoring')+' · '+num(r.spread_bps,1)+' bps</div></td><td class="mono">'+fmtPx(r.mid)+'</td><td class="mono">'+num((r.obi||0)*100,0)+' / '+num((r.cvd||0)*100,0)+'</td><td class="mono '+cls(r.momentum_bps)+'">'+(r.momentum_bps>=0?'+':'')+num(r.momentum_bps,1)+'</td><td class="mono">'+num(r.confluence,1)+'</td><td class="mono">'+(r.path||'—')+'</td></tr>').join('');
  document.querySelectorAll('#radarBody tr').forEach(el=>el.onclick=()=>{selected=el.dataset.sym;render();});
  renderPair();
  $('bookEq').textContent=money(s.equity); $('bookPnl').textContent=(s.total_pnl>=0?'+':'')+money(s.total_pnl)+' · '+(s.total_pnl_pct>=0?'+':'')+num(s.total_pnl_pct,2)+'%';
  $('bookPnl').className='book-pnl mono '+cls(s.total_pnl);
  $('policy').textContent='policy sniper-v2 · '+(s.trades||0)+' closed · '+num(s.average_net_bps_per_trade,2)+' bps/trade';
  $('cash').textContent=money(s.cash); $('dd').textContent=(s.drawdown_pct>=0?'+':'')+num(s.drawdown_pct,2)+'%'; $('dd').className='v mono '+(s.drawdown_pct>0?'bad':'good');
  $('wr').textContent=pct(s.win_rate); $('pf').textContent=s.profit_factor==null?'—':num(s.profit_factor,2);
  drawChart(s);
  const pos=s.positions||[], pend=s.pending||[]; $('bookCounts').textContent=pos.length+' open · '+pend.length+' resting';
  const book=[...pos.map(p=>'<div class="position-row"><div class="row"><b class="mono">'+p.symbol+'</b><span class="mono '+cls(p.pnl_quote)+'">'+(p.pnl_quote>=0?'+':'')+money(p.pnl_quote)+'</span></div><small class="mono">'+fmtPx(p.entry)+' → '+fmtPx(p.current)+' · '+num(p.ret_bps,1)+' bps · '+(p.signal_path||'—')+'</small></div>'),
    ...pend.map(p=>'<div class="position-row"><div class="row"><b class="mono">'+p.symbol+' RESTING</b><span class="mono">'+num(p.age_s,1)+'s</span></div><small class="mono">'+fmtPx(p.price)+' · '+num(p.qty,6)+'</small></div>')];
  $('bookRows').innerHTML=book.join('')||'<div class="empty">No open autonomous inventory.</div>';
  const trades=[...(s.recent_trades||[])].reverse();
  $('ledgerBody').innerHTML=trades.map(t=>'<tr><td class="mono">'+t.symbol+'</td><td class="mono '+cls(t.pnl_quote)+'">'+(t.pnl_quote>=0?'+':'')+money(t.pnl_quote)+'</td><td>'+t.reason+'</td><td class="mono">'+money(t.equity)+'</td><td class="mono">'+t.tier+'</td></tr>').join('')||'<tr><td colspan="5" class="empty">No realized trades yet.</td></tr>';
  const runtime=[
    ['Engine',String(s.engine||'—').toUpperCase()],['Thread',s.thread_alive?'ALIVE':'DOWN'],
    ['Persistence',String(s.persistence||'—').replaceAll('_',' ')],['Lease',String(s.lease_status||'disabled').toUpperCase()],
    ['Slots',(pos.length+pend.length)+' / '+(s.slots||0)],['Allocation',num(s.slot_allocation_pct,0)+'% / slot'],
    ['BTC regime',s.btc_regime_safe?'SAFE':'BLOCK'],['BTC momentum',num(s.btc_momentum_bps,1)+' bp']
  ];
  $('runtimeList').innerHTML=runtime.map(x=>'<div class="runtime-item"><div class="k">'+x[0]+'</div><div class="v mono">'+x[1]+'</div></div>').join('');
}
async function tick(){
  try{const r=await fetch('/api/status',{cache:'no-store'});state=await r.json();render();}
  catch(e){$('badges').innerHTML='<span class="badge bad">Server telemetry unavailable</span>';}
}
tick();setInterval(tick,1500);
</script>
</body>
</html>"""


DASHBOARD = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EFRA Ultra-Precision Sniper v2</title>
<style>
:root{color-scheme:dark;--bg:#05070b;--panel:#0b1018;--line:#182231;--text:#e8eef7;--muted:#7d8da4;--good:#42e695;--bad:#ff647c;--warn:#f6c85f;--cyan:#65d7ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#0c1724 0,#05070b 42%);color:var(--text);font:14px/1.4 Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif}
.wrap{max-width:1500px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:14px}.brand h1{font-size:22px;margin:0 0 4px}.brand small,.muted{color:var(--muted)}
.badge{border:1px solid var(--line);background:#09111b;border-radius:999px;padding:7px 10px;font-weight:700}.grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}.card,.panel{background:rgba(11,16,24,.92);border:1px solid var(--line);border-radius:12px;box-shadow:0 18px 50px rgba(0,0,0,.18)}
.card{padding:13px}.k{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}.v{font-size:20px;font-weight:800;margin-top:4px}.panel{padding:14px;margin-top:10px;overflow:auto}.panel h2{font-size:13px;margin:0 0 10px;color:#cbd8e8;text-transform:uppercase;letter-spacing:.08em}
.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}.cyan{color:var(--cyan)}
table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{text-align:right;padding:8px 9px;border-bottom:1px solid #111b27}th:first-child,td:first-child{text-align:left}th{font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--muted)}td{font-variant-numeric:tabular-nums}
.two{display:grid;grid-template-columns:1.2fr .8fr;gap:10px}.statusline{display:flex;gap:7px;flex-wrap:wrap;margin-top:7px}.pill{padding:3px 7px;border-radius:6px;background:#101927;color:#9fb0c6;font-size:11px}
@media(max-width:1000px){.grid{grid-template-columns:repeat(3,1fr)}.two{grid-template-columns:1fr}}@media(max-width:600px){.grid{grid-template-columns:repeat(2,1fr)}.wrap{padding:10px}.top{flex-direction:column}}
</style>
</head>
<body><div class="wrap">
<div class="top"><div class="brand"><h1>EFRA ULTRA-PRECISION SNIPER v2</h1><small>Engine-native production telemetry · no duplicated strategy state</small><div class="statusline" id="statusline"></div></div><div class="badge" id="engine">STARTING</div></div>
<div class="grid" id="metrics"></div>
<div class="two">
<div class="panel"><h2>Autonomous Positions & Resting Orders</h2><table><thead><tr><th>Pair</th><th>Entry</th><th>Current</th><th>Return</th><th>P&amp;L</th><th>Peak</th><th>Protection</th></tr></thead><tbody id="positions"></tbody></table></div>
<div class="panel"><h2>Risk / Compounding</h2><div id="risk"></div></div>
</div>
<div class="panel"><h2>Live Alpha Confluence Radar</h2><table><thead><tr><th>Pair</th><th>Mid</th><th>Spread</th><th>OBI</th><th>CVD</th><th>µSkew</th><th>Mom</th><th>Conf</th><th>Net Edge</th><th>Path</th><th>State</th></tr></thead><tbody id="radar"></tbody></table></div>
<div class="panel"><h2>Realized Trade Ledger</h2><table><thead><tr><th>Time</th><th>Pair</th><th>Entry</th><th>Exit</th><th>P&amp;L</th><th>Reason</th><th>Equity</th><th>Tier</th></tr></thead><tbody id="trades"></tbody></table></div>
</div>
<script>
const $=id=>document.getElementById(id),n=(x,d=2)=>Number(x||0).toFixed(d),pc=x=>x>=0?'good':'bad';
function card(k,v,c=''){return '<div class="card"><div class="k">'+k+'</div><div class="v '+c+'">'+v+'</div></div>'}
async function tick(){
 try{
  const s=await fetch('/api/status',{cache:'no-store'}).then(r=>r.json());
  $('engine').textContent=(s.engine||'unknown').toUpperCase();$('engine').className='badge '+(s.engine==='active'?'good':s.engine==='starting'?'warn':'bad');
  $('statusline').innerHTML=['Mode '+String(s.mode||'—').toUpperCase(),'Exchange '+String(s.exchange||'—').toUpperCase(),'Execution '+String(s.execution||'—').toUpperCase(),'Feed '+String(s.feed||'—').toUpperCase(),'Thread '+(s.thread_alive?'ALIVE':'DOWN'),'Lease '+String(s.lease_status||'—').toUpperCase(),'State '+String(s.persistence||'—').toUpperCase()].map(x=>'<span class="pill">'+x+'</span>').join('');
  $('metrics').innerHTML=card('Equity','$'+n(s.equity),pc(s.total_pnl))+card('Total P&L',(s.total_pnl>=0?'+':'')+'$'+n(s.total_pnl)+' · '+n(s.total_pnl_pct)+'%',pc(s.total_pnl))+card('Today',(s.day_pnl>=0?'+':'')+'$'+n(s.day_pnl)+' · '+n(s.day_pnl_pct)+'%',pc(s.day_pnl))+card('Closed',String(s.trades||0)+' · '+String(s.wins||0)+'W')+card('Win Rate',n(s.win_rate,1)+'%')+card('Drawdown',n(s.drawdown_pct,2)+'%',s.drawdown_pct>0?'warn':'good');
  const pos=[...(s.positions||[])].map(p=>'<tr><td>'+p.symbol+'</td><td>'+n(p.entry,6)+'</td><td>'+n(p.current,6)+'</td><td class="'+pc(p.ret_bps)+'">'+n(p.ret_bps,1)+' bp</td><td class="'+pc(p.pnl_quote)+'">'+n(p.pnl_quote,4)+'</td><td>'+n(p.peak_ret_bps,1)+' bp</td><td>'+(p.trailing_active?'TRAIL '+n(p.stop_bps,1):p.be_locked?'BE '+n(p.stop_bps,1):'SL '+n(p.stop_bps,1))+'</td></tr>');
  const pend=[...(s.pending||[])].map(p=>'<tr><td>'+p.symbol+' · PENDING</td><td>'+n(p.price,6)+'</td><td>—</td><td>—</td><td>—</td><td>—</td><td>'+n(p.age_s,1)+'s</td></tr>');
  $('positions').innerHTML=[...pos,...pend].join('')||'<tr><td colspan="7" class="muted">Scanning for high-confluence setups…</td></tr>';
  $('risk').innerHTML='<div class="grid" style="grid-template-columns:repeat(2,1fr)">'+card('Slots',(s.positions?.length||0)+(s.pending?.length||0)+' / '+(s.slots||0))+card('Allocation',n(s.slot_allocation_pct,0)+'% / slot')+card('Compound Tier',s.compound_tier||1)+card('Loss Streak',s.loss_streak||0,(s.loss_streak||0)>=2?'warn':'good')+card('BTC Regime',s.btc_regime_safe?'SAFE':'BLOCK',s.btc_regime_safe?'good':'bad')+card('BTC Momentum',n(s.btc_momentum_bps,1)+' bp',pc(s.btc_momentum_bps))+'</div>';
  $('radar').innerHTML=(s.radar||[]).map(r=>'<tr><td>'+r.symbol+'</td><td>'+n(r.mid,6)+'</td><td>'+n(r.spread_bps,1)+'</td><td>'+n((r.obi||0)*100,1)+'%</td><td>'+n((r.cvd||0)*100,1)+'%</td><td>'+n(r.micro_skew_bps,2)+'</td><td>'+n(r.momentum_bps,1)+'</td><td>'+n(r.confluence,1)+'</td><td class="'+pc(r.net_edge_bps)+'">'+n(r.net_edge_bps,1)+'</td><td>'+String(r.path||'—')+'</td><td class="'+(r.ready?'good':'muted')+'">'+String(r.status||'')+'</td></tr>').join('')||'<tr><td colspan="11" class="muted">Waiting for scanner…</td></tr>';
  $('trades').innerHTML=[...(s.recent_trades||[])].reverse().map(t=>'<tr><td>'+new Date(t.ts*1000).toLocaleTimeString()+'</td><td>'+t.symbol+'</td><td>'+n(t.entry,6)+'</td><td>'+n(t.exit,6)+'</td><td class="'+pc(t.pnl_quote)+'">'+(t.pnl_quote>=0?'+':'')+n(t.pnl_quote,4)+'</td><td>'+t.reason+'</td><td>'+n(t.equity,2)+'</td><td>'+t.tier+'</td></tr>').join('')||'<tr><td colspan="8" class="muted">No completed trades yet.</td></tr>';
 }catch(e){$('engine').textContent='TELEMETRY OFFLINE';$('engine').className='badge bad'}
}
tick();setInterval(tick,1500);
</script></body></html>"""


def engine_enabled() -> bool:
    """Whether the trading engine is allowed to run inside the web container."""
    return _env_bool("EFRA_ENGINE_ENABLED", True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if engine_enabled():
        start_engine_once()
    else:
        log.warning(
            "EFRA web started in UI-ONLY research mode; execution engine is disabled"
        )
    yield


app = FastAPI(title="EFRA Ultra-Precision Sniper v2", version="2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://efra-microstructure-paper-live.onrender.com",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


RENDER_TERMINAL_ORIGIN = "https://efra-microstructure-paper-live.onrender.com"
VPS_PUBLIC_ORIGIN = "https://efra-sniper-v2.169-58-175-192.nip.io"

def _proxy_terminal_asset(path: str, query: str = "") -> Response:
    upstream = f"{RENDER_TERMINAL_ORIGIN}/{path.lstrip('/')}"
    if query:
        upstream += "?" + query
    req = urllib.request.Request(
        upstream,
        headers={
            "User-Agent": "EFRA-VPS-UI-Proxy/1.0",
            "Accept-Encoding": "identity",
            "Accept": "*/*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read()
            content_type = resp.headers.get("content-type", "application/octet-stream")
            status = resp.status
    except urllib.error.HTTPError as exc:
        body = exc.read()
        content_type = exc.headers.get("content-type", "text/plain")
        status = exc.code
    except Exception as exc:
        return Response(
            content=f"EFRA terminal upstream unavailable: {type(exc).__name__}: {exc}",
            status_code=502,
            media_type="text/plain",
        )

    textual = (
        "text/" in content_type
        or "javascript" in content_type
        or "json" in content_type
        or "xml" in content_type
    )
    if textual:
        text = body.decode("utf-8", errors="replace")
        # Preserve the exact existing EFRA terminal, but make its Sniper v2
        # telemetry authoritative from this continuously running VPS.
        text = text.replace(
            "https://efra-sniper-v2.onrender.com",
            VPS_PUBLIC_ORIGIN,
        )
        body = text.encode("utf-8")

    return Response(
        content=body,
        status_code=status,
        headers={"cache-control": "no-store"},
        media_type=content_type.split(";", 1)[0],
    )


@app.get("/")
def dashboard(request: Request) -> Response:
    return _proxy_terminal_asset("", request.url.query)


@app.get("/native", response_class=HTMLResponse)
def native_dashboard() -> str:
    return DASHBOARD


@app.get("/terminal")
def legacy_terminal() -> RedirectResponse:
    terminal_url = os.getenv(
        "EFRA_TERMINAL_URL",
        "https://efra-microstructure-paper-live.onrender.com",
    )
    return RedirectResponse(terminal_url, status_code=307)


@app.get("/health")
def health() -> dict[str, Any]:
    s = snapshot()
    execution_paused = bool(s.get("execution_paused"))
    return {
        "ok": bool(s.get("ok") and (s.get("thread_alive") or execution_paused)),
        "engine": s.get("engine"),
        "mode": s.get("mode"),
        "exchange": s.get("exchange"),
        "thread_alive": s.get("thread_alive"),
        "execution_paused": execution_paused,
        "research_connected": bool((s.get("research") or {}).get("connected")),
        "research_age_s": (s.get("research") or {}).get("age_s"),
        "error": s.get("error"),
    }


@app.get("/api/status")
def api_status() -> dict[str, Any]:
    return snapshot()


@app.get("/api/trades")
def api_trades() -> dict[str, Any]:
    s = snapshot()
    return {"trades": s.get("recent_trades", []), "count": s.get("trades", 0)}


@app.get("/api/positions")
def api_positions() -> dict[str, Any]:
    s = snapshot()
    return {"positions": s.get("positions", []), "pending": s.get("pending", [])}


@app.get("/api/radar")
def api_radar() -> dict[str, Any]:
    return {"radar": snapshot().get("radar", [])}


@app.get("/{path:path}")
def terminal_assets(path: str, request: Request) -> Response:
    # Specific local API/health routes above remain authoritative. Everything
    # else is the already-built EFRA terminal UI and its static assets.
    return _proxy_terminal_asset(path, request.url.query)
