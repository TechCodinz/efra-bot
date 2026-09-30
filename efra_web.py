#!/usr/bin/env python3
"""EFRA Sniper v2 web runtime.

Runs the existing EFRA trading engine in a background thread and exposes
read-only telemetry plus a browser dashboard. Trading logic stays in
efra_bot.py; this module is only the production web/observability shell.
"""

import csv
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

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
    c.min_impulse_cost_ratio = _env_float("EFRA_MIN_IMPULSE_COST_RATIO", c.min_impulse_cost_ratio)
    c.min_entry_confidence = _env_float("EFRA_MIN_ENTRY_CONFIDENCE", c.min_entry_confidence)
    c.min_signal_paths = _env_int("EFRA_MIN_SIGNAL_PATHS", c.min_signal_paths)
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
            state_key = f"{cfg.exchange}:{cfg.mode}:{'paper' if cfg.paper else 'live'}:{cfg.quote}"
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
            })
        except (TypeError, ValueError):
            continue
    return out


def snapshot() -> dict[str, Any]:
    with _state_lock:
        bot = _bot
        thread = _engine_thread
        err = _engine_error
        started = _engine_started_at
        lease_status = _lease_status

    if bot is None:
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
                "entry_mom_bps": float(p.get("entry_mom_bps", 0.0)),
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
            cost = 2 * (bot.c.fee_bps + bot.c.slippage_bps) + spread
            net_edge = bot.c.tp_bps - cost
            mom_ok = bot.c.min_mom_bps <= mom <= bot.c.max_mom_bps
            sig_obi = imb >= bot.c.imbalance_entry and skew >= 0.8 and cvd >= bot.c.min_cvd and mom >= bot.c.min_mom_bps
            sig_conf = conf >= bot.c.min_confluence and imb >= 0.62 and cvd >= bot.c.min_cvd and mom >= bot.c.min_mom_bps
            sig_tape = cvd >= 0.72 and imb >= 0.60 and skew >= 0.8 and mom >= bot.c.min_mom_bps
            paths = [name for name, passed in (("OBI", sig_obi), ("CONF", sig_conf), ("TAPE", sig_tape)) if passed]
            ready = mom_ok and bool(paths) and net_edge >= bot.c.min_net_edge_bps
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
                "path": "+".join(paths) if paths else "—",
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
        return {
            "ok": err is None,
            "engine": "halted" if bot.halted else "active",
            "error": err,
            "thread_alive": bool(thread and thread.is_alive()),
            "lease_status": lease_status,
            "persistence": "postgres" if getattr(bot, "state_store", None) is not None else "local_ephemeral",
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
                "min_impulse_cost_ratio": bot.c.min_impulse_cost_ratio,
                "min_entry_confidence": bot.c.min_entry_confidence,
                "min_signal_paths": bot.c.min_signal_paths,
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


@asynccontextmanager
async def lifespan(_: FastAPI):
    start_engine_once()
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


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return DASHBOARD


@app.get("/health")
def health() -> dict[str, Any]:
    s = snapshot()
    return {
        "ok": bool(s.get("ok") and s.get("thread_alive")),
        "engine": s.get("engine"),
        "mode": s.get("mode"),
        "exchange": s.get("exchange"),
        "thread_alive": s.get("thread_alive"),
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
