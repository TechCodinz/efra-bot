#!/usr/bin/env python3
"""
EFRA DASHBOARD - Real-time terminal HUD for monitoring Efra Bot.
Uses 'rich' for a high-performance visual display of telemetry,
order books, signal confluence, active positions, and compounding progress.
"""

import csv
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text


class EfraDashboard:
    def __init__(self, bot):
        self.bot = bot
        self.console = Console()
        self.last_render_ts = 0.0

    def render(self):
        now = time.time()
        if now - self.last_render_ts < 0.8:
            return
        self.last_render_ts = now

        b = self.bot
        c = b.c
        eq = b.equity(b.last_books if hasattr(b, "last_books") else {})
        start_eq = getattr(b, "start_balance", c.start_balance)
        pnl_tot = eq - start_eq
        pnl_pct = (eq / start_eq - 1.0) * 100.0 if start_eq > 0 else 0.0
        day_start_eq = getattr(b, "day_start_eq", None) or eq
        day_pnl = eq - day_start_eq
        day_pnl_pct = (eq / day_start_eq - 1.0) * 100.0 if day_start_eq > 0 else 0.0
        hwm = max(float(getattr(b, "high_water_mark", eq) or eq), eq)
        drawdown_pct = ((hwm - eq) / hwm * 100.0) if hwm > 0 else 0.0
        slots, alloc_frac = b.get_slot_allocation(eq)
        pending = getattr(b, "pending", {})
        open_risk = len(b.pos) + len(pending)
        loss_streak = getattr(b, "loss_streak", 0)
        macro_safe = getattr(b, "last_macro_safe", True)
        btc_mom = getattr(b, "last_btc_mom", 0.0)
        pause_remaining = getattr(b, "last_inter_trade_pause_remaining", 0.0)
        recent_trades = []
        try:
            with open(c.log_file, "r", encoding="utf-8", newline="") as trade_file:
                recent_trades = list(csv.DictReader(trade_file))[-6:]
        except (OSError, csv.Error):
            recent_trades = []

        # Tier & Compounding calculations
        tier = getattr(b, "compound_tier", 1)
        base_tier_eq = getattr(b, "last_tier_equity", start_eq)
        next_tier_eq = base_tier_eq * (1.0 + c.compound_step)
        tier_prog = max(0.0, min(100.0, (eq - base_tier_eq) / (next_tier_eq - base_tier_eq) * 100.0)) if (next_tier_eq > base_tier_eq) else 0.0

        # Create Layout
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="metrics", size=5),
            Layout(name="risk", size=4),
            Layout(name="positions", size=7),
            Layout(name="watchlist", size=10),
            Layout(name="trades", size=7),
            Layout(name="footer", size=3),
        )

        # Header
        mode_str = "[bold green]PAPER MODE[/]" if c.paper else "[bold red]LIVE TRADING[/]"
        ws_str = "[bold cyan]WEBSOCKET (ccxt.pro)[/]" if getattr(b, "ws_engine", None) else "[yellow]REST POLLING[/]"
        header_text = Text.from_markup(
            f" [bold white]EFRA ULTRA-PRECISION SNIPER v2[/] | {mode_str} | Exchange: [bold yellow]{c.exchange.upper()}[/] | "
            f"Execution: [bold cyan]{c.mode.upper()}[/] | Feed: {ws_str} | Quote: [bold]{c.quote}[/]"
        )
        layout["header"].update(Panel(header_text, style="blue"))

        # Metrics Panel
        pnl_color = "green" if pnl_tot >= 0 else "red"
        win_rate = (b.n_wins / b.n_trades * 100.0) if b.n_trades > 0 else 0.0
        metrics_table = Table.grid(expand=True)
        for _ in range(6):
            metrics_table.add_column(ratio=1)

        day_color = "green" if day_pnl >= 0 else "red"
        metrics_table.add_row(
            f" [bold]Equity[/]\n[bold {pnl_color}]${eq:.2f}[/]",
            f" [bold]Total P&L[/]\n[{pnl_color}]{pnl_tot:+.2f} ({pnl_pct:+.2f}%)[/]",
            f" [bold]Today[/]\n[{day_color}]{day_pnl:+.2f} ({day_pnl_pct:+.2f}%)[/]",
            f" [bold]Closed[/]\n{b.n_trades} · [green]{b.n_wins}W[/]/[red]{b.n_trades - b.n_wins}L[/]",
            f" [bold]Win Rate[/]\n[{pnl_color}]{win_rate:.1f}%[/]",
            f" [bold]Drawdown[/]\n{'[red]' if drawdown_pct > 0 else '[green]'}{drawdown_pct:.2f}%[/]",
        )
        layout["metrics"].update(Panel(metrics_table, title="Realized Performance & Account Telemetry", style="cyan"))

        risk_table = Table.grid(expand=True)
        for _ in range(6):
            risk_table.add_column(ratio=1)
        risk_table.add_row(
            f" [bold]Engine[/]\n{'[red]HALTED[/]' if b.halted else '[green]ACTIVE[/]'}",
            f" [bold]Slots[/]\n{open_risk}/{slots} · {alloc_frac*100:.0f}% each",
            f" [bold]Compounding[/]\nTier {tier} · {tier_prog:.0f}% to +{c.compound_step*100:.0f}%",
            f" [bold]Loss Shield[/]\n{'[yellow]DEFENSIVE[/]' if loss_streak >= 2 else '[green]NORMAL[/]'} · streak {loss_streak}",
            f" [bold]BTC Regime[/]\n{'[green]SAFE[/]' if macro_safe else '[red]BLOCK[/]'} {btc_mom:+.1f}bps",
            f" [bold]Redeploy Gate[/]\n{'[green]READY[/]' if pause_remaining <= 0 else '[yellow]PAUSE[/] ' + format(pause_remaining, '.1f') + 's'}",
        )
        layout["risk"].update(Panel(risk_table, title="Autonomous Risk & Compounding Control", style="yellow"))

        # Active Positions Table
        pos_table = Table(expand=True, box=None)
        pos_table.add_column("Symbol", style="bold")
        pos_table.add_column("Entry Px", justify="right")
        pos_table.add_column("Current Px", justify="right")
        pos_table.add_column("Hold", justify="right")
        pos_table.add_column("P&L (bps / $)", justify="right")
        pos_table.add_column("Peak", justify="right")
        pos_table.add_column("Protection", justify="center")
        pos_table.add_column("Target TP", justify="right")

        if b.pos:
            for sym, p in b.pos.items():
                bk = (b.last_books.get(sym) if hasattr(b, "last_books") else None) or {}
                current_px = bk.get("ask", p["entry"]) if c.mode == "maker" else bk.get("bid", bk.get("mid", p["entry"]))
                ret_bps = (current_px - p["entry"]) / p["entry"] * 1e4
                pnl_dollar = (current_px - p["entry"]) * p["qty"]
                hold_s = int(now - p["ts"])
                color = "green" if ret_bps >= 0 else "red"

                stop_info = f"[yellow]{p.get('stop_bps', -c.sl_bps):+.1f} bps[/]"
                if p.get("trailing_active"):
                    stop_info = f"[bold cyan]TRAILING ({p.get('stop_bps', 0):+.1f} bps)[/]"
                elif p.get("be_locked"):
                    stop_info = f"[bold green]BE LOCKED ({p.get('stop_bps', 0):+.1f} bps)[/]"

                pos_table.add_row(
                    sym,
                    f"{p['entry']:.5g}",
                    f"{current_px:.5g}",
                    f"{hold_s}s",
                    f"[{color}]{ret_bps:+.1f} bps ({pnl_dollar:+.3f})[/]",
                    f"{p.get('peak_ret_bps', 0.0):+.1f} bps",
                    stop_info,
                    f"{p['entry'] * (1 + c.tp_bps / 1e4):.5g} (+{c.tp_bps:.0f} bps)",
                )
        else:
            pos_table.add_row("[dim]No active positions (Scanning for high-confluence setups...)[/]", "", "", "", "", "", "", "")

        for sym, pd in pending.items():
            age = now - pd.get("ts", now)
            pos_table.add_row(
                f"{sym} [yellow]PENDING[/]",
                f"{pd.get('px', 0):.5g}",
                "—",
                f"{age:.1f}s",
                "maker bid",
                "—",
                f"cancel < OBI {c.maker_cancel_imb:.2f}",
                f"TTL {c.maker_timeout_s}s",
            )

        layout["positions"].update(Panel(pos_table, title="Autonomous Positions & Resting Maker Orders", style="magenta"))

        # Watchlist & Signal Confluence Table
        watch_table = Table(expand=True, box=None)
        watch_table.add_column("Pair", style="bold")
        watch_table.add_column("Mid Price", justify="right")
        watch_table.add_column("Spread", justify="right")
        watch_table.add_column("OBI (Depth)", justify="right")
        watch_table.add_column("CVD (Tape)", justify="right")
        watch_table.add_column("µSkew", justify="right")
        watch_table.add_column("Mom", justify="right")
        watch_table.add_column("Confluence", justify="center")
        watch_table.add_column("Net Edge", justify="right")
        watch_table.add_column("Path", justify="center")
        watch_table.add_column("Status", justify="left")

        watched = getattr(b, "watch", [])
        last_books = getattr(b, "last_books", {})
        for sym in watched[:10]:
            bk = last_books.get(sym)
            if bk:
                spread = bk.get("spread", 0.0)
                imb = bk.get("imb", 0.5)
                cvd = bk.get("cvd_5s", 0.5)
                mom = bk.get("mom", 0.0)
                conf = bk.get("confluence", 0.0)
                micro_skew = bk.get("micro_skew", 0.0)
                mid = bk.get("mid", 0.0)

                # Color coding + exact execution gate parity
                momentum_ok = c.min_mom_bps <= mom <= getattr(c, "max_mom_bps", 25.0)
                imb_str = f"[green]{imb*100:.1f}%[/]" if imb >= c.imbalance_entry else f"[dim]{imb*100:.1f}%[/]"
                cvd_str = f"[green]{cvd*100:.1f}%[/]" if cvd >= c.min_cvd else f"[dim]{cvd*100:.1f}%[/]"
                mom_str = f"[{'green' if momentum_ok else 'dim'}]{mom:+.1f} bps[/]"
                skew_str = f"[{'green' if micro_skew >= 0.8 else 'dim'}]{micro_skew:+.2f} bps[/]"
                
                conf_color = "bold green" if conf >= c.min_confluence else "yellow" if conf >= 15 else "dim"
                conf_bar = f"[{conf_color}]{conf:.1f}/100[/]"

                cost = 2 * (c.fee_bps + c.slippage_bps) + spread
                net_edge = c.tp_bps - cost
                sig_obi = (imb >= c.imbalance_entry and micro_skew >= 0.8 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
                sig_conf = (conf >= c.min_confluence and imb >= 0.62 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
                sig_tape = (cvd >= 0.72 and imb >= 0.60 and micro_skew >= 0.8 and mom >= c.min_mom_bps)
                ready = momentum_ok and (sig_obi or sig_conf or sig_tape) and (net_edge >= c.min_net_edge_bps)
                pathways = "+".join([
                    label for label, passed in (
                        ("OBI", sig_obi),
                        ("CONF", sig_conf),
                        ("TAPE", sig_tape),
                    ) if passed
                ]) or "—"
                edge_str = f"[{'green' if net_edge >= c.min_net_edge_bps else 'red'}]{net_edge:+.1f} bps[/]"

                status = "[dim]Monitoring[/]"
                if sym in b.pos:
                    status = "[bold magenta]IN TRADE[/]"
                elif sym in pending:
                    status = "[bold yellow]RESTING MAKER[/]"
                elif b.cool.get(sym, 0) > now:
                    status = f"[yellow]Cooldown ({int(b.cool[sym] - now)}s)[/]"
                elif ready and not macro_safe:
                    status = "[red]BTC REGIME BLOCK[/]"
                elif ready and pause_remaining > 0:
                    status = f"[yellow]REDEPLOY {pause_remaining:.1f}s[/]"
                elif ready:
                    status = "[bold green]SIGNAL READY[/]"

                watch_table.add_row(
                    sym,
                    f"{mid:.5g}",
                    f"{spread:.1f} bps",
                    imb_str,
                    cvd_str,
                    skew_str,
                    mom_str,
                    conf_bar,
                    edge_str,
                    pathways,
                    status,
                )
            else:
                watch_table.add_row(sym, "-", "-", "-", "-", "-", "-", "-", "-", "-", "[dim]Awaiting Feed[/]")

        layout["watchlist"].update(Panel(watch_table, title="Live Alpha Confluence Radar — Exact Engine Gates", style="green"))

        # Realized ledger — reads the same CSV written by Bot.close().
        trade_table = Table(expand=True, box=None)
        trade_table.add_column("Time")
        trade_table.add_column("Symbol", style="bold")
        trade_table.add_column("Entry", justify="right")
        trade_table.add_column("Exit", justify="right")
        trade_table.add_column("P&L", justify="right")
        trade_table.add_column("Reason")
        trade_table.add_column("Equity", justify="right")
        trade_table.add_column("Tier", justify="right")
        if recent_trades:
            for row in reversed(recent_trades):
                try:
                    trade_ts = time.strftime("%H:%M:%S", time.localtime(float(row.get("ts", 0) or 0)))
                    trade_pnl = float(row.get("pnl_quote", 0) or 0)
                    trade_color = "green" if trade_pnl >= 0 else "red"
                    trade_table.add_row(
                        trade_ts,
                        row.get("symbol", ""),
                        f"{float(row.get('entry', 0) or 0):.6g}",
                        f"{float(row.get('exit', 0) or 0):.6g}",
                        f"[{trade_color}]{trade_pnl:+.4f}[/]",
                        row.get("reason", ""),
                        f"{float(row.get('equity', 0) or 0):.2f}",
                        row.get("tier", ""),
                    )
                except (TypeError, ValueError):
                    continue
        else:
            trade_table.add_row("[dim]No completed trades in this session log[/]", "", "", "", "", "", "", "")
        layout["trades"].update(
            Panel(trade_table, title="Realized Trade Ledger — Bot.close() Source", style="blue")
        )

        # Footer
        footer_text = Text.from_markup(
            f" [dim]TP {c.tp_bps:.0f}bps | SL {c.sl_bps:.0f}bps | BE {c.breakeven_bps:.0f}bps | "
            f"Trail trigger/cushion {c.trail_trigger_bps:.0f}/{c.trail_bps:.0f}bps | Conf ≥{c.min_confluence:.0f} | "
            f"OBI ≥{c.imbalance_entry:.2f} | CVD ≥{c.min_cvd:.2f} | µSkew ≥0.8bps | "
            f"Mom {c.min_mom_bps:.1f}..{getattr(c, 'max_mom_bps', 25.0):.1f}bps | Daily guard {c.daily_loss_limit_frac*100:.0f}% | "
            f"Loop {c.loop_s:.1f}s | Ctrl+C Stop[/]"
        )
        layout["footer"].update(Panel(footer_text, style="dim"))

        # Clear screen & print
        self.console.clear()
        self.console.print(layout)
