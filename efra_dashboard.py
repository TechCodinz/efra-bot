#!/usr/bin/env python3
"""
EFRA DASHBOARD - Real-time terminal HUD for monitoring Efra Bot.
Uses 'rich' for a high-performance visual display of telemetry,
order books, signal confluence, active positions, and compounding progress.
"""

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

        # Tier & Compounding calculations
        tier = getattr(b, "compound_tier", 1)
        base_tier_eq = getattr(b, "last_tier_equity", start_eq)
        next_tier_eq = base_tier_eq * (1.0 + c.compound_step)
        tier_prog = max(0.0, min(100.0, (eq - base_tier_eq) / (next_tier_eq - base_tier_eq) * 100.0)) if (next_tier_eq > base_tier_eq) else 0.0

        # Create Layout
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="metrics", size=4),
            Layout(name="positions", size=6),
            Layout(name="watchlist", size=10),
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
        metrics_table.add_column(ratio=1)
        metrics_table.add_column(ratio=1)
        metrics_table.add_column(ratio=1)
        metrics_table.add_column(ratio=1)
        metrics_table.add_column(ratio=1)

        metrics_table.add_row(
            f" [bold]Equity:[/] [bold {pnl_color}]${eq:.2f}[/]",
            f" [bold]P&L:[/] [{pnl_color}]{pnl_tot:+.2f} ({pnl_pct:+.2f}%)[/]",
            f" [bold]Trades:[/] {b.n_trades} ([green]{b.n_wins}W[/]/[red]{b.n_trades - b.n_wins}L[/])",
            f" [bold]Win Rate:[/] [{pnl_color}]{win_rate:.1f}%[/]",
            f" [bold]Compounding:[/] Tier {tier} [cyan]({tier_prog:.0f}% to next +{c.compound_step*100:.0f}%)[/]",
        )
        layout["metrics"].update(Panel(metrics_table, title="Account & Performance Telemetry", style="cyan"))

        # Active Positions Table
        pos_table = Table(expand=True, box=None)
        pos_table.add_column("Symbol", style="bold")
        pos_table.add_column("Entry Px", justify="right")
        pos_table.add_column("Current Px", justify="right")
        pos_table.add_column("Hold", justify="right")
        pos_table.add_column("P&L (bps / $)", justify="right")
        pos_table.add_column("Trail Stop / Lock", justify="center")
        pos_table.add_column("Target TP", justify="right")

        if b.pos:
            for sym, p in b.pos.items():
                bk = (b.last_books.get(sym) if hasattr(b, "last_books") else None) or {}
                mid = bk.get("mid", p["entry"])
                ret_bps = (mid - p["entry"]) / p["entry"] * 1e4
                pnl_dollar = (mid - p["entry"]) * p["qty"]
                hold_s = int(now - p["ts"])
                color = "green" if ret_bps >= 0 else "red"

                stop_info = f"[yellow]{p.get('stop_bps', -c.sl_bps):+.1f} bps[/]"
                if p.get("be_locked"):
                    stop_info = "[bold green]BE LOCKED (+cost)[/]"
                elif p.get("trailing_active"):
                    stop_info = f"[bold cyan]TRAILING ({p.get('trail_stop_bps', 0):+.1f} bps)[/]"

                pos_table.add_row(
                    sym,
                    f"{p['entry']:.5g}",
                    f"{mid:.5g}",
                    f"{hold_s}s",
                    f"[{color}]{ret_bps:+.1f} bps ({pnl_dollar:+.3f})[/]",
                    stop_info,
                    f"{p['entry'] * (1 + c.tp_bps / 1e4):.5g} (+{c.tp_bps:.0f} bps)",
                )
        else:
            pos_table.add_row("[dim]No active positions (Scanning for high-confluence setups...)[/]", "", "", "", "", "", "")

        layout["positions"].update(Panel(pos_table, title="Active Positions (Micro-Scalp)", style="magenta"))

        # Watchlist & Signal Confluence Table
        watch_table = Table(expand=True, box=None)
        watch_table.add_column("Pair", style="bold")
        watch_table.add_column("Mid Price", justify="right")
        watch_table.add_column("Spread", justify="right")
        watch_table.add_column("OBI (Depth)", justify="right")
        watch_table.add_column("CVD (Tape)", justify="right")
        watch_table.add_column("Mom", justify="right")
        watch_table.add_column("Confluence", justify="center")
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

                # Color coding
                imb_str = f"[green]{imb*100:.1f}%[/]" if imb >= c.imbalance_entry else f"[dim]{imb*100:.1f}%[/]"
                cvd_str = f"[green]{cvd*100:.1f}%[/]" if cvd >= c.min_cvd else f"[dim]{cvd*100:.1f}%[/]"
                mom_str = f"[{'green' if mom >= c.min_mom_bps else 'dim'}]{mom:+.1f} bps[/]"
                
                conf_color = "bold green" if conf >= c.min_confluence else "yellow" if conf >= 15 else "dim"
                conf_bar = f"[{conf_color}]{conf:.1f}/100[/]"

                cost = 2 * (c.fee_bps + c.slippage_bps) + spread
                net_edge = c.tp_bps - cost
                momentum_ok = c.min_mom_bps <= mom <= getattr(c, "max_mom_bps", 25.0)
                sig_obi = (imb >= c.imbalance_entry and micro_skew >= 0.8 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
                sig_conf = (conf >= c.min_confluence and imb >= 0.62 and cvd >= c.min_cvd and mom >= c.min_mom_bps)
                sig_tape = (cvd >= 0.72 and imb >= 0.60 and micro_skew >= 0.8 and mom >= c.min_mom_bps)
                ready = momentum_ok and (sig_obi or sig_conf or sig_tape) and (net_edge >= c.min_net_edge_bps)

                status = "[dim]Monitoring[/]"
                if sym in b.pos:
                    status = "[bold magenta]IN TRADE[/]"
                elif b.cool.get(sym, 0) > now:
                    status = f"[yellow]Cooldown ({int(b.cool[sym] - now)}s)[/]"
                elif ready:
                    status = "[bold green]SIGNAL READY[/]"

                watch_table.add_row(
                    sym,
                    f"{mid:.5g}",
                    f"{spread:.1f} bps",
                    imb_str,
                    cvd_str,
                    mom_str,
                    conf_bar,
                    status,
                )
            else:
                watch_table.add_row(sym, "-", "-", "-", "-", "-", "-", "[dim]Awaiting Feed[/]")

        layout["watchlist"].update(Panel(watch_table, title="Real-Time Alpha Confluence Radar", style="green"))

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
