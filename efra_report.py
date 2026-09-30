#!/usr/bin/env python3
"""
EFRA PERFORMANCE ANALYTICS & REPORTING
Reads efra_trades.csv to evaluate statistical edge, compounding efficiency,
and risk-adjusted returns.

Usage:
    python efra_report.py [efra_trades.csv]
"""

import csv
import math
import statistics
import sys
import glob
import os
from collections import Counter

if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    if os.path.exists("efra_trades.csv"):
        path = "efra_trades.csv"
    else:
        candidates = sorted(glob.glob("efra_trades*.csv"), key=os.path.getmtime, reverse=True)
        path = candidates[0] if candidates else "efra_trades.csv"

try:
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
except FileNotFoundError:
    sys.exit(f"No trade log at '{path}' - has the bot executed any trades yet?")

if not rows:
    sys.exit(f"Log at '{path}' is empty: no trades have been completed yet.")

pnl = [float(r["pnl_quote"]) for r in rows]
eq = [float(r["equity"]) for r in rows]
wins = [p for p in pnl if p > 0]
losses = [p for p in pnl if p <= 0]
n = len(pnl)
start_eq = eq[0] - pnl[0] if eq else 100.0
end_eq = eq[-1] if eq else start_eq

# Robust drawdown calculation from cumulative equity path
cum_curve = [start_eq]
for p in pnl:
    cum_curve.append(cum_curve[-1] + p)
peak = cum_curve[0]
mdd = 0.0
for c_eq in cum_curve:
    peak = max(peak, c_eq)
    if peak > 0:
        mdd = max(mdd, (peak - c_eq) / peak)

# Win rate & Profit factor
win_rate = len(wins) / n * 100.0 if n > 0 else 0.0
gross_profit = sum(wins)
gross_loss = abs(sum(losses))
pf = (gross_profit / gross_loss) if gross_loss > 0 else (99.9 if gross_profit > 0 else 0.0)

avg_win = statistics.mean(wins) if wins else 0.0
avg_loss = statistics.mean(losses) if losses else 0.0
payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else 0.0

# Expected return per trade
ev = sum(pnl) / n if n > 0 else 0.0

# Returns standard deviation for Sharpe
trade_rets = [(p / start_eq) for p in pnl]
std_ret = statistics.stdev(trade_rets) if len(trade_rets) > 1 else 0.0
sharpe = (statistics.mean(trade_rets) / std_ret * math.sqrt(365 * 24)) if std_ret > 0 else 0.0

# Compounding tiers
tiers = [int(r.get("tier", 1)) for r in rows if r.get("tier")]
max_tier = max(tiers) if tiers else 1

print("\n" + "=" * 65)
print("             EFRA TRADING PERFORMANCE & ALPHA REPORT")
print("=" * 65)
print(f" Total Trades Executed : {n}  ({len(wins)} Wins / {len(losses)} Losses)")
print(f" Win Rate              : {win_rate:.1f}%")
print(f" Total Net P&L         : {sum(pnl):+.4f} (Starting: ${start_eq:.2f} -> Current: ${end_eq:.2f})")
print(f" Net Account Return    : {100.0 * (end_eq / start_eq - 1.0):+.2f}%")
print(f" Compounding Tier      : Tier {max_tier} reached")
print("-" * 65)
print(f" Profit Factor         : {pf:.2f}  (>1.3 = strong edge; >2.0 = institutional)")
print(f" Expected Value / Trade: {ev:+.5f} quote units (post-fees & slippage)")
print(f" Average Win           : {avg_win:+.5f}")
print(f" Average Loss          : {avg_loss:+.5f}")
print(f" Win/Loss Payoff Ratio : {payoff:.2f}x")
print(f" Maximum Drawdown      : {100.0 * mdd:.2f}%")
print(f" Estimated Sharpe Ratio: {sharpe:.2f}")
print("-" * 65)
print(" Exit Reason Breakdown :")
reasons = Counter(r.get("reason", "unknown") for r in rows)
for reason, count in reasons.most_common():
    pct = count / n * 100.0
    print(f"   - {reason.upper():<16}: {count:>3} trades ({pct:>5.1f}%)")
print("=" * 65)

# Actionable Verdict
if n < 30:
    print(f" VERDICT: Sample size is small ({n} trades). Continue paper trading to 50+.")
elif ev > 0 and pf > 1.25 and win_rate >= 50.0:
    print(" VERDICT: HIGH STATISTICAL EDGE! The confluence model consistently")
    print("          beats spread + fees. Strategy is ready for scaled compounding.")
elif ev > 0:
    print(" VERDICT: POSITIVE EDGE. The strategy is net profitable. Optimize")
    print("          trailing stop triggers to further increase payoff ratio.")
else:
    print(" VERDICT: DEFENSIVE ADJUSTMENT NEEDED. Tighten confluence entry score.")
print("=" * 65 + "\n")
