# EFRA BOT v2.0 - Microstructure Scalping & Hyper-Growth Compounding Engine

An institutional-grade spot microstructure trading bot built with `ccxt` and `ccxt.pro`, engineered for ultra-low latency execution, spoofing-resilient alpha confluence, and exponential capital compounding.

---

## 🚀 Quick Start

### 1. Instant System Diagnostic
Test exchange connectivity, WebSocket latency, trading fee tier, and order limits in 5 seconds:
```bash
python efra_bot.py --exchange kraken --diagnostic
```

### 2. Paper Trading with Real-Time HUD Dashboard (Recommended)
Simulate trades against real live order books with full fee & slippage deductions:
```bash
python efra_bot.py --exchange kraken --paper --dashboard
```

### 3. One-Click Launcher (Windows)
Double-click `start_bot.bat` or run:
```cmd
start_bot.bat
```

### 4. Real-Time Performance & Edge Analytics
Analyze your trade log, win rate, profit factor, payoff ratio, and compounding velocity:
```bash
python efra_report.py
```

### 5. Live Trading (Real Capital)
When you are satisfied with paper results:
```bash
# Set your API credentials
export EFRA_API_KEY="your_api_key"
export EFRA_API_SECRET="your_api_secret"

# Run in live trading mode
python efra_bot.py --exchange kraken --live --dashboard
```

---

## 🧠 Core Architecture & Profitability Upgrades

### 1. Sub-50ms WebSocket Live Data (`efra_stream.py`)
- Replaces legacy 1.5s–3.0s REST polling with asynchronous `ccxt.pro` WebSockets.
- Continuously streams top-of-book L2 depth and public trade tape.
- Loop step execution time reduced from **2,000 ms to 0.04 ms** (40 microseconds).

### 2. The "Sweet Spot" Alpha Confluence Model
Filters out fake spoofing walls by requiring multi-factor confluence before any entry:
1. **Order Book Imbalance (OBI)**: Bid depth dominates asks ($\ge 68\%$).
2. **Cumulative Volume Delta (CVD)**: Aggressive market buy volume on the trade tape exceeds sells ($\ge 52\%$).
3. **Micro-Momentum**: Short-term mid-price velocity ($\ge 2.0\text{ bps}$).
4. **Spread Quality**: Ensures spread is tight ($\le 6\text{ bps}$) and not widening.
5. **Cost Clearance**: Anticipated edge clears round-trip maker/taker fees + slippage by at least 5.0 bps.

### 3. Dynamic 3-Stage Profit Lock & Trailing Exits
The current Ultra-Precision v2 runtime uses asymmetric protection tuned to let strong winners run:
- **Stage 1 (Breakeven Ratchet)**: once return reaches **+80 bps** (or the actual round-trip cost plus the configured profit buffer, whichever is higher), the stop ratchets above costs.
- **Stage 2 (Dynamic Trailing Stop)**: once return reaches **+100 bps**, trailing protection follows peak return with a **30 bps** cushion, allowing continuation toward the **+200 bps TP** and beyond when the trail is active.
- **Stage 3 (Hard Stop & Safety)**: initial stop is **-40 bps**, max hold is **420 seconds**, structural breakdown exits are stricter, and the daily loss halt is **8%**.

### 4. Hyper-Growth Compounding Engine
Built specifically for rapid, disciplined capital scaling:
- **Auto-Capital Re-Indexing**: every **+5%** increase from the current compounding tier promotes the account to the next tier.
- **Dynamic Multi-Slot Allocation**:
  - Capital **< $300**: 2 concurrent sniper slots at the configured position fraction (currently 40% each).
  - Capital **$300 - $1,000**: 3 slots at 30% each.
  - Capital **$1,000 - $3,000**: 4 slots at 22% each.
  - Capital **> $3,000**: 5 slots at 18% each.
- **Anti-Martingale Defensive Shield**: after **2 consecutive losses**, allocation is reduced by **30%** until a winning close resets the streak.

---

## 🛠️ Command-Line Options Reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--exchange` | `binance` | Exchange ID (e.g. `kraken`, `coinbase`, `bybit`, `okx`, `binance`) |
| `--quote` | `USDT` | Base quote asset (`USDT`, `USD`, `USDC`, `EUR`) |
| `--paper` | `True` | Run paper trading against live order books |
| `--live` | `False` | Enable real order placement on exchange |
| `--dashboard` | `False` | Display the rich terminal HUD |
| `--diagnostic` | `False` | Run instant 5-second environment diagnostic |
| `--reset` | `False` | Reset saved session state (`efra_state.json`) |
| `--start-balance` | `100.0` | Initial virtual cash balance for paper trading |
| `--compound-step` | `0.10` | Percentage equity gain to advance compounding tier (10%) |
| `--no-ws` | `False` | Disable WebSocket and use REST polling |
| `--mode` | `taker` | Execution mode: `taker` (IOC market) or `maker` (post-only) |
| `--tp-bps` | `200.0` | Target take-profit in basis points (+2.00%) |
| `--sl-bps` | `40.0` | Hard stop-loss in basis points (-0.40%) |
| `--breakeven-bps` | `80.0` | Profit threshold to ratchet stop above round-trip costs |
| `--trail-trigger-bps` | `100.0` | Profit threshold that activates the trailing stop |
| `--trail-bps` | `30.0` | Trailing stop cushion distance from peak return |
| `--compound-step` | `0.05` | Equity growth required to advance the compounding tier (+5%) |
| `--daily-loss-limit-frac` | `0.08` | Daily equity drawdown that halts and flattens the engine |
