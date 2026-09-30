#!/usr/bin/env python3
"""
EFRA RESEARCH - find out which signals actually predict price on YOUR exchange
at YOUR fee tier, before trading anything.

  1) RECORD live order-book snapshots (no API key needed, run it on the VPS):
       python efra_research.py record --exchange bybit --hours 48

  2) ANALYZE what the data says (can run while recording):
       python efra_research.py analyze --fee-bps 10 --maker-fee-bps 10

What analyze does
  For every snapshot it computes signals (top-of-book imbalance, 5-level
  imbalance, 5s/15s momentum, the current Efra entry rule) and measures the
  forward price move 5s / 15s / 60s later, NET of what a taker trade would
  really have cost (buy at ask, sell at bid, fees both ways).

Built-in guards against fooling yourself
  * Overlap thinning: observations are kept >= horizon seconds apart, so
    1-second snapshots do not masquerade as thousands of independent trades.
  * Out-of-sample: the data is split in time. A signal is only flagged if it
    is net-positive with t >= 2.5 in BOTH halves.
  * Many buckets are tested, so a few will look good by pure luck. Treat any
    flag as a HYPOTHESIS to confirm on fresh data, not as proof.
"""
import argparse
import bisect
import logging
import math
import sqlite3
import statistics
import sys
import time
from collections import defaultdict

HORIZONS = (5, 15, 60)


# --------------------------------------------------------------------------
# RECORD
# --------------------------------------------------------------------------
def pick_symbols(ex, quote, n, min_vol, max_spread_bps):
    rows = []
    for sym, t in ex.fetch_tickers().items():
        m = ex.markets.get(sym)
        if not m or not m.get("spot") or not m.get("active", True) or m.get("quote") != quote:
            continue
        bid, ask, last = t.get("bid"), t.get("ask"), t.get("last")
        hi, lo, qv = t.get("high"), t.get("low"), t.get("quoteVolume")
        if not all([bid, ask, last, hi, lo, qv]) or qv < min_vol:
            continue
        spread = (ask - bid) / ((ask + bid) / 2) * 1e4
        if spread > max_spread_bps:
            continue
        rows.append(((hi - lo) / last * 1e4 / (spread + 1.0), sym))
    rows.sort(reverse=True)
    return [s for _, s in rows[:n]]


def record(a):
    try:
        import ccxt
    except ImportError:
        sys.exit("ccxt is not installed. Run:  pip install ccxt")
    if not hasattr(ccxt, a.exchange):
        sys.exit(f"Unknown exchange '{a.exchange}'")
    ex = getattr(ccxt, a.exchange)({"enableRateLimit": True, "timeout": 20000,
                                    "options": {"defaultType": "spot"}})
    ex.load_markets()
    syms = [s.strip() for s in a.symbols.split(",")] if a.symbols else \
        pick_symbols(ex, a.quote, a.pairs, a.min_quote_volume, a.max_spread_bps)
    if not syms:
        sys.exit("No pairs matched the filters. Try --quote USDC or lower --min-quote-volume.")
    logging.info("recording %d pairs: %s", len(syms), ", ".join(syms))

    db = sqlite3.connect(a.db)
    db.execute("""CREATE TABLE IF NOT EXISTS snap(
        ts REAL, sym TEXT, bid REAL, ask REAL, bq1 REAL, aq1 REAL, bv5 REAL, av5 REAL)""")
    db.execute("CREATE INDEX IF NOT EXISTS ix ON snap(sym, ts)")
    end = time.time() + a.hours * 3600
    n = errs = 0
    last_beat = 0.0
    try:
        while time.time() < end:
            t0 = time.time()
            for sym in syms:
                try:
                    ob = ex.fetch_order_book(sym, limit=10)
                    b, k = ob["bids"], ob["asks"]
                    if not b or not k:
                        continue
                    db.execute("INSERT INTO snap VALUES(?,?,?,?,?,?,?,?)", (
                        time.time(), sym, b[0][0], k[0][0],
                        b[0][0] * b[0][1], k[0][0] * k[0][1],
                        sum(x[0] * x[1] for x in b[:5]), sum(x[0] * x[1] for x in k[:5])))
                    n += 1
                except Exception as e:
                    errs += 1
                    logging.debug("%s: %s", sym, e)
                    if errs and errs % 50 == 0:
                        logging.warning("%d fetch errors so far (last: %s)", errs, e)
            db.commit()
            if time.time() - last_beat > 300:
                last_beat = time.time()
                logging.info("recorded %d snapshots, %d errors, %.1fh left",
                             n, errs, (end - time.time()) / 3600)
            time.sleep(max(0.0, a.interval - (time.time() - t0)))
    except KeyboardInterrupt:
        logging.info("stopped")
    finally:
        db.commit()
        db.close()
    logging.info("done: %d snapshots in %s. Now run:  python efra_research.py analyze", n, a.db)


# --------------------------------------------------------------------------
# ANALYZE
# --------------------------------------------------------------------------
def load(db_path):
    db = sqlite3.connect(db_path)
    data = defaultdict(list)
    for r in db.execute("SELECT ts,sym,bid,ask,bq1,aq1,bv5,av5 FROM snap ORDER BY sym, ts"):
        data[r[1]].append(r)
    db.close()
    return data


def bucketize(name, x, edges, labels):
    i = bisect.bisect_right(edges, x)
    return f"{name}:{labels[i]}"


IMB_E, IMB_L = [0.25, 0.4, 0.6, 0.75], ["<0.25", "0.25-0.4", "0.4-0.6", "0.6-0.75", ">=0.75"]
MOM_E, MOM_L = [-5, -1, 1, 5], ["<-5bps", "-5..-1", "-1..1", "1..5", ">5bps"]


def signals(imb1, imb5, mom5, mom15):
    out = [bucketize("imb1", imb1, IMB_E, IMB_L),
           bucketize("imb5", imb5, IMB_E, IMB_L),
           bucketize("mom5", mom5, MOM_E, MOM_L),
           bucketize("mom15", mom15, MOM_E, MOM_L)]
    if imb5 >= 0.68 and mom5 >= 2:
        out.append("EFRA_RULE: imb5>=0.68 & mom5>=2bps")
    if imb5 >= 0.68 and mom5 < -2:
        out.append("dip-buy: imb5>=0.68 & mom5<-2bps")
    if imb5 <= 0.32 and mom5 >= 2:
        out.append("fade: imb5<=0.32 & mom5>=2bps")
    return out


def tstat(xs):
    n = len(xs)
    if n < 3:
        return 0.0
    sd = statistics.pstdev(xs)
    return statistics.fmean(xs) / (sd / math.sqrt(n)) if sd > 0 else 0.0


def analyze(a):
    data = load(a.db)
    if not data:
        sys.exit(f"No data in {a.db}. Run the recorder first.")
    t_min = min(v[0][0] for v in data.values())
    t_max = max(v[-1][0] for v in data.values())
    split = (t_min + t_max) / 2
    hours = (t_max - t_min) / 3600
    total = sum(len(v) for v in data.values())
    print(f"\nData: {total:,} snapshots, {len(data)} pairs, {hours:.1f} hours "
          f"({a.db})")
    if hours < 6:
        print("WARNING: under 6 hours of data. Results below are anecdotes, not evidence.")

    fee, mfee = a.fee_bps, a.maker_fee_bps
    # samples[(signal, h, half)] -> lists of taker-net and mid returns
    taker = defaultdict(list)
    mid_r = defaultdict(list)
    spreads = []

    for sym, rows in data.items():
        ts = [r[0] for r in rows]
        mids = [(r[2] + r[3]) / 2 for r in rows]
        last_taken = {}
        for i, r in enumerate(rows):
            t, bid, ask, bq1, aq1, bv5, av5 = r[0], r[2], r[3], r[4], r[5], r[6], r[7]
            if not (bq1 + aq1) or not (bv5 + av5) or bid <= 0:
                continue
            j5, j15 = bisect.bisect_left(ts, t - 5), bisect.bisect_left(ts, t - 15)
            if t - ts[j5] > 8 or t - ts[j15] > 25 or ts[j5] == t:
                continue
            mom5 = (mids[i] / mids[j5] - 1) * 1e4
            mom15 = (mids[i] / mids[j15] - 1) * 1e4
            sigs = signals(bq1 / (bq1 + aq1), bv5 / (bv5 + av5), mom5, mom15)
            spreads.append((ask - bid) / mids[i] * 1e4)
            half = 0 if t < split else 1
            for h in HORIZONS:
                k = bisect.bisect_left(ts, t + h)
                if k >= len(ts) or ts[k] - (t + h) > max(3.0, h * 0.4):
                    continue
                tk = ((rows[k][2] / ask) - 1) * 1e4 - 2 * fee        # buy ask, sell bid, 2 fees
                mr = (mids[k] / mids[i] - 1) * 1e4
                for s in sigs:
                    key = (s, h, sym)
                    if t < last_taken.get(key, 0):                   # thin overlapping samples
                        continue
                    last_taken[key] = t + h
                    taker[(s, h, half)].append(tk)
                    mid_r[(s, h, half)].append(mr)

    avg_spread = statistics.fmean(spreads) if spreads else 0
    print(f"Average spread {avg_spread:.1f}bps | taker fee {fee}bps/side -> a taker round trip "
          f"costs ~{2*fee + avg_spread:.1f}bps before ANY edge")
    print(f"Maker round trip fees {2*mfee:.1f}bps (only if your resting orders actually fill)\n")

    results = []
    for (s, h, half), xs in taker.items():
        if half != 1:
            continue
        a_xs = taker.get((s, h, 0), [])
        b_xs = xs
        am, bm = mid_r.get((s, h, 0), []), mid_r.get((s, h, 1), [])
        if len(a_xs) < 30 or len(b_xs) < 30:
            continue
        results.append({
            "sig": s, "h": h, "n": (len(a_xs), len(b_xs)),
            "tk": (statistics.fmean(a_xs), statistics.fmean(b_xs)),
            "t": (tstat(a_xs), tstat(b_xs)),
            "mid": (statistics.fmean(am), statistics.fmean(bm)),
            "mt": (tstat(am), tstat(bm)),
        })

    def is_taker_edge(r):
        return all(m > 0 for m in r["tk"]) and all(t >= 2.5 for t in r["t"])

    def is_maker_potential(r):
        return (all(m - 2 * mfee > 0 for m in r["mid"]) and all(t >= 2.5 for t in r["mt"]))

    edges = [r for r in results if is_taker_edge(r)]
    pots = [r for r in results if is_maker_potential(r) and not is_taker_edge(r)]

    def show(r):
        return (f"  {r['sig']:<38} h={r['h']:>2}s  n={r['n'][0]}/{r['n'][1]}  "
                f"taker-net {r['tk'][0]:+6.1f}/{r['tk'][1]:+6.1f}bps  "
                f"mid {r['mid'][0]:+5.1f}/{r['mid'][1]:+5.1f}bps  "
                f"t {r['t'][0]:+.1f}/{r['t'][1]:+.1f}")

    print("=== Signals that beat TAKER costs in BOTH halves of the data ===")
    if edges:
        for r in sorted(edges, key=lambda r: -min(r["tk"])):
            print(show(r))
        print("\n  These are candidates only. Confirm on a fresh recording before trusting them.")
    else:
        print("  NONE. No signal tested clears taker fees + spread out of sample.")

    print("\n=== Signals with a move big enough for MAKER fees (needs fills to be realistic) ===")
    if pots:
        for r in sorted(pots, key=lambda r: -min(m - 2 * mfee for m in r["mid"]))[:10]:
            print(show(r))
        print("\n  'mid' is the raw mid-price move. It ignores whether your resting order fills,")
        print("  and you tend to be filled when price is moving AGAINST you. Paper-trade with")
        print("  --mode maker to see the real fill behaviour before believing this.")
    else:
        print("  NONE. Even before fill risk, nothing beats maker round-trip fees.")

    print("\n=== The current Efra entry rule, for reference ===")
    rule = [r for r in results if r["sig"].startswith("EFRA_RULE")]
    for r in sorted(rule, key=lambda r: r["h"]):
        print(show(r))
    if not rule:
        print("  (rule never fired often enough to measure - need more data)")

    print("\n=== Biggest raw mid-price predictors, ignoring costs (what is even predictable) ===")
    best = sorted(results, key=lambda r: -min(abs(r["mid"][0]), abs(r["mid"][1]))
                  if (r["mid"][0] > 0) == (r["mid"][1] > 0) else 0)[:6]
    for r in best:
        print(show(r))
    print(f"\nTested {len(results)} signal/horizon combinations. At t>=2.5 in both halves "
          f"a few false positives are still possible.\n")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("record")
    r.add_argument("--exchange", default="bybit")
    r.add_argument("--quote", default="USDT")
    r.add_argument("--pairs", type=int, default=8)
    r.add_argument("--symbols", default="", help="comma list, overrides auto-pick")
    r.add_argument("--hours", type=float, default=24)
    r.add_argument("--interval", type=float, default=1.0)
    r.add_argument("--min-quote-volume", type=float, default=2_000_000)
    r.add_argument("--max-spread-bps", type=float, default=8.0)
    r.add_argument("--db", default="efra_research.db")

    z = sub.add_parser("analyze")
    z.add_argument("--db", default="efra_research.db")
    z.add_argument("--fee-bps", type=float, default=10.0, help="taker fee per side")
    z.add_argument("--maker-fee-bps", type=float, default=10.0)

    a = ap.parse_args()
    record(a) if a.cmd == "record" else analyze(a)


if __name__ == "__main__":
    main()
