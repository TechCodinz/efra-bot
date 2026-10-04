#!/usr/bin/env python3
"""
EFRA EDGE AUDIT -- forward-return analysis against live research DB.

Run on VPS:
    python efra_edge_audit.py \
        --db /opt/app-platform/state/efra-research/efra_research.db \
        --fee-bps 20 \
        --slippage-bps 1 \
        --out /opt/app-platform/state/efra-research/edge_audit.txt

Schema:  snap(ts REAL, sym TEXT, bid REAL, ask REAL, bq1 REAL, aq1 REAL, bv5 REAL, av5 REAL)
"""

import argparse
import bisect
import math
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

HORIZONS         = (5, 15, 30, 60, 120, 180)
MIN_SAMPLES      = 40
TSTAT_THRESHOLD  = 2.0
FEE_BPS_DEFAULT  = 20.0
SLIP_BPS_DEFAULT = 1.0

# Current EFRA thresholds (ep-20261004)
EFRA_OBI_MIN     = 0.72
EFRA_MOM_MIN     = 3.5
EFRA_MOM_MAX     = 18.0
EFRA_MICRO_MIN   = 1.0
EFRA_CONF_MIN    = 62.0

# Challenger policy
CHALL_OBI_MIN    = 0.75
CHALL_MOM_MIN    = 5.0
CHALL_SPREAD_MAX = 4.0


def tstat(xs):
    n = len(xs)
    if n < 3:
        return 0.0
    sd = statistics.pstdev(xs)
    return statistics.fmean(xs) / (sd / math.sqrt(n)) if sd > 0 else 0.0


def pct_ci(xs, z=1.645):
    n = len(xs)
    if n < 4:
        return (None, None)
    sd = statistics.pstdev(xs)
    m = statistics.fmean(xs)
    mg = z * sd / math.sqrt(n)
    return (m - mg, m + mg)


def med(xs):
    return statistics.median(xs) if xs else 0.0


def load_data(db_path):
    db = sqlite3.connect(db_path)
    data = defaultdict(list)
    cnt = 0
    for row in db.execute(
        "SELECT ts,sym,bid,ask,bq1,aq1,bv5,av5 FROM snap ORDER BY sym,ts"
    ):
        data[row[1]].append(row)
        cnt += 1
    db.close()
    return data, cnt


def rtc(spread, fee, slip):
    return spread + 2 * fee + 2 * slip


def imb_bucket(v):
    if v < 0.40: return "imb:<0.40"
    if v < 0.55: return "imb:0.40-0.55"
    if v < 0.65: return "imb:0.55-0.65"
    if v < 0.72: return "imb:0.65-0.72"
    if v < 0.80: return "imb:0.72-0.80"
    return "imb:>=0.80"


def sp_bucket(v):
    if v < 2.0: return "sp:<2"
    if v < 4.0: return "sp:2-4"
    if v < 8.0: return "sp:4-8"
    return "sp:>=8"


def mom_bucket(v):
    if v < -5:  return "mom:<-5"
    if v < -2:  return "mom:-5..-2"
    if v < 0:   return "mom:-2..0"
    if v < 2:   return "mom:0..2"
    if v < 5:   return "mom:2..5"
    if v < 10:  return "mom:5..10"
    return "mom:>=10"


def sess_bucket(ts):
    h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
    if h < 6:   return "sess:Asia"
    if h < 12:  return "sess:EU"
    if h < 16:  return "sess:US-open"
    if h < 20:  return "sess:US-PM"
    return "sess:US-close"


def analyze(args, out):
    fee  = args.fee_bps
    slip = args.slippage_bps
    mfee = args.maker_fee_bps

    data, total = load_data(args.db)
    if not data:
        print("ERROR: no data in DB", file=out)
        return

    t_min = min(v[0][0] for v in data.values())
    t_max = max(v[-1][0] for v in data.values())
    hours = (t_max - t_min) / 3600
    s1 = t_min + (t_max - t_min) / 3
    s2 = t_min + 2 * (t_max - t_min) / 3

    def epoch(ts):
        if ts < s1: return 0
        if ts < s2: return 1
        return 2

    P = out  # shorthand

    def hdr(title):
        print(f"\n{'-'*72}", file=P)
        print(f"  {title}", file=P)
        print(f"{'-'*72}", file=P)

    print(f"\n{'='*72}", file=P)
    print("  EFRA EDGE AUDIT", file=P)
    print(f"  {datetime.now(timezone.utc).replace(tzinfo=None):%Y-%m-%d %H:%M} UTC", file=P)
    print(f"  DB: {args.db}", file=P)
    print(f"  {total:,} snapshots  |  {len(data)} pairs  |  {hours:.1f}h", file=P)
    print(f"  Split: IS<{datetime.fromtimestamp(s1,tz=timezone.utc):%H:%M}  "
          f"VAL<{datetime.fromtimestamp(s2,tz=timezone.utc):%H:%M}  OOS:rest", file=P)
    print(f"  Taker fee {fee}bps/side | Slip {slip}bps/side | Maker fee {mfee}bps/side", file=P)
    print(f"{'='*72}", file=P)

    # accumulate
    seg_net   = defaultdict(list)
    seg_gross = defaultdict(list)
    seg_mfe   = defaultdict(list)
    seg_mae   = defaultdict(list)
    spreads   = []
    last_tk   = {}

    for sym, rows in data.items():
        ts_a  = [r[0] for r in rows]
        mid_a = [(r[2]+r[3])/2 for r in rows]

        for i, r in enumerate(rows):
            t, bid, ask, bq1, aq1, bv5, av5 = r[0],r[2],r[3],r[4],r[5],r[6],r[7]
            if bid<=0 or ask<=0 or (bq1+aq1)<=0 or (bv5+av5)<=0:
                continue
            mid = (bid+ask)/2
            j5  = bisect.bisect_left(ts_a, t-5)
            j15 = bisect.bisect_left(ts_a, t-15)
            if j5<=0 or j15<=0: continue
            if t-ts_a[j5]>10 or t-ts_a[j15]>30: continue

            mom5  = (mid/mid_a[j5]-1)*1e4
            mom15 = (mid/mid_a[j15]-1)*1e4
            imb5  = bv5/(bv5+av5)
            imb1  = bq1/(bq1+aq1)
            sp    = (ask-bid)/mid*1e4
            spreads.append(sp)
            micro = (imb1-0.5)*20.0   # proxy bps
            wall  = bq1/(bq1+aq1)
            ep    = epoch(t)

            # Confluence (cvd_proxy=0.5, neutral)
            imb_s = max(0.0, min(1.0, (imb5-0.5)*3.0))
            mic_s = max(0.0, min(1.0, micro/2.0)) if micro>0 else 0.0
            mom_s = max(0.0, min(1.0, (mom5+2.0)/6.0)) if mom5>-2 else 0.0
            conf  = imb_s*40.0 + 0.0*30.0 + mic_s*20.0 + mom_s*10.0

            efra_obi  = (imb5>=EFRA_OBI_MIN and micro>=EFRA_MICRO_MIN and mom5>=EFRA_MOM_MIN)
            efra_conf = (conf>=EFRA_CONF_MIN and imb5>=(EFRA_OBI_MIN-0.02) and mom5>=EFRA_MOM_MIN)
            efra_ok   = (efra_obi or efra_conf) and mom5<=EFRA_MOM_MAX
            chall_ok  = (imb5>=CHALL_OBI_MIN and mom5>=CHALL_MOM_MIN
                         and mom5<=EFRA_MOM_MAX and micro>=EFRA_MICRO_MIN and sp<=CHALL_SPREAD_MAX)

            labels = [
                imb_bucket(imb5),
                sp_bucket(sp),
                mom_bucket(mom5),
                sess_bucket(t),
                f"sym:{sym}",
            ]
            if efra_ok:
                labels.append("EFRA_RULE")
                labels.append(f"EFRA_SYM:{sym}")
                paths = []
                if efra_obi:  paths.append("OBI")
                if efra_conf: paths.append("CONF")
                if paths:     labels.append("PATH:" + "+".join(paths))
            if chall_ok:
                labels.append("CHALLENGER")
            for thr in (0.60, 0.65, 0.68, 0.72, 0.75, 0.80):
                if imb5 >= thr:
                    labels.append(f"OBI>={thr:.2f}")
            if efra_ok and chall_ok:
                labels.append("EFRA+CHALL")
            if micro >= 1.0: labels.append("micro>=1")
            if micro >= 2.0: labels.append("micro>=2")
            if wall >= 0.70: labels.append("wall>=0.70")
            if sp <= 2.0:    labels.append("tight_sp")
            if sp > 8.0:     labels.append("wide_sp")

            cost = rtc(sp, fee, slip)
            maker_cost = 2*mfee + 2*slip

            for h in HORIZONS:
                k = bisect.bisect_left(ts_a, t+h)
                if k >= len(ts_a): continue
                if abs(ts_a[k]-(t+h)) > max(5.0, h*0.5): continue

                fm   = mid_a[k]
                gross = (fm/mid-1)*1e4
                net   = gross - cost
                mk_net = gross - maker_cost
                win_m = max((m/mid-1)*1e4 for m in mid_a[i:k+1])
                loss_m = min((m/mid-1)*1e4 for m in mid_a[i:k+1])

                for lbl in labels:
                    tk = (lbl, h, sym)
                    if t < last_tk.get(tk, 0): continue
                    last_tk[tk] = t + h
                    key = (lbl, h, ep)
                    seg_net[key].append(net)
                    seg_gross[key].append(gross)
                    seg_mfe[key].append(win_m)
                    seg_mae[key].append(loss_m)

                if efra_ok:
                    # Maker analysis must carry the same supporting metrics as taker
                    # analysis and use the same horizon thinning; otherwise get_by_epoch()
                    # crashes (missing gross/MFE/MAE) and the t-stat is inflated by
                    # heavily overlapping snapshots.
                    for mk_lbl in (f"MAKER:{sym}", "MAKER_EFRA"):
                        tk = (mk_lbl, h, sym)
                        if t < last_tk.get(tk, 0):
                            continue
                        last_tk[tk] = t + h
                        mk_key = (mk_lbl, h, ep)
                        seg_net[mk_key].append(mk_net)
                        seg_gross[mk_key].append(gross)
                        seg_mfe[mk_key].append(win_m)
                        seg_mae[mk_key].append(loss_m)

    # summary stats
    if spreads:
        avg_sp = statistics.fmean(spreads)
        med_sp = statistics.median(spreads)
        p90_sp = sorted(spreads)[int(len(spreads)*0.9)]
        hdr("COST REALITY CHECK")
        print(f"  Avg spread        : {avg_sp:.2f} bps", file=P)
        print(f"  Median spread     : {med_sp:.2f} bps", file=P)
        print(f"  90th pct spread   : {p90_sp:.2f} bps", file=P)
        print(f"  Avg round-trip    : {rtc(avg_sp,fee,slip):.2f} bps (spread + {2*fee:.0f} fees + {2*slip:.0f} slip)", file=P)
        print(f"  Maker round-trip  : {2*mfee+2*slip:.2f} bps (no spread if filled)", file=P)
        print(f"  REQUIRED gross >  : {rtc(avg_sp,fee,slip):.1f} bps to break even on taker", file=P)
        print(f"  EFRA TP target    : 250 bps -> edge buffer {250-rtc(avg_sp,fee,slip):.0f} bps", file=P)
        print(f"  EFRA SL           : 35 bps  -> RR ratio: 1:{(250-rtc(avg_sp,fee,slip))/35:.1f}", file=P)

    def row_str(lbl, h, by_ep):
        ref = by_ep.get(2) or by_ep.get(1) or by_ep.get(0)
        if not ref: return None
        ci = ref["ci"]
        ci_s = f"[{ci[0]:+.1f},{ci[1]:+.1f}]" if ci[0] is not None else "N/A"
        oos_ok = (
            by_ep.get(2)
            and by_ep[2]["mean_net"] > 0
            and by_ep[2]["t"] >= TSTAT_THRESHOLD
        )
        val_ok = (
            by_ep.get(1)
            and by_ep[1]["mean_net"] > 0
            and by_ep[1]["t"] >= TSTAT_THRESHOLD
        )
        oos_s = "[OK]" if oos_ok else ("~" if val_ok else "[NO]")
        return (f"  {lbl[:38]:<38} {h:>3}s"
                f" n={ref['n']:>5}"
                f" net={ref['mean_net']:>+7.2f}"
                f" med={ref['med_net']:>+7.2f}"
                f" gross={ref['mean_gross']:>+7.2f}"
                f" hit={ref['hit']:.0%}"
                f" MFE={ref['mfe']:>+6.1f}"
                f" MAE={ref['mae']:>+6.1f}"
                f" t={ref['t']:>+5.1f}"
                f" CI={ci_s}"
                f" OOS={oos_s}")

    def get_by_epoch(lbl, h):
        by_ep = {}
        for ep in (0, 1, 2):
            key = (lbl, h, ep)
            nets = seg_net.get(key, [])
            if len(nets) < MIN_SAMPLES:
                by_ep[ep] = None
                continue
            gross = seg_gross.get(key, [])
            mfes  = seg_mfe.get(key, [])
            maes  = seg_mae.get(key, [])
            by_ep[ep] = {
                "n": len(nets),
                "mean_net": statistics.fmean(nets),
                "med_net": med(nets),
                "mean_gross": statistics.fmean(gross),
                "med_gross": med(gross),
                "hit": sum(1 for x in nets if x>0)/len(nets),
                "mfe": med(mfes),
                "mae": med(maes),
                "t": tstat(nets),
                "ci": pct_ci(nets),
            }
        return by_ep

    def is_pos(by_ep):
        eps = [e for e,v in by_ep.items() if v]
        return eps and all(by_ep[e]["mean_net"]>0 and by_ep[e]["t"]>=TSTAT_THRESHOLD for e in eps)

    col_hdr = "  label                                h   n      net      med    gross   hit    MFE    MAE      t  CI90              OOS"

    # -- Section 1: EFRA rule all horizons -----------------------------------
    hdr("SECTION 1 -- EFRA_RULE forward expectancy at all horizons")
    print(col_hdr, file=P)
    for h in HORIZONS:
        by = get_by_epoch("EFRA_RULE", h)
        line = row_str("EFRA_RULE", h, by)
        if line:
            print(line + (" < POSITIVE" if is_pos(by) else ""), file=P)
        else:
            print(f"  EFRA_RULE {h}s -- n<{MIN_SAMPLES}", file=P)

    # -- Section 2: Path breakdown at 60s -------------------------------------
    hdr("SECTION 2 -- Signal path breakdown (60s horizon)")
    print(col_hdr, file=P)
    for lbl in ["PATH:OBI", "PATH:CONF", "PATH:OBI+CONF", "CHALLENGER", "EFRA+CHALL"]:
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 3: OBI sweep -------------------------------------------------
    hdr("SECTION 3 -- OBI threshold sweep (60s horizon)")
    print(col_hdr, file=P)
    for thr in (0.60, 0.65, 0.68, 0.72, 0.75, 0.80):
        lbl = f"OBI>={thr:.2f}"
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 4: Spread ----------------------------------------------------
    hdr("SECTION 4 -- Spread impact (60s horizon)")
    print(col_hdr, file=P)
    for lbl in ["sp:<2", "sp:2-4", "sp:4-8", "sp:>=8", "tight_sp", "wide_sp"]:
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 5: Momentum --------------------------------------------------
    hdr("SECTION 5 -- Momentum bucket expectancy (60s horizon)")
    print(col_hdr, file=P)
    for lbl in ["mom:<-5","mom:-5..-2","mom:-2..0","mom:0..2","mom:2..5","mom:5..10","mom:>=10"]:
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 6: Micro-skew / wall -----------------------------------------
    hdr("SECTION 6 -- Micro-skew and wall ratio (60s horizon)")
    print(col_hdr, file=P)
    for lbl in ["micro>=1","micro>=2","wall>=0.70"]:
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 7: Session ---------------------------------------------------
    hdr("SECTION 7 -- Session / time-of-day (60s horizon)")
    print(col_hdr, file=P)
    for lbl in ["sess:Asia","sess:EU","sess:US-open","sess:US-PM","sess:US-close"]:
        by = get_by_epoch(lbl, 60)
        line = row_str(lbl, 60, by)
        print((line + (" < POSITIVE" if is_pos(by) else "")) if line else f"  {lbl} -- n<{MIN_SAMPLES}", file=P)

    # -- Section 8: Per-symbol ------------------------------------------------
    hdr("SECTION 8 -- Per-symbol expectancy (60s, EFRA_RULE observations)")
    print(f"  {'Symbol':<20} {'OOS_net':>10} {'n':>6} {'hit%':>7} {'t':>6}", file=P)
    results = []
    for sym in sorted(data.keys()):
        by = get_by_epoch(f"EFRA_SYM:{sym}", 60)
        ref = by.get(2) or by.get(1) or by.get(0)
        if ref:
            results.append((sym, ref["mean_net"], ref["n"], ref["hit"], ref["t"]))
    results.sort(key=lambda x: -x[1])
    for sym, mn, n, hr, ts in results:
        flag = " < AVOID" if mn < -10 else (" < POSITIVE" if mn > 0 else "")
        print(f"  {sym:<20} {mn:>+10.2f} {n:>6} {hr:>7.1%} {ts:>+6.2f}{flag}", file=P)

    # -- Section 9: Challenger OOS --------------------------------------------
    hdr("SECTION 9 -- CHALLENGER (OBI>=0.75 + mom>=5 + spread<4bps) -- all horizons")
    print(col_hdr, file=P)
    for h in HORIZONS:
        by = get_by_epoch("CHALLENGER", h)
        line = row_str("CHALLENGER", h, by)
        print((line + (" < POSITIVE OOS" if is_pos(by) else "")) if line else f"  CHALLENGER {h}s -- n<{MIN_SAMPLES}", file=P)

    # -- Section 10: Maker execution ------------------------------------------
    hdr("SECTION 10 -- MAKER execution analysis (gross move - 2*maker_fee)")
    print("  NOTE: Does NOT model adverse fill selection risk.", file=P)
    print(col_hdr, file=P)
    for h in HORIZONS:
        by = get_by_epoch("MAKER_EFRA", h)
        line = row_str("MAKER_EFRA", h, by)
        print((line + (" < MAKER POSITIVE" if is_pos(by) else "")) if line else f"  MAKER_EFRA {h}s -- n<{MIN_SAMPLES}", file=P)

    # -- Section 11: 8-trade diagnostic --------------------------------------
    hdr("SECTION 11 -- Root-cause diagnostic for 8 losing trades")
    trades = [
        ("PONS/USDT","OBI+CONF+TAPE+ACCEL","SLOW", -0.214137),
        ("US/USDT",  "CONF",               "TIME", -0.037343),
        ("LTC/USDT", "CONF",               "FLIP", -0.137098),
        ("PONS/USDT","CONF+TAPE+ACCEL",    "SL",   -0.140619),
        ("SAND/USDT","TAPE",               "FLIP", -0.096185),
        ("FET/USDT", "CONF",               "TIME", -0.006349),
        ("QNT/USDT", "ALL",                "FLIP", -0.062120),
        ("CT/USDT",  "ALL",                "TIME", -0.028818),
    ]
    diagnoses_map = {
        "FLIP": "OBI inverted post-entry -- impulse was at tail, not start",
        "SLOW": "slow drift, no follow-through -- momentum was already exhausted",
        "TIME": "no directional move in 180s -- signal was transient/stale",
        "SL":   "hard reversal -- entry was noise, not sustained buying",
    }
    extra_map = {
        "TAPE": "TAPE path unvalidated (no live CVD in recorder) -- may be false positive",
        "ACCEL": "ACCEL path unvalidated (no live tape) -- may be false positive",
        "ALL":  "ALL-gate fires when streak>=5; very low sample at that regime",
    }

    print(f"  #  {'Symbol':<12} {'Path':<22} {'Exit':<6} {'PnL':>9}  Diagnosis", file=P)
    print(f"  {'-'*80}", file=P)
    for idx, (sym, path, exit_r, pnl) in enumerate(trades, 1):
        # look up sym OOS expectancy
        by = get_by_epoch(f"sym:{sym}", 60)
        ref = by.get(2) or by.get(1) or by.get(0)
        sym_ev = f" sym_OOS={ref['mean_net']:+.1f}bps" if ref else ""
        d = diagnoses_map.get(exit_r, "")
        for token in ("TAPE", "ACCEL", "ALL"):
            if token in path and token in extra_map:
                d += "; " + extra_map[token]
                break
        print(f"  {idx:<3} {sym.replace('/USDT',''):<12} {path:<22} {exit_r:<6} {pnl:>+9.4f}  {d}{sym_ev}", file=P)

    # -- Final verdict ---------------------------------------------------------
    print(f"\n{'='*72}", file=P)
    print("  VERDICT SUMMARY", file=P)
    print(f"{'='*72}", file=P)

    for label, name in [("EFRA_RULE", "Current policy"), ("CHALLENGER", "Challenger policy")]:
        for h in (60, 30, 15):
            oos = seg_net.get((label, h, 2), [])
            val = seg_net.get((label, h, 1), [])
            ref = oos if len(oos) >= MIN_SAMPLES else val
            if len(ref) >= MIN_SAMPLES:
                ev = statistics.fmean(ref)
                ts_v = tstat(ref)
                src = "OOS" if ref is oos else "VAL"
                sign = "POSITIVE" if ev > 0 else "NEGATIVE"
                print(f"  {name:<20} @{h:>3}s  EV={ev:>+7.2f}bps  t={ts_v:>+5.2f}  n={len(ref):>5}  [{src}]  {sign}", file=P)
                break
        else:
            print(f"  {name}: insufficient data at all horizons", file=P)

    print(f"\n  CVD LIMITATION: recorder DB has no trade tape.", file=P)
    print(f"  TAPE and ACCEL paths cannot be validated here.", file=P)
    print(f"  CONF scores use cvd_proxy=0.5 (neutral) -- understates true confidence.", file=P)
    print(f"\n  NEXT STEP: run the recorder with CVD tape, or read bot logs for", file=P)
    print(f"  actual CVD values at each of the 8 entry timestamps.", file=P)
    print(f"{'='*72}\n", file=P)


def main():
    # Force UTF-8 output on all platforms
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass  # Python < 3.7
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",            default="/opt/app-platform/state/efra-research/efra_research.db")
    ap.add_argument("--fee-bps",       type=float, default=FEE_BPS_DEFAULT)
    ap.add_argument("--maker-fee-bps", type=float, default=20.0)
    ap.add_argument("--slippage-bps",  type=float, default=SLIP_BPS_DEFAULT)
    ap.add_argument("--out",           default="-")
    args = ap.parse_args()

    if args.out == "-":
        analyze(args, sys.stdout)
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            analyze(args, f)
        with open(args.out, encoding="utf-8") as f:
            sys.stdout.write(f.read())
        print(f"\nWritten to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
