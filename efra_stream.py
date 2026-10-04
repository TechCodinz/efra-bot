#!/usr/bin/env python3
"""
EFRA STREAM - Ultra-low-latency live data engine using ccxt.pro WebSockets.

Maintains real-time L2 order book depth, rolling Cumulative Volume Delta (CVD)
from public trade tape, and micro-price momentum with sub-50ms latency.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
import threading


class LiveStreamEngine:
    def __init__(self, exchange_id: str, quote: str = "USDT", exchange_config: dict = None):
        self.exchange_id = exchange_id.lower()
        self.quote = quote
        self.exchange_config = exchange_config or {}
        
        self.symbols = set()
        self._books = {}         # sym -> snapshot dict
        self._trades = defaultdict(lambda: deque(maxlen=300))  # sym -> deque of (ts, side, amount, price)
        self._mids = defaultdict(lambda: deque(maxlen=20))      # sym -> deque of (ts, mid)
        self._lock = threading.Lock()
        
        self.is_running = False
        self._loop = None
        self._thread = None
        self._ex = None
        self._tasks = {}
        self.last_update_ts = {}

    def start(self, symbols: list, markets: dict = None):
        """Starts background WebSocket streaming thread for the given symbols."""
        self._initial_markets = markets
        with self._lock:
            self.symbols = set(symbols)
            self.is_running = True

        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()
        logging.info("LiveStreamEngine: Background streaming started for %d pairs", len(symbols))

    def update_symbols(self, new_symbols: list):
        """Dynamically updates watched symbols without dropping existing streams."""
        with self._lock:
            target_set = set(new_symbols)
            added = target_set - self.symbols
            removed = self.symbols - target_set
            self.symbols = target_set

        if self._loop and self.is_running:
            for s in added:
                asyncio.run_coroutine_threadsafe(self._subscribe_symbol(s), self._loop)
            for s in removed:
                t = self._tasks.pop(s, None)
                if t:
                    t.cancel()

    def stop(self):
        self.is_running = False
        if self._loop and self._ex:
            future = asyncio.run_coroutine_threadsafe(self._cleanup(), self._loop)
            try:
                future.result(timeout=4.0)
            except Exception:
                pass
        logging.info("LiveStreamEngine stopped.")

    def _run_event_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main_async())
        finally:
            self._loop.close()

    async def _main_async(self):
        import ccxt.pro as ccxtpro
        opts = {"defaultType": "spot"}
        opts.update(self.exchange_config.get("options", {}))
        params = {
            "enableRateLimit": True,
            "timeout": 15000,
            "options": opts,
        }
        if "apiKey" in self.exchange_config:
            params["apiKey"] = self.exchange_config["apiKey"]
            params["secret"] = self.exchange_config["secret"]
            if "password" in self.exchange_config:
                params["password"] = self.exchange_config["password"]

        ex_class = getattr(ccxtpro, self.exchange_id, None)
        if not ex_class:
            logging.error("LiveStreamEngine: ccxt.pro has no exchange '%s'", self.exchange_id)
            return

        self._ex = ex_class(params)
        try:
            # If markets were passed in, use set_markets immediately
            if getattr(self, "_initial_markets", None):
                try:
                    self._ex.set_markets(self._initial_markets)
                    logging.debug("LiveStreamEngine: Using %d pre-loaded markets", len(self._initial_markets))
                except Exception as e:
                    logging.debug("LiveStreamEngine set_markets error (%s), loading asynchronously...", e)
                    await asyncio.wait_for(self._ex.load_markets(), timeout=20.0)
            else:
                try:
                    await asyncio.wait_for(self._ex.load_markets(), timeout=20.0)
                except Exception as e:
                    logging.warning("LiveStreamEngine: load_markets slow/failed (%s), proceeding...", e)

            # Spawn subscriber tasks for all symbols
            for sym in list(self.symbols):
                self._tasks[sym] = asyncio.create_task(self._subscribe_symbol(sym))

            while self.is_running:
                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error("LiveStreamEngine main loop error: %s", e)
        finally:
            await self._cleanup()

    async def _cleanup(self):
        for t in self._tasks.values():
            t.cancel()
        if self._ex:
            await self._ex.close()

    async def _subscribe_symbol(self, sym: str):
        """Watches both order book and trade tape independently for a symbol."""
        ob_t = asyncio.create_task(self._watch_book(sym))
        tr_t = asyncio.create_task(self._watch_trades(sym))
        self._tasks[f"ob_{sym}"] = ob_t
        self._tasks[f"tr_{sym}"] = tr_t
        try:
            await asyncio.gather(ob_t, tr_t, return_exceptions=True)
        except asyncio.CancelledError:
            ob_t.cancel()
            tr_t.cancel()
        except Exception as e:
            logging.debug("Stream supervisor error for %s: %s", sym, e)

    async def _watch_book(self, sym: str):
        limit = 20 if self.exchange_id in ("kucoin", "gate", "gateio") else 10
        err_count = 0
        while self.is_running and sym in self.symbols:
            try:
                ob = await asyncio.wait_for(self._ex.watch_order_book(sym, limit=limit), timeout=8.0)
                now = time.time()
                bids = ob.get("bids") or []
                asks = ob.get("asks") or []
                if not bids or not asks:
                    continue

                best_bid, best_ask = bids[0][0], asks[0][0]
                mid = (best_bid + best_ask) / 2.0
                spread_bps = (best_ask - best_bid) / mid * 1e4

                # Depth calculations
                b5 = sum(lvl[0] * lvl[1] for lvl in bids[:5])
                a5 = sum(lvl[0] * lvl[1] for lvl in asks[:5])
                imb5 = b5 / (b5 + a5) if (b5 + a5) > 0 else 0.5

                b10 = sum(lvl[0] * lvl[1] for lvl in bids[:10])
                a10 = sum(lvl[0] * lvl[1] for lvl in asks[:10])
                imb10 = b10 / (b10 + a10) if (b10 + a10) > 0 else 0.5

                # Micro-price (volume-weighted top level)
                bq1 = bids[0][0] * bids[0][1]
                aq1 = asks[0][0] * asks[0][1]
                micro_px = (best_ask * bq1 + best_bid * aq1) / (bq1 + aq1) if (bq1 + aq1) > 0 else mid

                with self._lock:
                    self._mids[sym].append((now, mid))
                    # Momentum calculation
                    mids_hist = list(self._mids[sym])
                    mom = 0.0
                    mom_accel = 0.0
                    if len(mids_hist) >= 4:
                        # Primary: compare latest mid to ~6s ago (middle of buffer)
                        mid_old = mids_hist[0][1]
                        mom = (mid - mid_old) / mid_old * 1e4
                        # Acceleration: recent half vs older half slope
                        half = len(mids_hist) // 2
                        mid_mid = mids_hist[half][1]
                        mom_recent = (mid - mid_mid) / mid_mid * 1e4 if mid_mid > 0 else 0.0
                        mom_older = (mid_mid - mid_old) / mid_old * 1e4 if mid_old > 0 else 0.0
                        mom_accel = mom_recent - mom_older  # positive = accelerating up

                    # Read CVD metrics from trade tape
                    cvd_5s, cvd_15s = self._calc_cvd(sym, now)

                    # Top of book micro-price skew in bps
                    micro_skew = ((micro_px - mid) / mid) * 1e4 if mid > 0 else 0.0

                    # Confluence score: 0 to 100
                    # OBI(40%) + CVD(30%) + micro-skew(20%) + momentum(10%)
                    imb_score = max(0.0, min(1.0, (imb5 - 0.5) * 3.0))
                    cvd_score = max(0.0, min(1.0, (cvd_5s - 0.5) * 3.0))
                    micro_score = max(0.0, min(1.0, micro_skew / 2.0)) if micro_skew > 0 else 0.0
                    mom_score = max(0.0, min(1.0, (mom + 2.0) / 6.0)) if mom > -2.0 else 0.0
                    confluence = (imb_score * 40.0 + cvd_score * 30.0 + micro_score * 20.0 + mom_score * 10.0)

                    self._books[sym] = {
                        "bid": best_bid,
                        "ask": best_ask,
                        "mid": mid,
                        "micro_px": micro_px,
                        "micro_skew": micro_skew,
                        "spread": spread_bps,
                        "imb": imb5,
                        "imb10": imb10,
                        "cvd_5s": cvd_5s,
                        "cvd_15s": cvd_15s,
                        "mom": mom,
                        "mom_accel": mom_accel,
                        "confluence": confluence,
                        "ts": now,
                        "bids": bids[:5],
                        "asks": asks[:5],
                    }
                    self.last_update_ts[sym] = now
                    err_count = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                err_count += 1
                wait_time = min(5.0, 0.2 * (2 ** min(err_count, 5)))
                logging.debug("watch_book [%s] err #%d: %s. Backing off %.1fs", sym, err_count, e, wait_time)
                await asyncio.sleep(wait_time)

    async def _watch_trades(self, sym: str):
        """Watches live trades to calculate real-time trade aggressor flow (CVD)."""
        err_count = 0
        while self.is_running and sym in self.symbols:
            try:
                trades = await asyncio.wait_for(self._ex.watch_trades(sym, limit=50), timeout=8.0)
                now = time.time()
                with self._lock:
                    # Get current mid for side inference fallback
                    book = self._books.get(sym, {})
                    cur_mid = book.get("mid", 0.0)
                    tape = self._trades[sym]
                    for tr in trades:
                        # Multi-field side detection — Gate.io sometimes uses
                        # takerSide, taker_side, or leaves 'side' empty.
                        side = (
                            tr.get("side")
                            or tr.get("takerSide")
                            or tr.get("taker_side")
                            or tr.get("info", {}).get("side", "")
                            or tr.get("info", {}).get("type", "")
                        )
                        side = str(side).lower().strip()
                        # Normalize aliases
                        if side in ("b", "bid", "buy", "1"):
                            side = "buy"
                        elif side in ("s", "ask", "sell", "0", "-1"):
                            side = "sell"
                        else:
                            # Fallback: infer from price vs current mid
                            px_raw = float(tr.get("price") or 0.0)
                            if cur_mid > 0 and px_raw > 0:
                                side = "buy" if px_raw >= cur_mid else "sell"
                            else:
                                side = "buy"  # neutral default
                        amt = float(tr.get("amount") or 0.0)
                        px = float(tr.get("price") or 0.0)
                        tr_ts = float(tr.get("timestamp") or (now * 1000.0)) / 1000.0
                        if amt > 0 and px > 0:
                            tape.append((tr_ts, side, amt * px, px))
                err_count = 0
            except asyncio.TimeoutError:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception:
                err_count += 1
                await asyncio.sleep(min(5.0, 0.5 * err_count))

    def _calc_cvd(self, sym: str, now: float):
        """Calculates Cumulative Volume Delta (buyer ratio) over 5s and 15s."""
        tape = self._trades[sym]
        if not tape:
            return 0.5, 0.5

        buy_5s = sell_5s = 0.0
        buy_15s = sell_15s = 0.0

        t_5 = now - 5.0
        t_15 = now - 15.0

        for tr_ts, side, val, _ in tape:
            if tr_ts >= t_15:
                if side == "buy":
                    buy_15s += val
                else:
                    sell_15s += val
                if tr_ts >= t_5:
                    if side == "buy":
                        buy_5s += val
                    else:
                        sell_5s += val

        tot_5 = buy_5s + sell_5s
        tot_15 = buy_15s + sell_15s

        ratio_5s = buy_5s / tot_5 if tot_5 > 0 else 0.5
        ratio_15s = buy_15s / tot_15 if tot_15 > 0 else 0.5

        return ratio_5s, ratio_15s

    def get_book(self, sym: str, max_age_s: float = 3.0):
        """Returns the latest snapshot if fresh, else None."""
        with self._lock:
            b = self._books.get(sym)
            if b and (time.time() - b.get("ts", 0) <= max_age_s):
                return b
        return None

    def get_all_books(self, max_age_s: float = 3.0):
        """Returns all fresh snapshots."""
        now = time.time()
        with self._lock:
            return {
                sym: b for sym, b in self._books.items()
                if (now - b.get("ts", 0) <= max_age_s)
            }
