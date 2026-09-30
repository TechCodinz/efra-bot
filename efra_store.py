#!/usr/bin/env python3
"""PostgreSQL durability and single-writer execution lease for EFRA.

The store is intentionally optional.  When EFRA_DATABASE_URL is absent the
engine continues to use its local JSON/CSV files.  When configured, the same
runtime state and realized trade ledger are mirrored to PostgreSQL and an
advisory lock ensures only one process owns execution during rolling deploys.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("efra.store")


class PostgresStore:
    def __init__(self, database_url: str, state_key: str):
        self.database_url = database_url
        self.state_key = state_key
        digest = hashlib.sha256(("efra:" + state_key).encode("utf-8")).digest()
        # pg advisory locks accept a signed bigint.
        self.lock_id = int.from_bytes(digest[:8], "big", signed=True)
        self._lease_conn: psycopg.Connection | None = None

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            self.database_url,
            autocommit=True,
            connect_timeout=10,
            application_name="efra-sniper-v2",
        )

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS efra_runtime_state (
                    state_key TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS efra_trade_ledger (
                    id BIGSERIAL PRIMARY KEY,
                    state_key TEXT NOT NULL,
                    ts BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    entry DOUBLE PRECISION NOT NULL,
                    exit DOUBLE PRECISION NOT NULL,
                    qty DOUBLE PRECISION NOT NULL,
                    pnl_quote DOUBLE PRECISION NOT NULL,
                    reason TEXT NOT NULL,
                    equity DOUBLE PRECISION NOT NULL,
                    tier INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS efra_trade_ledger_state_ts_idx
                ON efra_trade_ledger (state_key, ts DESC)
                """
            )

    def try_acquire_lease(self) -> bool:
        if self._lease_conn is not None:
            return self.lease_alive()
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (self.lock_id,),
            ).fetchone()
            if not row or not bool(row[0]):
                conn.close()
                return False
            self._lease_conn = conn
            return True
        except Exception:
            conn.close()
            raise

    def lease_alive(self) -> bool:
        if self._lease_conn is None or self._lease_conn.closed:
            return False
        try:
            self._lease_conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def release_lease(self) -> None:
        conn = self._lease_conn
        self._lease_conn = None
        if conn is None:
            return
        try:
            if not conn.closed:
                conn.execute("SELECT pg_advisory_unlock(%s)", (self.lock_id,))
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def save_state(self, payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO efra_runtime_state (state_key, payload, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (state_key)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()
                """,
                (self.state_key, Jsonb(payload)),
            )

    def load_state(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM efra_runtime_state WHERE state_key = %s",
                (self.state_key,),
            ).fetchone()
        if not row:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return dict(payload)

    def save_trade(self, trade: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO efra_trade_ledger
                    (state_key, ts, symbol, entry, exit, qty, pnl_quote, reason, equity, tier)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    self.state_key,
                    int(trade["ts"]),
                    str(trade["symbol"]),
                    float(trade["entry"]),
                    float(trade["exit"]),
                    float(trade["qty"]),
                    float(trade["pnl_quote"]),
                    str(trade["reason"]),
                    float(trade["equity"]),
                    int(trade["tier"]),
                ),
            )

    def recent_trades(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ts, symbol, entry, exit, qty, pnl_quote, reason, equity, tier
                FROM efra_trade_ledger
                WHERE state_key = %s
                ORDER BY ts DESC, id DESC
                LIMIT %s
                """,
                (self.state_key, limit),
            ).fetchall()
        return [
            {
                "ts": int(r[0]),
                "symbol": r[1],
                "entry": float(r[2]),
                "exit": float(r[3]),
                "qty": float(r[4]),
                "pnl_quote": float(r[5]),
                "reason": r[6],
                "equity": float(r[7]),
                "tier": int(r[8]),
            }
            for r in reversed(rows)
        ]

    def close(self) -> None:
        self.release_lease()
