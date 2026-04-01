"""Trade persistence for strategy_trades table.

All strategies call save_trade_entry() on ENTRY and save_trade_exit() on WIN/LOSS/EXPIRED.
Uses the existing DatabaseService connection pool.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


async def save_trade_entry(
    db,
    strategy_name: str,
    ticker: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    target_price: float,
    entry_time: datetime,
    metadata: Optional[dict] = None,
    target_2: Optional[float] = None,
    target_3: Optional[float] = None,
) -> Optional[int]:
    """Insert a new trade row on ENTRY. Returns the trade_id (serial PK)."""
    try:
        if not db or not db.conn_pool:
            logger.warning("[TRADE_DB] No connection pool, skipping save_trade_entry")
            return None

        row = await db.fetch_one(
            """
            INSERT INTO strategy_trades
                (strategy_name, ticker, direction, entry_price, stop_loss,
                 target_price, target_2, target_3, entry_time, status, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ACTIVE', $10)
            RETURNING id
            """,
            strategy_name,
            ticker,
            direction,
            entry_price,
            stop_loss,
            target_price,
            target_2,
            target_3,
            entry_time,
            json.dumps(metadata or {}),
        )
        if row:
            trade_id = row["id"]
            logger.info(f"[TRADE_DB] Saved ENTRY #{trade_id}: {strategy_name} {direction} {ticker} @ ${entry_price:.4f}")
            return trade_id
        return None
    except Exception as e:
        logger.error(f"[TRADE_DB] Error saving entry for {ticker}: {e}")
        return None


async def save_trade_exit(
    db,
    trade_id: int,
    exit_price: float,
    exit_time: datetime,
    status: str,
    exit_reason: str,
    high_price: float = 0,
    low_price: float = 0,
    pnl_pct: float = 0,
    r_multiple: Optional[float] = None,
) -> bool:
    """Update a trade row on EXIT (WIN/LOSS/EXPIRED). Returns True on success."""
    try:
        if not db or not db.conn_pool:
            logger.warning("[TRADE_DB] No connection pool, skipping save_trade_exit")
            return False

        if not trade_id:
            logger.warning("[TRADE_DB] save_trade_exit called with trade_id=None, skipping DB update")
            return False

        await db.execute(
            """
            UPDATE strategy_trades
            SET exit_price = $1, exit_time = $2, status = $3, exit_reason = $4,
                high_price = $5, low_price = $6, pnl_pct = $7, r_multiple = $8,
                updated_at = NOW()
            WHERE id = $9
            """,
            exit_price,
            exit_time,
            status,
            exit_reason,
            high_price,
            low_price,
            pnl_pct,
            r_multiple,
            trade_id,
        )
        logger.info(f"[TRADE_DB] Saved EXIT #{trade_id}: {status} {exit_reason} P&L={pnl_pct:+.2f}%")
        return True
    except Exception as e:
        logger.error(f"[TRADE_DB] Error saving exit for trade #{trade_id}: {e}")
        return False


async def recover_trade_id(db, strategy_name: str, ticker: str) -> Optional[int]:
    """Recover a trade's DB id by strategy_name + ticker when db_id is lost (e.g. after restart)."""
    try:
        if not db or not db.conn_pool:
            return None
        row = await db.fetch_one(
            """SELECT id FROM strategy_trades
               WHERE strategy_name = $1 AND ticker = $2 AND status = 'ACTIVE'
               ORDER BY entry_time DESC LIMIT 1""",
            strategy_name, ticker,
        )
        if row:
            logger.info(f"[TRADE_DB] Recovered trade_id={row['id']} for {strategy_name} {ticker}")
            return row["id"]
        return None
    except Exception as e:
        logger.error(f"[TRADE_DB] Error recovering trade_id for {ticker}: {e}")
        return None


async def close_stale_trades(db, strategy_name: str, cutoff_reason: str = "EOD_STALE") -> int:
    """Close all ACTIVE trades for a strategy from previous days. Call on startup.
    Returns number of trades closed."""
    try:
        if not db or not db.conn_pool:
            return 0
        result = await db.execute(
            """UPDATE strategy_trades
               SET status = 'EXPIRED', exit_reason = $1,
                   exit_time = NOW(), updated_at = NOW(),
                   exit_price = entry_price,
                   pnl_pct = 0
               WHERE strategy_name = $2 AND status = 'ACTIVE'
                 AND entry_time < (NOW() AT TIME ZONE 'America/New_York')::date::timestamptz""",
            cutoff_reason, strategy_name,
        )
        count = int(result.split()[-1]) if result and result.split()[-1].isdigit() else 0
        if count > 0:
            logger.warning(f"[TRADE_DB] Closed {count} stale ACTIVE trades for {strategy_name}")
        return count
    except Exception as e:
        logger.error(f"[TRADE_DB] Error closing stale trades for {strategy_name}: {e}")
        return 0
