"""
Ticker Tracker - Historical tracking of all tickers that appear in scanners
Tracks which tickers appeared, when, in which scanner, and with what metrics
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Any
import json
from .redis_client import RedisClient

logger = logging.getLogger(__name__)


class TickerTracker:
    """Tracks historical ticker appearances across all scanners"""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self.ttl_days = 7  # Keep history for 7 days

    async def track_ticker(
        self,
        ticker: str,
        scanner_type: str,
        data: Dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Track a ticker appearance in a scanner

        Args:
            ticker: Stock ticker symbol
            scanner_type: Type of scanner (gap, momentum, breakout, etc)
            data: Scanner data (price, gap%, volume, etc)
            timestamp: When the ticker appeared (default: now)

        Returns:
            True if successfully tracked
        """
        try:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            date_key = timestamp.strftime("%Y-%m-%d")

            # Create tracking entry
            entry = {
                "ticker": ticker,
                "scanner": scanner_type,
                "timestamp": timestamp.isoformat(),
                "time": timestamp.strftime("%H:%M:%S"),
                "price": data.get("price", 0),
                "gap_percent": data.get("gap_percent", 0),
                "volume": data.get("volume", "0"),
                "change_percent": data.get("change_percent", 0),
                "high_spike_percent": data.get("high_spike_percent", 0),
                "low_bounce_percent": data.get("low_bounce_percent", 0),
                "momentum_score": data.get("momentum_score", 0)
            }

            # Store in daily ticker set (for quick lookup)
            daily_set_key = f"tickers:daily:{date_key}"
            await self.redis.sadd(daily_set_key, ticker)
            await self.redis.expire(daily_set_key, self.ttl_days * 86400)

            # Store in scanner-specific daily set
            scanner_set_key = f"tickers:{scanner_type}:{date_key}"
            await self.redis.sadd(scanner_set_key, ticker)
            await self.redis.expire(scanner_set_key, self.ttl_days * 86400)

            # Store detailed entry in sorted set (score = timestamp)
            history_key = f"ticker:history:{date_key}"
            score = timestamp.timestamp()
            await self.redis.zadd(history_key, {json.dumps(entry): score})
            await self.redis.expire(history_key, self.ttl_days * 86400)

            # Store ticker-specific history
            ticker_history_key = f"ticker:detail:{ticker}:{date_key}"
            await self.redis.zadd(ticker_history_key, {json.dumps(entry): score})
            await self.redis.expire(ticker_history_key, self.ttl_days * 86400)

            logger.debug(f"[TRACKER] Tracked {ticker} in {scanner_type} at {timestamp.strftime('%H:%M:%S')}")
            return True

        except Exception as e:
            logger.error(f"[TRACKER] Error tracking {ticker}: {e}")
            return False

    async def track_batch(
        self,
        tickers: List[Dict[str, Any]],
        scanner_type: str,
        timestamp: Optional[datetime] = None
    ) -> int:
        """
        Track multiple tickers at once

        Returns:
            Number of tickers successfully tracked
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        tracked = 0
        for ticker_data in tickers:
            ticker = ticker_data.get("ticker")
            if ticker and await self.track_ticker(ticker, scanner_type, ticker_data, timestamp):
                tracked += 1

        logger.info(f"[TRACKER] Tracked {tracked}/{len(tickers)} tickers from {scanner_type}")
        return tracked

    async def get_daily_tickers(self, date: Optional[str] = None) -> Set[str]:
        """
        Get all unique tickers that appeared on a specific date

        Args:
            date: Date in YYYY-MM-DD format (default: today)

        Returns:
            Set of ticker symbols
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        daily_set_key = f"tickers:daily:{date}"
        tickers = await self.redis.smembers(daily_set_key)
        return set(tickers) if tickers else set()

    async def get_scanner_tickers(self, scanner_type: str, date: Optional[str] = None) -> Set[str]:
        """
        Get tickers that appeared in a specific scanner on a date

        Args:
            scanner_type: Type of scanner (gap, momentum, etc)
            date: Date in YYYY-MM-DD format (default: today)

        Returns:
            Set of ticker symbols
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        scanner_set_key = f"tickers:{scanner_type}:{date}"
        tickers = await self.redis.smembers(scanner_set_key)
        return set(tickers) if tickers else set()

    async def get_ticker_history(
        self,
        ticker: str,
        date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get detailed history for a specific ticker on a date

        Args:
            ticker: Stock ticker symbol
            date: Date in YYYY-MM-DD format (default: today)

        Returns:
            List of appearance entries sorted by time
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        ticker_history_key = f"ticker:detail:{ticker}:{date}"

        # Get all entries sorted by timestamp
        entries = await self.redis.zrange(ticker_history_key, 0, -1, withscores=False)

        history = []
        for entry_json in entries:
            try:
                history.append(json.loads(entry_json))
            except json.JSONDecodeError:
                logger.warning(f"[TRACKER] Invalid JSON in history for {ticker}")

        return history

    async def get_daily_history(
        self,
        date: Optional[str] = None,
        scanner_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all ticker appearances for a date

        Args:
            date: Date in YYYY-MM-DD format (default: today)
            scanner_filter: Optional scanner type filter

        Returns:
            List of all appearances sorted by time
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        history_key = f"ticker:history:{date}"

        # Get all entries sorted by timestamp
        entries = await self.redis.zrange(history_key, 0, -1, withscores=False)

        history = []
        for entry_json in entries:
            try:
                entry = json.loads(entry_json)
                if scanner_filter is None or entry.get("scanner") == scanner_filter:
                    history.append(entry)
            except json.JSONDecodeError:
                logger.warning(f"[TRACKER] Invalid JSON in daily history")

        return history

    async def get_ticker_summary(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics for a date

        Returns:
            Summary with counts per scanner, top movers, etc
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Get all unique tickers
        all_tickers = await self.get_daily_tickers(date)

        # Get scanner-specific counts
        scanner_counts = {}
        for scanner in ["gap", "momentum", "breakout"]:
            scanner_tickers = await self.get_scanner_tickers(scanner, date)
            scanner_counts[scanner] = len(scanner_tickers)

        # Get detailed history for analysis
        history = await self.get_daily_history(date)

        # Find top movers
        top_gaps = []
        top_momentum = []

        # Group by ticker to find best values
        ticker_best = {}
        for entry in history:
            ticker = entry.get("ticker")
            if ticker not in ticker_best:
                ticker_best[ticker] = entry
            else:
                # Keep entry with highest gap/momentum
                if entry.get("gap_percent", 0) > ticker_best[ticker].get("gap_percent", 0):
                    ticker_best[ticker] = entry

        # Sort for top movers
        sorted_by_gap = sorted(
            ticker_best.values(),
            key=lambda x: abs(x.get("gap_percent", 0)),
            reverse=True
        )[:10]

        sorted_by_momentum = sorted(
            [e for e in ticker_best.values() if e.get("momentum_score", 0) > 0],
            key=lambda x: x.get("momentum_score", 0),
            reverse=True
        )[:10]

        return {
            "date": date,
            "total_unique_tickers": len(all_tickers),
            "scanner_counts": scanner_counts,
            "top_gaps": [
                {
                    "ticker": e.get("ticker"),
                    "gap_percent": e.get("gap_percent"),
                    "price": e.get("price"),
                    "time": e.get("time")
                }
                for e in sorted_by_gap
            ],
            "top_momentum": [
                {
                    "ticker": e.get("ticker"),
                    "momentum_score": e.get("momentum_score"),
                    "price": e.get("price"),
                    "time": e.get("time")
                }
                for e in sorted_by_momentum
            ],
            "total_appearances": len(history)
        }

    async def is_ticker_tracked_today(self, ticker: str) -> bool:
        """
        Check if a ticker has been tracked today

        Args:
            ticker: Stock ticker symbol

        Returns:
            True if ticker appeared in any scanner today
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_tickers = await self.get_daily_tickers(today)
        return ticker in daily_tickers

    async def cleanup_old_data(self, days_to_keep: int = 7) -> int:
        """
        Clean up data older than specified days

        Returns:
            Number of keys deleted
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
            deleted = 0

            # Scan for old keys and delete them
            for i in range(days_to_keep + 1, 30):  # Check up to 30 days back
                old_date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")

                # Delete various key patterns
                patterns = [
                    f"tickers:daily:{old_date}",
                    f"tickers:*:{old_date}",
                    f"ticker:history:{old_date}",
                    f"ticker:detail:*:{old_date}"
                ]

                for pattern in patterns:
                    keys = []
                    _cursor = 0
                    while True:
                        _cursor, _batch = await self.redis.scan(_cursor, match=pattern, count=100)
                        keys.extend(_batch)
                        if _cursor == 0:
                            break
                    if keys:
                        for key in keys:
                            if await self.redis.delete(key):
                                deleted += 1

            logger.info(f"[TRACKER] Cleaned up {deleted} old keys")
            return deleted

        except Exception as e:
            logger.error(f"[TRACKER] Error cleaning up old data: {e}")
            return 0