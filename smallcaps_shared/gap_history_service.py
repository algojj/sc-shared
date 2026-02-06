"""
Gap History Service
Tracks and calculates daily gap percentages for all tickers
Supports both scheduled daily updates and on-demand historical processing
"""

import asyncio
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
import asyncpg
import os
from dataclasses import dataclass
import pytz

from .polygon_base import PolygonBaseClient
from .database_service import DatabaseService

logger = logging.getLogger(__name__)


@dataclass
class GapData:
    """Data class for gap information"""
    ticker: str
    date: date
    gap_percent: float
    gap_type: str  # 'gap_up', 'gap_down', 'no_gap'
    open_price: float
    previous_close: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    average_volume: Optional[int] = None
    volume_ratio: Optional[float] = None
    fade_percent: Optional[float] = None
    close_vs_open_percent: Optional[float] = None
    intraday_range: Optional[float] = None
    market_gap: Optional[float] = None
    sector: Optional[str] = None
    market_cap: Optional[int] = None


class GapHistoryService:
    """Service for managing gap history data"""

    def __init__(self, polygon_api_key: Optional[str] = None):
        # Set API key in environment if provided
        if polygon_api_key:
            os.environ["POLYGON_API_KEY"] = polygon_api_key
        self.polygon_client = PolygonBaseClient()
        self.db_service = DatabaseService()
        self.et_tz = pytz.timezone('America/New_York')
        self.logger = logger

    async def initialize(self):
        """Initialize the service"""
        await self.db_service.init()
        self.logger.info("Gap History Service initialized")

    async def calculate_daily_gaps(self, date: Optional[date] = None) -> List[GapData]:
        """
        Calculate gaps for all active tickers for a specific date
        Uses grouped daily API call (1 request instead of N requests)
        """
        target_date = date or datetime.now(self.et_tz).date()
        self.logger.info(f"Calculating gaps for {target_date}")

        # Get all active tickers from database
        tickers = await self._get_active_tickers()
        self.logger.info(f"Processing {len(tickers)} tickers")

        # FIX: Use grouped daily bars (1 API call for ALL tickers)
        current_bars = await self._get_grouped_bars(target_date)
        if not current_bars:
            self.logger.error(f"No grouped data for {target_date}")
            return []

        # Get previous trading day
        prev_date = target_date - timedelta(days=1)
        while prev_date.weekday() >= 5:  # Skip weekends
            prev_date -= timedelta(days=1)

        previous_bars = await self._get_grouped_bars(prev_date)
        if not previous_bars:
            self.logger.error(f"No grouped data for {prev_date}")
            return []

        # Calculate gaps in memory (no more N API calls!)
        gaps = []
        for ticker in tickers:
            try:
                gap = await self._calculate_gap_from_bars(
                    ticker, target_date, current_bars, previous_bars
                )
                if gap:
                    gaps.append(gap)
            except Exception as e:
                self.logger.debug(f"Error processing {ticker}: {e}")

        self.logger.info(f"Calculated {len(gaps)} gaps for {target_date}")
        return gaps

    async def _get_grouped_bars(self, date: date) -> Dict[str, Dict]:
        """Get ALL ticker bars for a date in single API call"""
        try:
            # Use the existing get_grouped_daily method from PolygonBaseClient
            response = await self.polygon_client.get_grouped_daily(date.isoformat())

            if not response or 'results' not in response:
                return {}

            # Convert list to dict keyed by ticker for O(1) lookup
            return {bar['T']: bar for bar in response['results']}

        except Exception as e:
            self.logger.error(f"Error fetching grouped bars for {date}: {e}")
            return {}

    async def _calculate_gap_from_bars(
        self,
        ticker: str,
        target_date: date,
        current_bars: Dict[str, Dict],
        previous_bars: Dict[str, Dict]
    ) -> Optional[GapData]:
        """Calculate gap from pre-fetched bar data (no API calls)"""
        # Get bars from in-memory dicts (O(1) lookup)
        current = current_bars.get(ticker)
        previous = previous_bars.get(ticker)

        if not current or not previous:
            return None

        # Calculate gap
        gap_percent = ((current['o'] - previous['c']) / previous['c']) * 100

        # Determine gap type
        if gap_percent > 1:
            gap_type = 'gap_up'
        elif gap_percent < -1:
            gap_type = 'gap_down'
        else:
            gap_type = 'no_gap'

        # Calculate fade percent
        fade_percent = None
        if gap_type == 'gap_up' and current['h'] > 0:
            fade_percent = ((current['h'] - current['c']) / current['h']) * 100
        elif gap_type == 'gap_down' and current['l'] > 0:
            fade_percent = ((current['c'] - current['l']) / current['l']) * 100

        # Calculate close vs open
        close_vs_open = ((current['c'] - current['o']) / current['o']) * 100 if current['o'] > 0 else None

        # Get average volume (still needs individual call, but async cached)
        avg_volume = await self._get_average_volume(ticker, target_date)
        volume_ratio = current['v'] / avg_volume if avg_volume and avg_volume > 0 else None

        # Get market gap (optimized - only calculates once)
        market_gap = None if ticker == 'SPY' else await self._get_market_gap_from_bars(target_date, current_bars, previous_bars)

        return GapData(
            ticker=ticker,
            date=target_date,
            gap_percent=round(gap_percent, 4),
            gap_type=gap_type,
            open_price=current['o'],
            previous_close=previous['c'],
            high_price=current['h'],
            low_price=current['l'],
            close_price=current['c'],
            volume=current['v'],
            average_volume=avg_volume,
            volume_ratio=round(volume_ratio, 2) if volume_ratio else None,
            fade_percent=round(fade_percent, 4) if fade_percent else None,
            close_vs_open_percent=round(close_vs_open, 4) if close_vs_open else None,
            intraday_range=current['h'] - current['l'],
            market_gap=market_gap
        )

    async def _get_market_gap_from_bars(
        self,
        target_date: date,
        current_bars: Dict[str, Dict],
        previous_bars: Dict[str, Dict]
    ) -> Optional[float]:
        """Get SPY gap from pre-fetched bars (no API call)"""
        try:
            current = current_bars.get('SPY')
            previous = previous_bars.get('SPY')

            if not current or not previous:
                return None

            spy_gap = ((current['o'] - previous['c']) / previous['c']) * 100
            return round(spy_gap, 4)

        except Exception as e:
            self.logger.debug(f"Could not get SPY gap from bars: {e}")
            return None

    async def _calculate_gap_for_ticker(self, ticker: str, target_date: date) -> Optional[GapData]:
        """Calculate gap for a specific ticker and date"""
        try:
            # Get current day's data
            current_day = await self.polygon_client.get_daily_bars(
                ticker,
                target_date.isoformat(),
                target_date.isoformat()
            )

            if not current_day or 'results' not in current_day or not current_day['results']:
                return None

            current = current_day['results'][0]

            # Get previous trading day's data
            prev_date = target_date - timedelta(days=1)
            # Handle weekends - go back to Friday if Monday
            while prev_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                prev_date -= timedelta(days=1)

            previous_day = await self.polygon_client.get_daily_bars(
                ticker,
                prev_date.isoformat(),
                prev_date.isoformat()
            )

            if not previous_day or 'results' not in previous_day or not previous_day['results']:
                return None

            previous = previous_day['results'][0]

            # Calculate gap
            gap_percent = ((current['o'] - previous['c']) / previous['c']) * 100

            # Determine gap type
            if gap_percent > 1:
                gap_type = 'gap_up'
            elif gap_percent < -1:
                gap_type = 'gap_down'
            else:
                gap_type = 'no_gap'

            # Calculate fade percent
            fade_percent = None
            if gap_type == 'gap_up':
                # For gap up, fade is from high to close
                if current['h'] > 0:
                    fade_percent = ((current['h'] - current['c']) / current['h']) * 100
            elif gap_type == 'gap_down':
                # For gap down, fade is from close to low
                if current['l'] > 0:
                    fade_percent = ((current['c'] - current['l']) / current['l']) * 100

            # Calculate close vs open
            close_vs_open = ((current['c'] - current['o']) / current['o']) * 100 if current['o'] > 0 else None

            # Get average volume (20-day)
            avg_volume = await self._get_average_volume(ticker, target_date)
            volume_ratio = current['v'] / avg_volume if avg_volume and avg_volume > 0 else None

            # Get market (SPY) gap for context (skip if we're calculating SPY itself)
            market_gap = None if ticker == 'SPY' else await self._get_market_gap(target_date)

            return GapData(
                ticker=ticker,
                date=target_date,
                gap_percent=round(gap_percent, 4),
                gap_type=gap_type,
                open_price=current['o'],
                previous_close=previous['c'],
                high_price=current['h'],
                low_price=current['l'],
                close_price=current['c'],
                volume=current['v'],
                average_volume=avg_volume,
                volume_ratio=round(volume_ratio, 2) if volume_ratio else None,
                fade_percent=round(fade_percent, 4) if fade_percent else None,
                close_vs_open_percent=round(close_vs_open, 4) if close_vs_open else None,
                intraday_range=current['h'] - current['l'],
                market_gap=market_gap
            )

        except Exception as e:
            self.logger.error(f"Error calculating gap for {ticker} on {target_date}: {e}")
            return None

    async def _get_average_volume(self, ticker: str, end_date: date, days: int = 20) -> Optional[int]:
        """Calculate average volume for the past N trading days"""
        try:
            start_date = end_date - timedelta(days=days * 2)  # Account for weekends/holidays

            bars = await self.polygon_client.get_daily_bars(
                ticker,
                start_date.isoformat(),
                end_date.isoformat()
            )

            if not bars or 'results' not in bars or not bars['results']:
                return None

            volumes = [bar['v'] for bar in bars['results'][-days:]]  # Last N days
            return int(sum(volumes) / len(volumes)) if volumes else None

        except Exception as e:
            self.logger.error(f"Error getting average volume for {ticker}: {e}")
            return None

    async def _get_market_gap(self, target_date: date) -> Optional[float]:
        """Get SPY gap for market context"""
        # Avoid infinite recursion - calculate SPY gap directly
        if not hasattr(self, '_calculating_market_gap'):
            self._calculating_market_gap = False

        if self._calculating_market_gap:
            return None  # Prevent infinite recursion

        try:
            self._calculating_market_gap = True

            # Get current day's data for SPY
            current_day = await self.polygon_client.get_daily_bars(
                'SPY',
                target_date.isoformat(),
                target_date.isoformat()
            )

            if not current_day or 'results' not in current_day or not current_day['results']:
                return None

            current = current_day['results'][0]

            # Get previous trading day's data
            prev_date = target_date - timedelta(days=1)
            while prev_date.weekday() >= 5:  # Skip weekends
                prev_date -= timedelta(days=1)

            previous_day = await self.polygon_client.get_daily_bars(
                'SPY',
                prev_date.isoformat(),
                prev_date.isoformat()
            )

            if not previous_day or 'results' not in previous_day or not previous_day['results']:
                return None

            previous = previous_day['results'][0]

            # Calculate SPY gap
            spy_gap = ((current['o'] - previous['c']) / previous['c']) * 100
            return round(spy_gap, 4)

        except Exception as e:
            self.logger.debug(f"Could not get SPY gap for {target_date}: {e}")
            return None
        finally:
            self._calculating_market_gap = False

    async def save_gap_data(self, gaps: List[GapData]) -> int:
        """Save gap data to database with transaction"""
        self.logger.info(f"[SAVE_GAP] Starting save for {len(gaps)} gaps")

        if not self.db_service.conn_pool:
            self.logger.error("[SAVE_GAP] ❌ No connection pool available!")
            return 0

        self.logger.info(f"[SAVE_GAP] Connection pool status: size={self.db_service.conn_pool.get_size()}, free={self.db_service.conn_pool.get_idle_size()}")
        saved_count = 0
        target_date = gaps[0].date if gaps else None

        try:
            self.logger.info("[SAVE_GAP] Acquiring connection...")
            async with self.db_service.conn_pool.acquire() as conn:
                self.logger.info("[SAVE_GAP] Connection acquired, starting transaction...")
                async with conn.transaction():
                    self.logger.info("[SAVE_GAP] Transaction started")

                    # Fetch float and sector from companies table for enrichment
                    gap_tickers = list(set(g.ticker for g in gaps))
                    company_data = {}
                    try:
                        enrichment_rows = await conn.fetch(
                            "SELECT ticker, dt_float, sector, market_cap FROM companies WHERE ticker = ANY($1::text[])",
                            gap_tickers
                        )
                        for row in enrichment_rows:
                            company_data[row['ticker']] = {
                                'float': row['dt_float'],
                                'sector': row['sector'],
                                'market_cap': row['market_cap']
                            }
                        self.logger.info(f"[SAVE_GAP] Enrichment: got company data for {len(company_data)}/{len(gap_tickers)} tickers")
                    except Exception as e:
                        self.logger.warning(f"[SAVE_GAP] Could not fetch company enrichment data: {e}")

                    query = """
                        INSERT INTO historical_gappers (
                            ticker, date, gap_value, poly_open, prev_close,
                            poly_high, poly_low, poly_close, volume,
                            sector_sic, market_cap, return_pct, close_red, float
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        ON CONFLICT (ticker, date)
                        DO UPDATE SET
                            gap_value = EXCLUDED.gap_value,
                            poly_open = EXCLUDED.poly_open,
                            prev_close = EXCLUDED.prev_close,
                            poly_high = EXCLUDED.poly_high,
                            poly_low = EXCLUDED.poly_low,
                            poly_close = EXCLUDED.poly_close,
                            volume = EXCLUDED.volume,
                            sector_sic = COALESCE(EXCLUDED.sector_sic, historical_gappers.sector_sic),
                            market_cap = COALESCE(EXCLUDED.market_cap, historical_gappers.market_cap),
                            return_pct = EXCLUDED.return_pct,
                            close_red = EXCLUDED.close_red,
                            float = COALESCE(EXCLUDED.float, historical_gappers.float)
                    """

                    for gap in gaps:
                        # Calculate return_pct and close_red from gap data
                        return_pct = round(((gap.close_price - gap.open_price) / gap.open_price) * 100, 2) if gap.open_price > 0 else None
                        close_red = 'red' if gap.close_price < gap.open_price else 'green'
                        # Enrich with company data (float, sector, market_cap)
                        enrichment = company_data.get(gap.ticker, {})
                        sector = gap.sector or enrichment.get('sector')
                        market_cap = gap.market_cap or enrichment.get('market_cap')
                        float_shares = enrichment.get('float')
                        await conn.execute(
                            query,
                            gap.ticker, gap.date, gap.gap_percent,
                            gap.open_price, gap.previous_close,
                            gap.high_price, gap.low_price, gap.close_price,
                            gap.volume, sector, market_cap,
                            return_pct, close_red, float_shares
                        )
                        saved_count += 1
                        if saved_count % 500 == 0:
                            self.logger.info(f"[SAVE_GAP] Progress: {saved_count}/{len(gaps)} inserts")

                    self.logger.info(f"[SAVE_GAP] Loop complete, {saved_count} inserts executed, transaction will commit...")
                # Transaction auto-commits here when exiting without exception
                self.logger.info("[SAVE_GAP] Transaction context exited (should be committed)")

            self.logger.info(f"[SAVE_GAP] ✅ Saved {saved_count}/{len(gaps)} gap records (connection released)")

            # VERIFY: Check if data is actually in the database
            if target_date:
                async with self.db_service.conn_pool.acquire() as verify_conn:
                    verify_count = await verify_conn.fetchval(
                        "SELECT COUNT(*) FROM historical_gappers WHERE date = $1",
                        target_date
                    )
                    self.logger.info(f"[SAVE_GAP] 🔍 VERIFY: {verify_count} records found in DB for {target_date}")
                    if verify_count == 0:
                        self.logger.error(f"[SAVE_GAP] ❌ CRITICAL: Data not persisted! Expected {saved_count}, found 0")

        except Exception as e:
            self.logger.error(f"[SAVE_GAP] ❌ Transaction failed: {e}. All changes rolled back.")
            import traceback
            self.logger.error(f"[SAVE_GAP] Traceback: {traceback.format_exc()}")
            saved_count = 0

        return saved_count

    async def process_historical_gaps(
        self,
        ticker: str,
        start_date: date,
        end_date: Optional[date] = None
    ) -> List[GapData]:
        """
        Process historical gaps for a specific ticker and date range
        Useful for backfilling data
        """
        end_date = end_date or datetime.now(self.et_tz).date()
        self.logger.info(f"Processing historical gaps for {ticker} from {start_date} to {end_date}")

        gaps = []
        current_date = start_date

        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                gap = await self._calculate_gap_for_ticker(ticker, current_date)
                if gap:
                    gaps.append(gap)

            current_date += timedelta(days=1)

        self.logger.info(f"Processed {len(gaps)} historical gaps for {ticker}")
        return gaps

    async def get_gap_history(
        self,
        ticker: str,
        days_back: int = 10
    ) -> List[Dict[str, Any]]:
        """Get gap history for a ticker for pattern detection"""
        query = """
            SELECT
                ticker,
                date,
                gap_value as gap_percent,
                CASE WHEN gap_value > 1 THEN 'gap_up' WHEN gap_value < -1 THEN 'gap_down' ELSE 'no_gap' END as gap_type,
                poly_open as open_price,
                prev_close as previous_close,
                poly_high as high_price,
                poly_low as low_price,
                poly_close as close_price,
                volume,
                NULL::bigint as average_volume,
                NULL::numeric as volume_ratio,
                return_pct as fade_percent,
                return_pct as close_vs_open_percent,
                (poly_high - poly_low) as intraday_range,
                NULL::numeric as market_gap,
                (CURRENT_DATE - date)::INTEGER as days_ago
            FROM historical_gappers
            WHERE ticker = $1
            AND date >= CURRENT_DATE - ($2 || ' days')::INTERVAL
            ORDER BY date DESC
        """

        rows = await self.db_service.fetch_all(query, ticker, str(days_back))
        return [dict(row) for row in rows] if rows else []

    async def get_all_historical_gaps(
        self,
        ticker: str,
        min_gap_percent: float = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ALL historical gap instances for a ticker (not limited by days)

        Args:
            ticker: Stock ticker symbol
            min_gap_percent: Minimum absolute gap percentage to include (default 0 = all)
            limit: Maximum number of records to return (default 100)

        Returns:
            List of all gap records for the ticker
        """
        query = """
            SELECT
                ticker,
                date,
                gap_value as gap_percent,
                CASE WHEN gap_value > 1 THEN 'gap_up' WHEN gap_value < -1 THEN 'gap_down' ELSE 'no_gap' END as gap_type,
                poly_open as open_price,
                prev_close as previous_close,
                poly_high as high_price,
                poly_low as low_price,
                poly_close as close_price,
                volume,
                NULL::bigint as average_volume,
                NULL::numeric as volume_ratio,
                return_pct as fade_percent,
                return_pct as close_vs_open_percent,
                (poly_high - poly_low) as intraday_range,
                NULL::numeric as market_gap,
                (CURRENT_DATE - date)::INTEGER as days_ago,
                NULL::timestamp as created_at,
                NULL::timestamp as updated_at
            FROM historical_gappers
            WHERE ticker = $1
            AND ABS(gap_value) >= $2
            ORDER BY date DESC
            LIMIT $3
        """

        rows = await self.db_service.fetch_all(query, ticker, min_gap_percent, limit)

        result = []
        for row in rows:
            row_dict = dict(row)
            # Convert date objects to ISO strings for JSON serialization
            if row_dict.get('date'):
                row_dict['date'] = row_dict['date'].isoformat()
            if row_dict.get('created_at'):
                row_dict['created_at'] = row_dict['created_at'].isoformat()
            if row_dict.get('updated_at'):
                row_dict['updated_at'] = row_dict['updated_at'].isoformat()
            result.append(row_dict)

        return result

    async def get_gap_stats(
        self,
        ticker: str,
        min_gap_percent: float = 20.0,
        days_back: int = 365
    ) -> Dict[str, Any]:
        """
        Get aggregated gap statistics for a ticker.
        Only considers gap days where gap_percent >= min_gap_percent.

        Returns metrics like:
          GapsAbove 20%: 11 | Avg.Gap: 33% | Prev.GAP High: 5.87 | Avg.Spike: 13%
          Avg.$Range: $4.25 | Prev.Vol.: 329.44M | Avg.High: 23% | Avg.Close: 13%
          Close RED: 6 = 55% | Avg.Low: -19% | Avg.Close(red): -15%
        """
        query = """
            WITH gap_days AS (
                SELECT
                    date, gap_value, poly_open, poly_high, poly_low, poly_close, poly_volume,
                    CASE WHEN poly_open > 0 THEN ((poly_high - poly_open) / poly_open * 100) ELSE 0 END AS spike_pct,
                    CASE WHEN poly_open > 0 THEN ((poly_close - poly_open) / poly_open * 100) ELSE 0 END AS close_vs_open_pct,
                    CASE WHEN poly_open > 0 THEN ((poly_low - poly_open) / poly_open * 100) ELSE 0 END AS low_vs_open_pct,
                    (poly_high - poly_low) AS dollar_range,
                    CASE WHEN close_red = 'green' THEN TRUE ELSE FALSE END AS is_green,
                    ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                FROM historical_gappers
                WHERE ticker = $1
                  AND gap_value >= $2
                  AND date >= CURRENT_DATE - ($3 || ' days')::INTERVAL
            )
            SELECT
                COUNT(*) AS gaps_above_threshold,
                ROUND(AVG(gap_value)::NUMERIC, 1) AS avg_gap,
                MAX(CASE WHEN rn = 1 THEN poly_high END) AS prev_gap_high,
                MAX(CASE WHEN rn = 1 THEN poly_volume END) AS prev_gap_volume,
                ROUND(AVG(spike_pct)::NUMERIC, 1) AS avg_spike,
                ROUND(AVG(dollar_range)::NUMERIC, 2) AS avg_dollar_range,
                ROUND(AVG(CASE WHEN is_green THEN spike_pct END)::NUMERIC, 1) AS avg_high_pct,
                ROUND(AVG(CASE WHEN is_green THEN close_vs_open_pct END)::NUMERIC, 1) AS avg_close_green,
                SUM(CASE WHEN NOT is_green THEN 1 ELSE 0 END)::INTEGER AS close_red_count,
                ROUND(AVG(CASE WHEN NOT is_green THEN close_vs_open_pct END)::NUMERIC, 1) AS avg_close_red,
                ROUND(AVG(CASE WHEN NOT is_green THEN low_vs_open_pct END)::NUMERIC, 1) AS avg_low_pct
            FROM gap_days;
        """

        import time as _time
        _t0 = _time.monotonic()
        try:
            row = await self.db_service.fetch_one(query, ticker, min_gap_percent, str(days_back))
        except Exception as e:
            logger.error(f"[GAP-STATS] DB query failed for {ticker}: {e}")
            return {"ticker": ticker, "gaps_above_threshold": 0, "threshold": min_gap_percent, "days_back": days_back}
        _elapsed = (_time.monotonic() - _t0) * 1000
        logger.info(f"[GAP-STATS] {ticker} query took {_elapsed:.1f}ms")

        if not row or row['gaps_above_threshold'] == 0:
            return {
                "ticker": ticker,
                "gaps_above_threshold": 0,
                "threshold": min_gap_percent,
                "days_back": days_back
            }

        total = row['gaps_above_threshold']
        red_count = row['close_red_count'] or 0
        logger.info(f"[GAP-STATS] {ticker}: {total} gaps >= {min_gap_percent}%, avg_gap={row['avg_gap']}, red={red_count}")

        return {
            "ticker": ticker,
            "gaps_above_threshold": total,
            "avg_gap": float(row['avg_gap']) if row['avg_gap'] is not None else None,
            "prev_gap_high": float(row['prev_gap_high']) if row['prev_gap_high'] is not None else None,
            "prev_gap_volume": int(row['prev_gap_volume']) if row['prev_gap_volume'] is not None else None,
            "avg_spike": float(row['avg_spike']) if row['avg_spike'] is not None else None,
            "avg_dollar_range": float(row['avg_dollar_range']) if row['avg_dollar_range'] is not None else None,
            "avg_high_pct": float(row['avg_high_pct']) if row['avg_high_pct'] is not None else None,
            "avg_close_green": float(row['avg_close_green']) if row['avg_close_green'] is not None else None,
            "close_red_count": red_count,
            "close_red_pct": round(red_count / total * 100, 1) if total > 0 else None,
            "avg_close_red": float(row['avg_close_red']) if row['avg_close_red'] is not None else None,
            "avg_low_pct": float(row['avg_low_pct']) if row['avg_low_pct'] is not None else None,
            "threshold": min_gap_percent,
            "days_back": days_back
        }

    async def get_gap_stats_batch(
        self,
        tickers: List[str],
        min_gap_percent: float = 20.0,
        days_back: int = 365
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get gap statistics for multiple tickers in a single query.
        Returns dict keyed by ticker.
        """
        if not tickers:
            return {}

        query = """
            WITH gap_days AS (
                SELECT
                    ticker,
                    date, gap_value, poly_open, poly_high, poly_low, poly_close, poly_volume,
                    CASE WHEN poly_open > 0 THEN ((poly_high - poly_open) / poly_open * 100) ELSE 0 END AS spike_pct,
                    CASE WHEN poly_open > 0 THEN ((poly_close - poly_open) / poly_open * 100) ELSE 0 END AS close_vs_open_pct,
                    CASE WHEN poly_open > 0 THEN ((poly_low - poly_open) / poly_open * 100) ELSE 0 END AS low_vs_open_pct,
                    (poly_high - poly_low) AS dollar_range,
                    CASE WHEN close_red = 'green' THEN TRUE ELSE FALSE END AS is_green,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM historical_gappers
                WHERE ticker = ANY($1::text[])
                  AND gap_value >= $2
                  AND date >= CURRENT_DATE - ($3 || ' days')::INTERVAL
            )
            SELECT
                ticker,
                COUNT(*) AS gaps_above_threshold,
                ROUND(AVG(gap_value)::NUMERIC, 1) AS avg_gap,
                MAX(CASE WHEN rn = 1 THEN poly_high END) AS prev_gap_high,
                MAX(CASE WHEN rn = 1 THEN poly_volume END) AS prev_gap_volume,
                ROUND(AVG(spike_pct)::NUMERIC, 1) AS avg_spike,
                ROUND(AVG(dollar_range)::NUMERIC, 2) AS avg_dollar_range,
                ROUND(AVG(CASE WHEN is_green THEN spike_pct END)::NUMERIC, 1) AS avg_high_pct,
                ROUND(AVG(CASE WHEN is_green THEN close_vs_open_pct END)::NUMERIC, 1) AS avg_close_green,
                SUM(CASE WHEN NOT is_green THEN 1 ELSE 0 END)::INTEGER AS close_red_count,
                ROUND(AVG(CASE WHEN NOT is_green THEN close_vs_open_pct END)::NUMERIC, 1) AS avg_close_red,
                ROUND(AVG(CASE WHEN NOT is_green THEN low_vs_open_pct END)::NUMERIC, 1) AS avg_low_pct
            FROM gap_days
            GROUP BY ticker;
        """

        import time as _time
        _t0 = _time.monotonic()
        try:
            rows = await self.db_service.fetch_all(query, tickers, min_gap_percent, str(days_back))
        except Exception as e:
            logger.error(f"[GAP-STATS-BATCH] DB query failed for {len(tickers)} tickers: {e}")
            return {}
        _elapsed = (_time.monotonic() - _t0) * 1000
        logger.info(f"[GAP-STATS-BATCH] Query for {len(tickers)} tickers took {_elapsed:.1f}ms, got {len(rows or [])} results")

        result = {}
        for row in (rows or []):
            t = row['ticker']
            total = row['gaps_above_threshold']
            red_count = row['close_red_count'] or 0
            result[t] = {
                "ticker": t,
                "gaps_above_threshold": total,
                "avg_gap": float(row['avg_gap']) if row['avg_gap'] is not None else None,
                "prev_gap_high": float(row['prev_gap_high']) if row['prev_gap_high'] is not None else None,
                "prev_gap_volume": int(row['prev_gap_volume']) if row['prev_gap_volume'] is not None else None,
                "avg_spike": float(row['avg_spike']) if row['avg_spike'] is not None else None,
                "avg_dollar_range": float(row['avg_dollar_range']) if row['avg_dollar_range'] is not None else None,
                "avg_high_pct": float(row['avg_high_pct']) if row['avg_high_pct'] is not None else None,
                "avg_close_green": float(row['avg_close_green']) if row['avg_close_green'] is not None else None,
                "close_red_count": red_count,
                "close_red_pct": round(red_count / total * 100, 1) if total > 0 else None,
                "avg_close_red": float(row['avg_close_red']) if row['avg_close_red'] is not None else None,
                "avg_low_pct": float(row['avg_low_pct']) if row['avg_low_pct'] is not None else None,
                "threshold": min_gap_percent,
                "days_back": days_back
            }

        return result

    async def detect_multi_day_patterns(self, ticker: str) -> Optional[str]:
        """
        Detect multi-day patterns based on gap history
        Returns the detected pattern name or None
        """
        history = await self.get_gap_history(ticker, days_back=5)

        if len(history) < 2:
            return None

        # Check for Day 2 patterns
        if len(history) >= 2:
            today = history[0]
            yesterday = history[1]

            # Day 2 Dump pattern
            if (yesterday['gap_percent'] > 20 and
                today['fade_percent'] and today['fade_percent'] > 10):
                return 'DAY_2_DUMP'

            # Day 2 Fade pattern
            if (yesterday['gap_percent'] > 15 and
                today['fade_percent'] and today['fade_percent'] > 8):
                return 'DAY_2_FADE'

        # Check for Day 3 patterns
        if len(history) >= 3:
            today = history[0]
            day2 = history[1]
            day3 = history[2]

            # Day 3 Breakdown
            if (day3['gap_percent'] > 15 and
                today['fade_percent'] and today['fade_percent'] > 15):
                return 'DAY_3_BREAKDOWN'

            # Day 3 Fade
            if (day3['gap_percent'] > 10 and
                today['fade_percent'] and today['fade_percent'] > 5):
                return 'DAY_3_FADE'

        return None

    async def _get_active_tickers(self) -> List[str]:
        """Get list of active tickers from database"""
        query = """
            SELECT DISTINCT ticker
            FROM companies
            WHERE is_active = true
            AND ticker NOT LIKE '%.%'  -- Exclude special characters
            AND LENGTH(ticker) <= 5    -- Reasonable ticker length
            ORDER BY ticker
        """

        rows = await self.db_service.fetch_all(query)
        return [row['ticker'] for row in rows] if rows else []

    async def update_monthly_summary(self, year: int, month: int):
        """Update monthly summary table for faster aggregations"""
        query = """
            INSERT INTO gap_monthly_summary (
                ticker, year, month,
                total_gaps, positive_gaps, negative_gaps,
                avg_gap_percent, max_gap_percent, min_gap_percent,
                last_updated
            )
            SELECT
                ticker,
                EXTRACT(YEAR FROM date)::INTEGER as year,
                EXTRACT(MONTH FROM date)::INTEGER as month,
                COUNT(*) as total_gaps,
                SUM(CASE WHEN gap_value > 0 THEN 1 ELSE 0 END) as positive_gaps,
                SUM(CASE WHEN gap_value < 0 THEN 1 ELSE 0 END) as negative_gaps,
                AVG(gap_value) as avg_gap_percent,
                MAX(gap_value) as max_gap_percent,
                MIN(gap_value) as min_gap_percent,
                CURRENT_TIMESTAMP
            FROM historical_gappers
            WHERE EXTRACT(YEAR FROM date) = $1
            AND EXTRACT(MONTH FROM date) = $2
            GROUP BY ticker, year, month
            ON CONFLICT (ticker, year, month)
            DO UPDATE SET
                total_gaps = EXCLUDED.total_gaps,
                positive_gaps = EXCLUDED.positive_gaps,
                negative_gaps = EXCLUDED.negative_gaps,
                avg_gap_percent = EXCLUDED.avg_gap_percent,
                max_gap_percent = EXCLUDED.max_gap_percent,
                min_gap_percent = EXCLUDED.min_gap_percent,
                last_updated = CURRENT_TIMESTAMP
        """

        await self.db_service.execute(query, year, month)
        self.logger.info(f"Updated monthly summary for {year}-{month:02d}")

    async def close(self):
        """Close database connections"""
        await self.db_service.close()