"""Database Service for saving scanner results and price history"""

import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, time, date, timezone
import asyncpg
import json
import pytz
from .models import ScanResult
from .settings import DatabaseSettings

logger = logging.getLogger(__name__)

# Enable SQL debug logging with environment variable
SQL_DEBUG = os.getenv("SQL_DEBUG", "false").lower() == "true"


class DatabaseService:
    """Service for saving scanner results and price data to PostgreSQL"""

    # Price tracking throttle: minimum seconds between snapshots per ticker
    PRICE_SNAPSHOT_THROTTLE_SECONDS = 300  # 5 minutes
    _last_snapshot_times = {}  # ticker -> timestamp

    # Market holidays checked dynamically via MarketStatusChecker (Polygon API)
    # Fallback: weekend check only if API unavailable

    def __init__(self):
        self.settings = DatabaseSettings()
        self.conn_pool: Optional[asyncpg.Pool] = None

    async def init(self):
        """Initialize database connection pool"""
        try:
            # Optimized pool sizing for multiple microservices:
            # - PostgreSQL max_connections = 100
            # - ~15 microservices with DB access
            # - max_size=5 gives ~75 connections max (25 reserved for admin/maintenance)
            # - min_size=1 reduces idle connections during low traffic
            # - During rolling restarts, this prevents "too many clients" errors
            self.conn_pool = await asyncpg.create_pool(
                host=self.settings.db_host,
                port=self.settings.db_port,
                user=self.settings.db_user,
                password=self.settings.db_password,
                database=self.settings.db_name,
                min_size=1,  # Reduced from 2 to save idle connections
                max_size=5,  # Reduced from 10 to prevent connection exhaustion
                timeout=60,
                command_timeout=60
            )
            logger.info("[DB] Connection pool initialized (min=1, max=5)")
        except Exception as e:
            logger.error(f"[DB] Failed to initialize connection pool: {e}", exc_info=True)
            raise

    async def close(self):
        """Close database connection pool"""
        if self.conn_pool:
            await self.conn_pool.close()
            logger.info("[DB] Connection pool closed")

    async def _ensure_connection(self):
        """Ensure connection pool is healthy, reconnect if needed"""
        if not self.conn_pool:
            logger.warning("[DB] Connection pool not initialized, initializing now...")
            await self.init()
            return

        # Try to acquire a connection to test if pool is healthy
        try:
            async with self.conn_pool.acquire(timeout=5) as conn:
                await conn.fetchval("SELECT 1")
        except Exception as e:
            logger.error(f"[DB] Connection pool unhealthy: {e}, reconnecting...", exc_info=True)
            try:
                await self.conn_pool.close()
            except Exception as close_error:
                logger.debug(f"[DB] Error closing unhealthy pool (expected): {close_error}")
            self.conn_pool = None
            await self.init()

    async def execute(self, query: str, *args, retry=True):
        """Execute a query with parameters and auto-retry on connection errors"""
        if not self.conn_pool:
            await self._ensure_connection()

        try:
            # Debug SQL if enabled
            if SQL_DEBUG:
                logger.info(f"[SQL DEBUG] Query: {query}")
                logger.info(f"[SQL DEBUG] Params: {args}")

            async with self.conn_pool.acquire() as conn:
                result = await conn.execute(query, *args)

                if SQL_DEBUG:
                    logger.info(f"[SQL DEBUG] Result: {result}")
                else:
                    logger.debug(f"[DB] Executed query: {query[:50]}...")
                return result
        except (asyncpg.exceptions.ConnectionDoesNotExistError,
                asyncpg.exceptions.InterfaceError,
                asyncpg.exceptions.ConnectionFailureError) as e:
            logger.warning(f"[DB] Connection error: {e}")
            if retry:
                logger.info("[DB] Retrying after reconnection...")
                await self._ensure_connection()
                return await self.execute(query, *args, retry=False)
            raise
        except Exception as e:
            logger.error(f"[DB] Error executing query: {e}", exc_info=True)
            raise

    async def fetch_one(self, query: str, *args, retry=True):
        """Fetch a single row from query with auto-retry on connection errors"""
        if not self.conn_pool:
            await self._ensure_connection()

        try:
            if SQL_DEBUG:
                logger.info(f"[SQL DEBUG] Fetch one: {query}")
                logger.info(f"[SQL DEBUG] Params: {args}")

            async with self.conn_pool.acquire() as conn:
                row = await conn.fetchrow(query, *args)
                if SQL_DEBUG and row:
                    logger.info(f"[SQL DEBUG] Result: {dict(row)}")
                if row:
                    return dict(row)
                return None
        except (asyncpg.exceptions.ConnectionDoesNotExistError,
                asyncpg.exceptions.InterfaceError,
                asyncpg.exceptions.ConnectionFailureError) as e:
            logger.warning(f"[DB] Connection error in fetch_one: {e}")
            if retry:
                logger.info("[DB] Retrying after reconnection...")
                await self._ensure_connection()
                return await self.fetch_one(query, *args, retry=False)
            return None
        except Exception as e:
            logger.error(f"[DB] Error fetching row: {e}", exc_info=True)
            return None

    async def fetch_all(self, query: str, *args, retry=True):
        """Fetch all rows from query with auto-retry on connection errors"""
        if not self.conn_pool:
            await self._ensure_connection()

        try:
            if SQL_DEBUG:
                logger.info(f"[SQL DEBUG] Fetch all: {query}")
                logger.info(f"[SQL DEBUG] Params: {args}")

            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                result = [dict(row) for row in rows] if rows else []

                if SQL_DEBUG:
                    logger.info(f"[SQL DEBUG] Result count: {len(result)}")
                    if result and len(result) <= 3:
                        logger.info(f"[SQL DEBUG] Sample results: {result}")

                return result
        except (asyncpg.exceptions.ConnectionDoesNotExistError,
                asyncpg.exceptions.InterfaceError,
                asyncpg.exceptions.ConnectionFailureError) as e:
            logger.warning(f"[DB] Connection error in fetch_all: {e}")
            if retry:
                logger.info("[DB] Retrying after reconnection...")
                await self._ensure_connection()
                return await self.fetch_all(query, *args, retry=False)
            return []
        except Exception as e:
            logger.error(f"[DB] Error fetching rows: {e}", exc_info=True)
            return []

    async def fetch(self, query: str, *args, **kwargs):
        """Alias for fetch_all (asyncpg compat)"""
        return await self.fetch_all(query, *args, **kwargs)

    async def save_scan_results(self, scanner_type: str, results: List[ScanResult]):
        """Save scanner results to scan_history table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving scan results")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                async with conn.transaction():
                    # Prepare data for batch insert
                    values = []
                    scan_timestamp = datetime.now(timezone.utc)

                    for result in results:
                        metadata = {
                            'high_spike_percent': result.high_spike_percent,
                            'low_bounce_percent': result.low_bounce_percent,
                            'change_5m': result.change_5m,
                            'volume_premarket': result.volume_premarket,
                            'prev_close': result.prev_close,
                            'day_high': getattr(result, 'day_high', None),
                            'day_low': getattr(result, 'day_low', None),
                            'exchange': getattr(result, 'exchange', None),
                            'market_cap': getattr(result, 'market_cap_millions', None),
                            'shares_outstanding': getattr(result, 'dt_outstanding_shares', getattr(result, 'shares_outstanding', None)),
                            'scanner_timestamp': scan_timestamp.isoformat()
                        }

                        # Remove None values from metadata
                        metadata = {k: v for k, v in metadata.items() if v is not None}

                        values.append((
                            scan_timestamp,
                            scanner_type,
                            result.ticker,
                            result.price,
                            result.gap_percent,
                            result.change_percent,
                            self._parse_volume(result.volume),
                            result.float_shares,
                            result.momentum_score,
                            result.breakout_signal,
                            getattr(result, 'pattern', None),
                            json.dumps(metadata)
                        ))

                    # Batch insert with transaction
                    await conn.executemany(
                        """
                        INSERT INTO scan_history (
                            scan_timestamp, scanner_type, ticker, price,
                            gap_percent, change_percent, volume, float_shares,
                            momentum_score, breakout_signal, pattern_type, metadata
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        """,
                        values
                    )

                    logger.info(f"[DB] Saved {len(values)} {scanner_type} scan results to database")

        except Exception as e:
            logger.error(f"[DB] Error saving scan results: {e}", exc_info=True)

    async def save_price_snapshot(self, ticker: str, price_data: Dict[str, Any], force: bool = False):
        """
        Save price snapshot to price_history table with intelligent throttling

        Args:
            ticker: Stock ticker symbol
            price_data: Price data dict with keys: price, volume, high, low, open, close, vwap, change_5m, change_15m
            force: If True, bypass throttling (use for critical price points)

        Returns:
            bool: True if snapshot was saved, False if throttled
        """
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving price snapshot")
            return False

        # Throttling check (unless forced)
        if not force:
            now = datetime.now(timezone.utc).timestamp()
            last_snapshot = self._last_snapshot_times.get(ticker, 0)

            if now - last_snapshot < self.PRICE_SNAPSHOT_THROTTLE_SECONDS:
                # Too soon, skip this snapshot
                return False

            # Update last snapshot time
            self._last_snapshot_times[ticker] = now

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO price_history (
                        ticker, timestamp, price, volume,
                        high, low, open, close, vwap,
                        change_5m, change_15m
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    ticker.upper(),
                    datetime.now(timezone.utc),
                    price_data.get('price'),
                    price_data.get('volume'),
                    price_data.get('high'),
                    price_data.get('low'),
                    price_data.get('open'),
                    price_data.get('close'),
                    price_data.get('vwap'),
                    price_data.get('change_5m'),
                    price_data.get('change_15m')
                )

                logger.debug(f"[PRICE_TRACKING] Saved snapshot for {ticker} @ ${price_data.get('price')}")
                return True

        except Exception as e:
            logger.error(f"[DB] Error saving price snapshot for {ticker}: {e}", exc_info=True)
            return False

    async def save_alert(self, alert_data: Dict[str, Any]):
        """Save or update alert in alerts table with new metadata fields"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving alert")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                # Prepare metadata
                metadata = {
                    'scanner_type': alert_data.get('scanner_type'),
                    'trigger_reason': alert_data.get('trigger_reason'),
                    'technical_setup': alert_data.get('technical_setup'),
                    'risk_level': alert_data.get('risk_level'),
                    'additional_notes': alert_data.get('notes')
                }

                # Remove None values
                metadata = {k: v for k, v in metadata.items() if v is not None}

                result = await conn.fetchrow(
                    """
                    INSERT INTO alerts (
                        ticker, alert_type, price, message,
                        float_shares, market_cap,
                        gap_percent, volume_ratio, volume, alert_metadata,
                        acknowledged
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                    """,
                    alert_data['ticker'],
                    alert_data['alert_type'],
                    alert_data['price'],
                    alert_data['message'],
                    alert_data.get('float_shares'),
                    alert_data.get('market_cap'),
                    alert_data.get('gap_percent'),
                    alert_data.get('volume_ratio'),
                    alert_data.get('volume'),
                    json.dumps(metadata),
                    False  # New alerts start unacknowledged
                )

                alert_id = result['id']
                logger.info(f"[DB] Saved alert {alert_id} for {alert_data['ticker']}")
                return alert_id

        except Exception as e:
            logger.error(f"[DB] Error saving alert: {e}", exc_info=True)
            return None

    async def acknowledge_alert(self, alert_id: int):
        """Mark an alert as acknowledged"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE alerts
                    SET acknowledged = TRUE, acknowledged_at = NOW()
                    WHERE id = $1
                    """,
                    alert_id
                )
                logger.info(f"[DB] Acknowledged alert {alert_id}")
        except Exception as e:
            logger.error(f"[DB] Error acknowledging alert {alert_id}: {e}", exc_info=True)

    async def get_active_alerts(self, limit: int = 50):
        """Get active unacknowledged alerts"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, ticker, alert_type, price, percentage_change,
                           volume, message, session_type,
                           metadata, created_at, float_shares, market_cap,
                           gap_percent, volume_ratio, alert_metadata,
                           acknowledged, acknowledged_at, breaking_count, price_sensitive_count
                    FROM alerts
                    WHERE acknowledged = FALSE
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit
                )

                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"[DB] Error getting active alerts: {e}", exc_info=True)
            return []

    async def get_scan_history(self, ticker: str = None, scanner_type: str = None, days: int = 7):
        """Get historical scan results"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                query = """
                    SELECT id, scan_timestamp, scanner_type, ticker, price,
                           gap_percent, change_percent, volume, float_shares,
                           momentum_score, breakout_signal, pattern_type,
                           metadata, created_at
                    FROM scan_history
                    WHERE scan_timestamp > NOW() - ($1 || ' days')::INTERVAL
                """
                params = [str(days)]

                if ticker:
                    query += " AND ticker = $2"
                    params.append(ticker)
                if scanner_type:
                    param_num = len(params) + 1
                    query += f" AND scanner_type = ${param_num}"
                    params.append(scanner_type)

                query += " ORDER BY scan_timestamp DESC"

                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Error getting scan history: {e}", exc_info=True)
            return []

    async def get_price_history(self, ticker: str, hours: int = 24):
        """Get price history for a ticker"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, ticker, timestamp, price, volume, high, low,
                           open, close, vwap, change_5m, change_15m, created_at
                    FROM price_history
                    WHERE ticker = $1
                    AND timestamp > NOW() - ($2 || ' hours')::INTERVAL
                    ORDER BY timestamp DESC
                    """,
                    ticker, str(hours)
                )

                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"[DB] Error getting price history for {ticker}: {e}", exc_info=True)
            return []

    def _parse_volume(self, volume_str: str) -> Optional[int]:
        """Parse volume string to integer"""
        if not volume_str:
            return None

        try:
            # Remove commas and convert
            volume_str = str(volume_str).replace(',', '').replace('M', '000000').replace('K', '000')

            # Handle decimal millions (e.g., "1.5M")
            if '.' in volume_str and volume_str.endswith('000000'):
                base = volume_str.replace('000000', '')
                return int(float(base) * 1000000)

            return int(float(volume_str))
        except (ValueError, TypeError) as e:
            logger.debug(f"[DB] Could not parse volume '{volume_str}': {e}")
            return None

    async def get_float_shares(self, ticker: str) -> Optional[float]:
        """
        Get float shares for a ticker (in millions)

        Priority:
        1. dt_float (DilutionTracker - more reliable)
        2. float_shares (Polygon - fallback)
        """
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return None

        try:
            async with self.conn_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT dt_float, float_shares FROM companies WHERE UPPER(ticker) = UPPER($1)",
                    ticker
                )

                if not row:
                    return None

                # Priority 1: DilutionTracker float (more reliable)
                if row['dt_float'] and row['dt_float'] > 0:
                    # dt_float is stored in raw shares, convert to millions
                    return row['dt_float'] / 1_000_000

                # Priority 2: Polygon float (fallback)
                if row['float_shares'] and row['float_shares'] > 0:
                    # Convert from raw shares to millions
                    return row['float_shares'] / 1_000_000

                return None

        except Exception as e:
            logger.error(f"[DB] Error getting float shares for {ticker}: {e}", exc_info=True)
            return None

    async def get_multiple_float_shares(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        """
        Get float shares for multiple tickers in one query (in millions)

        Priority:
        1. dt_float (DilutionTracker - more reliable)
        2. float_shares (Polygon - fallback)
        """
        if not tickers or not self.conn_pool:
            return {}

        try:
            # Convert all tickers to uppercase for query
            tickers_upper = [t.upper() for t in tickers]

            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT ticker, dt_float, float_shares
                    FROM companies
                    WHERE ticker = ANY($1::text[])
                    """,
                    tickers_upper
                )

                # FIX: Build case-insensitive lookup map (O(1) instead of O(n²))
                ticker_case_map = {t.upper(): t for t in tickers}

                # Build result dict with original case
                result = {ticker: None for ticker in tickers}

                # O(n) loop instead of O(n²)
                for row in rows:
                    original_ticker = ticker_case_map.get(row['ticker'])
                    if original_ticker:
                        # Priority 1: DilutionTracker float (more reliable)
                        if row['dt_float'] and row['dt_float'] > 0:
                            # dt_float is stored in raw shares, convert to millions
                            result[original_ticker] = row['dt_float'] / 1_000_000
                        # Priority 2: Polygon float (fallback)
                        elif row['float_shares'] and row['float_shares'] > 0:
                            # Check if value is already in millions or raw shares
                            # If value < 1000, assume it's already in millions
                            # If value >= 1000, assume it's raw shares and convert to millions
                            float_val = row['float_shares']
                            if float_val >= 1000:
                                result[original_ticker] = float_val / 1_000_000
                            else:
                                # Already in millions
                                result[original_ticker] = float_val

                return result

        except Exception as e:
            logger.error(f"[DB] Error getting float shares for multiple tickers: {e}", exc_info=True)
            return {}

    async def get_scanner_enrichment_data(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Get enrichment data for scanner results (float, SI%, ownership, country) in one query.

        Returns dict with ticker as key and dict of:
        - float_shares: float in millions
        - short_interest: percentage (e.g., 12.5 = 12.5%)
        - institutional_ownership: percentage
        - insider_ownership: percentage
        - country_code: two-letter country code (e.g., 'US', 'CN', 'HK')
        """
        if not tickers or not self.conn_pool:
            return {}

        try:
            tickers_upper = [t.upper() for t in tickers]

            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT ticker, dt_float, float_shares,
                           dt_short_interest, institutional_ownership, insider_ownership,
                           COALESCE(country_code, 'US') as country_code
                    FROM companies
                    WHERE ticker = ANY($1::text[])
                    """,
                    tickers_upper
                )

                # Build case-insensitive lookup map
                ticker_case_map = {t.upper(): t for t in tickers}

                # Build result dict
                result = {}
                for row in rows:
                    original_ticker = ticker_case_map.get(row['ticker'])
                    if original_ticker:
                        # Calculate float in millions
                        float_val = None
                        if row['dt_float'] and row['dt_float'] > 0:
                            float_val = row['dt_float'] / 1_000_000
                        elif row['float_shares'] and row['float_shares'] > 0:
                            fv = row['float_shares']
                            float_val = fv / 1_000_000 if fv >= 1000 else fv

                        result[original_ticker] = {
                            'float_shares': float_val,
                            'short_interest': float(row['dt_short_interest']) if row['dt_short_interest'] else None,
                            'institutional_ownership': float(row['institutional_ownership']) if row['institutional_ownership'] else None,
                            'insider_ownership': float(row['insider_ownership']) if row['insider_ownership'] else None,
                            'country_code': row['country_code'] or 'US',
                        }

                return result

        except Exception as e:
            logger.error(f"[DB] Error getting scanner enrichment data: {e}", exc_info=True)
            return {}

    async def save_ticker_history(self, date: datetime, ticker: str, scanner_type: str,
                                 reason: str, gap_rth: float = None, gap_premarket: float = None,
                                 price: float = None, volume: int = None):
        """Save or update ticker history for a specific date"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving ticker history")
            return

        # Market hours validation
        # Convert to ET timezone for market hours check
        et_tz = pytz.timezone('US/Eastern')
        if date.tzinfo is None:
            # If no timezone, assume UTC and convert to ET
            date_et = pytz.utc.localize(date).astimezone(et_tz)
        else:
            date_et = date.astimezone(et_tz)

        # Check if it's a trading day (weekend + holiday check via Polygon API)
        try:
            from .market_status import get_market_checker
            if not await get_market_checker().is_trading_day(date_et.date()):
                logger.info(f"[DB] Skipping ticker history save for {ticker} - not a trading day ({date_et.strftime('%Y-%m-%d')})")
                return
        except Exception:
            # Fallback: basic weekend check if API unavailable
            if date_et.weekday() >= 5:
                logger.info(f"[DB] Skipping ticker history save for {ticker} - weekend ({date_et.strftime('%A')})")
                return

        # Check if within market hours (4 AM ET premarket to 8 PM ET afterhours)
        current_time = date_et.time()
        market_open = time(4, 0)  # 4 AM ET premarket start
        market_close = time(20, 0)  # 8 PM ET afterhours end

        if current_time < market_open or current_time > market_close:
            logger.info(f"[DB] Skipping ticker history save for {ticker} - outside market hours ({current_time.strftime('%H:%M')} ET)")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ticker_history (
                        date, ticker, scanner_type, reason,
                        gap_rth, gap_premarket, price, volume
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, ticker, scanner_type)
                    DO UPDATE SET
                        reason = EXCLUDED.reason,
                        gap_rth = EXCLUDED.gap_rth,
                        gap_premarket = EXCLUDED.gap_premarket,
                        price = EXCLUDED.price,
                        volume = EXCLUDED.volume,
                        timestamp = NOW()
                    """,
                    date.date(),
                    ticker,
                    scanner_type,
                    reason,
                    gap_rth,
                    gap_premarket,
                    price,
                    volume
                )
                logger.info(f"[DB] Saved ticker history for {ticker} on {date.date()}")

        except Exception as e:
            logger.error(f"[DB] Error saving ticker history: {e}", exc_info=True)

    async def upsert_company_data(self, ticker: str, company_name: str = None,
                                  sector: str = None, industry: str = None,
                                  market_cap: int = None, dt_outstanding_shares: int = None,
                                  float_shares: int = None, employees: int = None,
                                  headquarters: str = None, description: str = None,
                                  website: str = None, exchange: str = None,
                                  listing_date: str = None, country_code: str = None):
        """Save or update company data in the companies table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving company data")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                async with conn.transaction():
                    # Float shares should be stored as raw shares in database
                    # If value is less than 10000, it's likely already in millions and needs conversion
                    if float_shares and float_shares < 10000:
                        float_shares = int(float_shares * 1_000_000)

                    # Convert listing_date string to date object if needed
                    from datetime import datetime, date
                    listing_date_obj = None
                    if listing_date:
                        if isinstance(listing_date, str):
                            try:
                                # Try common date formats
                                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                                    try:
                                        listing_date_obj = datetime.strptime(listing_date.strip(), fmt).date()
                                        break
                                    except ValueError:
                                        continue
                            except Exception as e:
                                logger.warning(f"[DB] Could not parse listing_date '{listing_date}': {e}")
                                listing_date_obj = None
                        elif isinstance(listing_date, date):
                            listing_date_obj = listing_date

                    await conn.execute(
                        """
                        INSERT INTO companies (
                            ticker, company_name, sector, industry,
                            market_cap, dt_outstanding_shares, float_shares,
                            employees, headquarters, description,
                            website, exchange, listing_date, country_code,
                            is_active, created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, TRUE, NOW(), NOW())
                        ON CONFLICT (ticker)
                        DO UPDATE SET
                            company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
                            sector = COALESCE(EXCLUDED.sector, companies.sector),
                            industry = COALESCE(EXCLUDED.industry, companies.industry),
                            market_cap = COALESCE(EXCLUDED.market_cap, companies.market_cap),
                            dt_outstanding_shares = COALESCE(EXCLUDED.dt_outstanding_shares, companies.dt_outstanding_shares),
                            float_shares = COALESCE(EXCLUDED.float_shares, companies.float_shares),
                            employees = COALESCE(EXCLUDED.employees, companies.employees),
                            headquarters = COALESCE(EXCLUDED.headquarters, companies.headquarters),
                            description = COALESCE(EXCLUDED.description, companies.description),
                            website = COALESCE(EXCLUDED.website, companies.website),
                            exchange = COALESCE(EXCLUDED.exchange, companies.exchange),
                            listing_date = COALESCE(EXCLUDED.listing_date, companies.listing_date),
                            country_code = COALESCE(EXCLUDED.country_code, companies.country_code),
                            updated_at = NOW()
                        """,
                        ticker.upper(),
                        company_name,
                        sector,
                        industry,
                        market_cap,
                        dt_outstanding_shares,
                        float_shares,
                        employees,
                        headquarters,
                        description,
                        website,
                        exchange,
                        listing_date_obj,  # Use the converted date object
                        country_code
                    )
                    logger.info(f"[DB] Saved company data for {ticker}")

        except Exception as e:
            logger.error(f"[DB] Error saving company data for {ticker}: {e}", exc_info=True)

    async def get_ticker_history_by_date(self, date: datetime):
        """Get all tickers that appeared on a specific date"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, date, ticker, scanner_type, reason,
                           gap_rth, gap_premarket, price, volume,
                           timestamp, created_at
                    FROM ticker_history
                    WHERE date = $1
                    ORDER BY timestamp DESC
                    """,
                    date.date()
                )
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Error getting ticker history for {date.date()}: {e}", exc_info=True)
            return []

    async def get_ticker_history_summary(self, days: int = 30):
        """Get summary of ticker appearances by date for the last N days"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        date,
                        COUNT(*) as ticker_count,
                        COUNT(DISTINCT ticker) as unique_tickers,
                        ARRAY_AGG(DISTINCT scanner_type) as scanner_types
                    FROM ticker_history
                    WHERE date > CURRENT_DATE - $1::integer
                    GROUP BY date
                    ORDER BY date DESC
                    """,
                    days
                )
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Error getting ticker history summary: {e}", exc_info=True)
            return []

    async def cleanup_old_data(self, days_to_keep: int = 7):
        """Clean up old data from history tables"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for cleanup")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                # Clean price history
                result = await conn.execute(
                    """
                    DELETE FROM price_history
                    WHERE created_at < NOW() - ($1 || ' days')::INTERVAL
                    """,
                    str(days_to_keep)
                )
                logger.info(f"[DB] Cleaned up old price history records")

                # Clean scan history (keep longer, 30 days)
                result = await conn.execute(
                    """
                    DELETE FROM scan_history
                    WHERE created_at < NOW() - ($1 || ' days')::INTERVAL
                    """,
                    str(days_to_keep * 4)  # Keep scan history 4x longer
                )
                logger.info(f"[DB] Cleaned up old scan history records")

                # Clean ticker history (keep longer, 90 days)
                result = await conn.execute(
                    """
                    DELETE FROM ticker_history
                    WHERE created_at < NOW() - ($1 || ' days')::INTERVAL
                    """,
                    str(days_to_keep * 12)  # Keep ticker history 12x longer (90 days)
                )
                logger.info(f"[DB] Cleaned up old ticker history records")

        except Exception as e:
            logger.error(f"[DB] Error cleaning up old data: {e}", exc_info=True)

    # ===== NEW METHODS FOR POLYGON WEBSOCKET V2 =====

    async def connect(self):
        """Alias for init() - for compatibility with polygon-stream-service"""
        await self.init()

    async def save_price_tick(self, ticker: str, price: float, size: int,
                              timestamp: datetime, exchange: Optional[str],
                              conditions: list, tape: Optional[str]):
        """Save trade tick to price_ticks table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving price tick")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO price_ticks (
                        ticker, timestamp, price, size, exchange,
                        conditions, tape
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    ticker.upper(),
                    timestamp,
                    price,
                    size,
                    exchange,
                    json.dumps(conditions) if conditions else None,
                    tape
                )
                logger.debug(f"[DB] Saved price tick for {ticker} @ ${price}")

        except Exception as e:
            logger.error(f"[DB] Error saving price tick for {ticker}: {e}", exc_info=True)

    async def save_quote(self, ticker: str, bid_price: float, bid_size: int,
                         ask_price: float, ask_size: int, spread: float,
                         spread_percent: float, timestamp: datetime,
                         bid_exchange: Optional[str], ask_exchange: Optional[str],
                         tape: Optional[str]):
        """Save NBBO quote to quotes table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving quote")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO quotes (
                        ticker, timestamp, bid_price, bid_size, bid_exchange,
                        ask_price, ask_size, ask_exchange, spread, spread_percent, tape
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    ticker.upper(),
                    timestamp,
                    bid_price,
                    bid_size,
                    bid_exchange,
                    ask_price,
                    ask_size,
                    ask_exchange,
                    spread,
                    spread_percent,
                    tape
                )
                logger.debug(f"[DB] Saved quote for {ticker} bid=${bid_price} ask=${ask_price}")

        except Exception as e:
            logger.error(f"[DB] Error saving quote for {ticker}: {e}", exc_info=True)

    async def batch_insert_trades(self, trades: List[Dict[str, Any]]):
        """Batch insert multiple price ticks using PostgreSQL COPY (10x faster, async native)"""
        if not self.conn_pool or not trades:
            return

        try:
            # Filter out trades with null size (violates NOT NULL constraint)
            valid_trades = [t for t in trades if t.get('size') is not None]
            if len(valid_trades) < len(trades):
                skipped_count = len(trades) - len(valid_trades)
                logger.warning(f"[DB] Skipping {skipped_count} trades with null size")

            if not valid_trades:
                logger.warning(f"[DB] No valid trades to insert (all had null size)")
                return

            # Prepare records for COPY - must match table column order
            records = [
                (
                    trade['ticker'].upper(),
                    datetime.fromtimestamp(trade['timestamp'] / 1000),
                    trade['price'],
                    trade['size'],
                    trade.get('exchange'),
                    json.dumps(trade.get('conditions', [])) if trade.get('conditions') else None,
                    trade.get('tape')
                )
                for trade in valid_trades
            ]

            async with self.conn_pool.acquire() as conn:
                # COPY is async native, 10x faster than executemany, doesn't block event loop
                await conn.copy_records_to_table(
                    'price_ticks',
                    records=records,
                    columns=['ticker', 'timestamp', 'price', 'size', 'exchange', 'conditions', 'tape']
                )
                logger.info(f"[DB] COPY inserted {len(valid_trades)} price ticks")

        except Exception as e:
            logger.error(f"[DB] Error batch inserting trades: {e}", exc_info=True)
            raise

    async def batch_insert_quotes(self, quotes: List[Dict[str, Any]]):
        """Batch insert multiple quotes using PostgreSQL COPY (10x faster, async native)"""
        if not self.conn_pool or not quotes:
            return

        try:
            # Prepare records for COPY - must match table column order
            records = [
                (
                    quote['ticker'].upper(),
                    datetime.fromtimestamp(quote['timestamp'] / 1000),
                    quote['bid_price'],
                    quote['bid_size'],
                    quote.get('bid_exchange'),
                    quote['ask_price'],
                    quote['ask_size'],
                    quote.get('ask_exchange'),
                    quote['spread'],
                    quote['spread_percent'],
                    quote.get('tape')
                )
                for quote in quotes
            ]

            async with self.conn_pool.acquire() as conn:
                # COPY is async native, 10x faster than executemany, doesn't block event loop
                await conn.copy_records_to_table(
                    'quotes',
                    records=records,
                    columns=['ticker', 'timestamp', 'bid_price', 'bid_size', 'bid_exchange',
                             'ask_price', 'ask_size', 'ask_exchange', 'spread', 'spread_percent', 'tape']
                )
                logger.info(f"[DB] COPY inserted {len(quotes)} quotes")

        except Exception as e:
            logger.error(f"[DB] Error batch inserting quotes: {e}", exc_info=True)
            raise

    async def save_price_history(self, ticker: str, timestamp: datetime,
                                  price: float, volume: int, high: float,
                                  low: float, open_price: float, close: float,
                                  vwap: Optional[float]):
        """Save aggregate bar to price_history table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving price history")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO price_history (
                        ticker, timestamp, price, volume, high, low,
                        open, close, vwap
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    ticker.upper(),
                    timestamp,
                    price,
                    volume,
                    high,
                    low,
                    open_price,
                    close,
                    vwap
                )
                logger.debug(f"[DB] Saved price history for {ticker} @ ${price}")

        except Exception as e:
            logger.error(f"[DB] Error saving price history for {ticker}: {e}", exc_info=True)

    async def save_trading_halt(self, ticker: str, halt_timestamp: datetime,
                                 halt_status: str, halt_reason: str,
                                 tape: Optional[str],
                                 resume_timestamp: Optional[datetime] = None,
                                 halt_duration_seconds: Optional[int] = None):
        """Save trading halt event to trading_halts table"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for saving trading halt")
            return

        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO trading_halts (
                        ticker, halt_timestamp, resume_timestamp, halt_status,
                        halt_reason, halt_duration_seconds, tape
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    ticker.upper(),
                    halt_timestamp,
                    resume_timestamp,
                    halt_status,
                    halt_reason,
                    halt_duration_seconds,
                    tape
                )
                logger.info(f"[DB] Saved trading halt for {ticker} - {halt_status}")

        except Exception as e:
            logger.error(f"[DB] Error saving trading halt for {ticker}: {e}", exc_info=True)

    async def get_trading_halts(self, ticker: Optional[str] = None,
                                 active_only: bool = False,
                                 hours: int = 24):
        """Get trading halts from database"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available")
            return []

        try:
            async with self.conn_pool.acquire() as conn:
                query = """
                    SELECT id, ticker, halt_timestamp, resume_timestamp,
                           halt_status, halt_reason, halt_duration_seconds,
                           tape, created_at
                    FROM trading_halts
                    WHERE halt_timestamp > NOW() - ($1 || ' hours')::INTERVAL
                """
                params = [str(hours)]

                if ticker:
                    query += " AND ticker = $2"
                    params.append(ticker.upper())

                if active_only:
                    query += " AND resume_timestamp IS NULL AND halt_status = 'halted'"

                query += " ORDER BY halt_timestamp DESC"

                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"[DB] Error getting trading halts: {e}", exc_info=True)
            return []

    async def get_multiple_country_codes(self, tickers: List[str]) -> Dict[str, str]:
        """Get country codes for multiple tickers in one query (defaults to 'US' if not found)"""
        if not tickers or not self.conn_pool:
            return {}

        try:
            # Convert all tickers to uppercase for query
            tickers_upper = [t.upper() for t in tickers]

            async with self.conn_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT ticker, COALESCE(country_code, 'US') as country_code
                    FROM companies
                    WHERE ticker = ANY($1::text[])
                    """,
                    tickers_upper
                )

                # Build case-insensitive lookup map
                ticker_case_map = {t.upper(): t for t in tickers}

                # Build result dict with original case and default to 'US'
                result = {ticker: 'US' for ticker in tickers}

                for row in rows:
                    original_ticker = ticker_case_map.get(row['ticker'])
                    if original_ticker:
                        result[original_ticker] = row['country_code']

                return result

        except Exception as e:
            logger.error(f"[DB] Error getting country codes for multiple tickers: {e}", exc_info=True)
            return {}

    async def update_country_code(self, ticker: str, country_code: str) -> bool:
        """Update country code for a ticker"""
        if not self.conn_pool:
            logger.warning("[DB] No connection pool available for updating country code")
            return False

        try:
            async with self.conn_pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE companies
                    SET country_code = $2, updated_at = NOW()
                    WHERE UPPER(ticker) = UPPER($1)
                    """,
                    ticker, country_code.upper()
                )

                # If no row was updated, insert minimal record
                if result == 'UPDATE 0':
                    await conn.execute(
                        """
                        INSERT INTO companies (ticker, company_name, country_code)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (ticker) DO UPDATE
                        SET country_code = EXCLUDED.country_code, updated_at = NOW()
                        """,
                        ticker.upper(), ticker.upper(), country_code.upper()
                    )

                logger.debug(f"[DB] Updated country code for {ticker} to {country_code}")
                return True

        except Exception as e:
            logger.error(f"[DB] Error updating country code for {ticker}: {e}", exc_info=True)
            return False