"""
NHOD (New High of Day) Tracker
Tracks the true high of day including premarket, regular, and after-hours sessions
"""

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Tuple, Any
import asyncio
import pytz
from .polygon_base import PolygonBaseClient
from .redis_client import RedisClient
from .database_service import DatabaseService

logger = logging.getLogger(__name__)

# Define Eastern timezone for market hours
ET_TZ = pytz.timezone('America/New_York')


class NHODTracker:
    """Tracks true high of day for tickers including all sessions"""

    def __init__(self, polygon_client: PolygonBaseClient, redis_client: Optional[RedisClient] = None, database_client: Optional[DatabaseService] = None):
        self.polygon = polygon_client
        self.redis = redis_client
        self.database = database_client
        self.cache_ttl = 300  # Cache for 5 minutes
        self.nhod_cache = {}  # Local memory cache

    async def get_ticker_from_database(self, ticker: str, today: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """Query ticker tracking data from database first"""
        if not self.database:
            return None

        try:
            if today is None:
                today = datetime.now(ET_TZ).date()

            # Query ticker_tracking table for today's data
            query = """
                SELECT ticker, date, scanner_type, first_seen_at,
                       premarket_high, premarket_low,
                       regular_high, regular_low,
                       afterhours_high, afterhours_low,
                       day_high, day_low,
                       day_high_time, day_low_time,
                       day_high_session, day_low_session,
                       last_price, last_volume, last_updated,
                       gap_percent, float_shares, market_cap
                FROM ticker_tracking
                WHERE ticker = $1 AND date = $2 AND is_active = TRUE
            """

            result = await self.database.fetch_one(query, ticker.upper(), today)

            if result:
                logger.info(f"[NHOD] Found {ticker} in database with high={result['day_high']}, low={result['day_low']}")
                return dict(result)

            return None

        except Exception as e:
            logger.error(f"[NHOD] Database query failed for {ticker}: {e}")
            return None

    async def save_ticker_to_database(self, ticker: str, data: Dict[str, Any], scanner_type: str = "unknown") -> bool:
        """Save or update ticker tracking data in database"""
        if not self.database:
            return False

        try:
            today = datetime.now(ET_TZ).date()

            # Prepare the data for insertion/update
            query = """
                INSERT INTO ticker_tracking (
                    ticker, date, scanner_type, first_seen_at,
                    premarket_high, premarket_low,
                    regular_high, regular_low,
                    afterhours_high, afterhours_low,
                    day_high, day_low,
                    day_high_time, day_low_time,
                    day_high_session, day_low_session,
                    last_price, last_volume,
                    gap_percent, float_shares, market_cap
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    premarket_high = GREATEST(ticker_tracking.premarket_high, EXCLUDED.premarket_high),
                    premarket_low = LEAST(ticker_tracking.premarket_low, EXCLUDED.premarket_low),
                    regular_high = GREATEST(ticker_tracking.regular_high, EXCLUDED.regular_high),
                    regular_low = LEAST(ticker_tracking.regular_low, EXCLUDED.regular_low),
                    afterhours_high = GREATEST(ticker_tracking.afterhours_high, EXCLUDED.afterhours_high),
                    afterhours_low = LEAST(ticker_tracking.afterhours_low, EXCLUDED.afterhours_low),
                    day_high = GREATEST(ticker_tracking.day_high, EXCLUDED.day_high),
                    day_low = LEAST(ticker_tracking.day_low, EXCLUDED.day_low),
                    day_high_time = CASE WHEN EXCLUDED.day_high > ticker_tracking.day_high THEN EXCLUDED.day_high_time ELSE ticker_tracking.day_high_time END,
                    day_low_time = CASE WHEN EXCLUDED.day_low < ticker_tracking.day_low THEN EXCLUDED.day_low_time ELSE ticker_tracking.day_low_time END,
                    day_high_session = CASE WHEN EXCLUDED.day_high > ticker_tracking.day_high THEN EXCLUDED.day_high_session ELSE ticker_tracking.day_high_session END,
                    day_low_session = CASE WHEN EXCLUDED.day_low < ticker_tracking.day_low THEN EXCLUDED.day_low_session ELSE ticker_tracking.day_low_session END,
                    last_price = EXCLUDED.last_price,
                    last_volume = COALESCE(EXCLUDED.last_volume, ticker_tracking.last_volume),
                    last_updated = NOW()
            """

            # Extract values from data dict
            high_time_str = data.get('high_time', '')
            low_time_str = data.get('low_time', '')

            # Convert time strings to timestamps if they're not already
            high_time = None
            low_time = None
            if high_time_str:
                try:
                    high_time = datetime.strptime(f"{today} {high_time_str}", "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not parse high_time '{high_time_str}': {e}")
                    pass
            if low_time_str:
                try:
                    low_time = datetime.strptime(f"{today} {low_time_str}", "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not parse low_time '{low_time_str}': {e}")
                    pass

            await self.database.execute(
                query,
                ticker.upper(),
                today,
                scanner_type,
                datetime.now(ET_TZ),
                data.get('premarket_high', 0) or None,
                data.get('premarket_low', 0) or None,
                data.get('regular_high', 0) or None,
                data.get('regular_low', 0) or None,
                data.get('afterhours_high', 0) or None,
                data.get('afterhours_low', 0) or None,
                data.get('overall_high', 0),
                data.get('overall_low', 0),
                high_time,
                low_time,
                data.get('high_session', 'unknown'),
                data.get('low_session', 'unknown'),
                data.get('current_price', 0),
                data.get('volume'),
                data.get('gap_percent'),
                data.get('float_shares'),
                data.get('market_cap')
            )

            logger.info(f"[NHOD] Saved {ticker} to database: high={data.get('overall_high')}, low={data.get('overall_low')}")
            return True

        except Exception as e:
            logger.error(f"[NHOD] Failed to save {ticker} to database: {e}")
            return False

    async def get_true_nhod(self, ticker: str, use_cache: bool = True, save_to_db: bool = True) -> Dict[str, float]:
        """
        Get the true NHOD and NLOD for a ticker including all trading sessions

        Returns:
            {
                'overall_high': float,  # Highest price of entire day
                'premarket_high': float,  # Highest in premarket (4:00-9:30 ET)
                'regular_high': float,   # Highest in regular (9:30-16:00 ET)
                'afterhours_high': float, # Highest in after hours (16:00-20:00 ET)
                'overall_low': float,    # Lowest price of entire day
                'premarket_low': float,  # Lowest in premarket
                'regular_low': float,    # Lowest in regular
                'afterhours_low': float, # Lowest in after hours
                'current_price': float,   # Current price
                'nhod_percent': float,    # Percent below NHOD
                'nlod_percent': float,    # Percent above NLOD
                'high_time': str,        # Time when high was reached
                'low_time': str,         # Time when low was reached
                'high_session': str,     # Session where high occurred
                'low_session': str       # Session where low occurred
            }
        """

        # STEP 1: Check database first for existing tracking data
        db_data = None
        if self.database:
            db_data = await self.get_ticker_from_database(ticker)
            if db_data:
                logger.info(f"[NHOD] Found {ticker} in database - using as baseline")
                # Use database values as the source of truth
                # We'll update with current price below

        # Check cache first (only if not found in DB)
        if use_cache and not db_data:
            cache_key = f"nhod:{ticker}"

            # Check Redis cache - prioritize real-time data
            if self.redis:
                try:
                    # First check if we have fresh real-time data
                    realtime_key = f"nhod:realtime:{ticker}"
                    realtime_flag = await self.redis.get(realtime_key)

                    if realtime_flag:
                        # We have very fresh data, use the latest key
                        latest_key = f"nhod:latest:{ticker}"
                        latest_data = await self.redis.get(latest_key)
                        if latest_data:
                            logger.debug(f"[NHOD] Using fresh real-time Redis data for {ticker}")
                            return latest_data

                    # Fall back to standard cache key
                    cached = await self.redis.get(cache_key)
                    if cached:
                        logger.debug(f"[NHOD] Using cached data for {ticker}")
                        return cached
                except Exception as e:
                    logger.warning(f"[NHOD] Redis cache error: {e}")

            # Check memory cache
            if ticker in self.nhod_cache:
                cached_data, cached_time = self.nhod_cache[ticker]
                if datetime.now(ET_TZ) - cached_time < timedelta(seconds=self.cache_ttl):
                    logger.debug(f"[NHOD] Using memory cached data for {ticker}")
                    return cached_data

        # Fetch fresh data with multiple strategies
        try:
            # Get today's date in ET timezone
            today = datetime.now(ET_TZ).strftime("%Y-%m-%d")

            # If we have DB data, we can skip fetching historical bars
            # Just get current price for real-time update
            if db_data:
                bars = []  # Skip historical data
                # Get current price only
                try:
                    quote_url = f"/v1/last/stocks/{ticker}"
                    quote_data = await self.polygon._get(quote_url, {})
                    if quote_data and quote_data.get("last"):
                        current_price = quote_data["last"]["price"]
                        logger.info(f"[NHOD] Got current price for {ticker}: ${current_price}")

                        # Build result from DB data + current price
                        result = {
                            'overall_high': max(float(db_data.get('day_high', 0)), current_price),
                            'premarket_high': float(db_data.get('premarket_high', 0)) if db_data.get('premarket_high') else 0,
                            'regular_high': float(db_data.get('regular_high', 0)) if db_data.get('regular_high') else 0,
                            'afterhours_high': float(db_data.get('afterhours_high', 0)) if db_data.get('afterhours_high') else 0,
                            'overall_low': min(float(db_data.get('day_low', float('inf'))), current_price) if db_data.get('day_low') else current_price,
                            'premarket_low': float(db_data.get('premarket_low', 0)) if db_data.get('premarket_low') else 0,
                            'regular_low': float(db_data.get('regular_low', 0)) if db_data.get('regular_low') else 0,
                            'afterhours_low': float(db_data.get('afterhours_low', 0)) if db_data.get('afterhours_low') else 0,
                            'current_price': current_price,
                            'nhod_percent': round(((float(db_data.get('day_high', current_price)) - current_price) / float(db_data.get('day_high', current_price)) * 100) if db_data.get('day_high') else 0, 2),
                            'nlod_percent': round(((current_price - float(db_data.get('day_low', current_price))) / float(db_data.get('day_low', current_price)) * 100) if db_data.get('day_low') else 0, 2),
                            'high_time': db_data.get('day_high_time', '').strftime("%H:%M:%S") if db_data.get('day_high_time') else '',
                            'low_time': db_data.get('day_low_time', '').strftime("%H:%M:%S") if db_data.get('day_low_time') else '',
                            'high_session': db_data.get('day_high_session', 'unknown'),
                            'low_session': db_data.get('day_low_session', 'unknown'),
                            'session': db_data.get('day_high_session', 'unknown')
                        }

                        # Update database with new current price if needed
                        if save_to_db:
                            await self.save_ticker_to_database(ticker, result)

                        # Cache the result
                        if use_cache:
                            self.nhod_cache[ticker] = (result, datetime.now(ET_TZ))
                            if self.redis:
                                await self.redis.set(f"nhod:{ticker}", result, ttl=self.cache_ttl)

                        return result
                except Exception as e:
                    logger.warning(f"[NHOD] Failed to get current price for {ticker}: {e}")
                    # Fall through to normal processing

            # Strategy 1: Try to get comprehensive minute data (if no DB data)
            # Use 1-hour bars instead of 1-minute for efficiency (less data, same accuracy)
            url = f"/v2/aggs/ticker/{ticker}/range/1/hour/{today}/{today}"
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "premarket": "true",  # Include premarket data
                "afterhours": "true"  # Include afterhours data
            }

            data = await self.polygon._get(url, params)
            bars = data.get("results", []) if data else []

            # Strategy 2: If no minute data, try to get current snapshot for fallback
            current_snapshot = None
            if not bars:
                try:
                    snapshot_url = f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
                    snapshot_data = await self.polygon._get(snapshot_url, {})
                    if snapshot_data and snapshot_data.get("results"):
                        current_snapshot = snapshot_data["results"]
                        logger.info(f"[NHOD] Using snapshot data for {ticker} (no minute bars)")
                except Exception as e:
                    logger.warning(f"[NHOD] Failed to get snapshot for {ticker}: {e}")

            # Strategy 3: If still no data, try previous day + current quote
            if not bars and not current_snapshot:
                logger.warning(f"[NHOD] No aggregate data for {ticker}, trying fallback methods")
                return await self._get_fallback_nhod_data(ticker)

            # If we have snapshot but no bars, create synthetic data
            if not bars and current_snapshot:
                return self._create_nhod_from_snapshot(ticker, current_snapshot)

            logger.info(f"[NHOD] Processing {len(bars)} minute bars for {ticker}")

            # Initialize session data
            premarket_bars = []
            regular_bars = []
            afterhours_bars = []

            # Classify bars by session (FIXED: Use ET timezone)
            for bar in bars:
                timestamp = bar["t"] / 1000  # Convert from milliseconds
                # CRITICAL FIX: Use ET timezone explicitly
                dt = datetime.fromtimestamp(timestamp, tz=ET_TZ)
                hour = dt.hour
                minute = dt.minute
                time_decimal = hour + minute/60

                # ET times (now correctly using ET timezone)
                if time_decimal < 9.5:  # Before 9:30 AM ET
                    premarket_bars.append(bar)
                elif time_decimal >= 9.5 and time_decimal < 16:  # 9:30 AM - 4:00 PM ET
                    regular_bars.append(bar)
                else:  # After 4:00 PM ET
                    afterhours_bars.append(bar)

            # Calculate highs and lows for each session
            premarket_high = max([b["h"] for b in premarket_bars], default=0)
            regular_high = max([b["h"] for b in regular_bars], default=0)
            afterhours_high = max([b["h"] for b in afterhours_bars], default=0)
            overall_high = max([b["h"] for b in bars], default=0)

            # Calculate lows (use float('inf') as default to get minimum)
            premarket_low = min([b["l"] for b in premarket_bars], default=float('inf'))
            regular_low = min([b["l"] for b in regular_bars], default=float('inf'))
            afterhours_low = min([b["l"] for b in afterhours_bars], default=float('inf'))
            overall_low = min([b["l"] for b in bars], default=float('inf'))

            # Convert inf to 0 for consistency
            if premarket_low == float('inf'): premarket_low = 0
            if regular_low == float('inf'): regular_low = 0
            if afterhours_low == float('inf'): afterhours_low = 0
            if overall_low == float('inf'): overall_low = 0

            # Get current price (last bar's close)
            current_price = bars[-1]["c"] if bars else 0

            # LOG: Critical price data
            logger.info(f"[NHOD-PEPG] {ticker}: Current=${current_price:.2f} | Overall_H=${overall_high:.2f} | Overall_L=${overall_low:.2f}")
            logger.info(f"[NHOD-PEPG] {ticker}: PM(H=${premarket_high:.2f}/L=${premarket_low:.2f}) | RTH(H=${regular_high:.2f}/L=${regular_low:.2f})")

            # Find when and where the overall high occurred
            high_time = ""
            high_session = ""

            if overall_high > 0:
                high_bar = next((b for b in bars if b["h"] == overall_high), None)
                if high_bar:
                    dt = datetime.fromtimestamp(high_bar["t"] / 1000)
                    high_time = dt.strftime("%H:%M")
                    hour = dt.hour
                    minute = dt.minute
                    time_decimal = hour + minute/60

                    if time_decimal < 9.5:
                        high_session = "premarket"
                    elif time_decimal >= 9.5 and time_decimal < 16:
                        high_session = "regular"
                    else:
                        high_session = "afterhours"

            # Find when and where the overall low occurred
            low_time = ""
            low_session = ""

            if overall_low > 0:
                low_bar = next((b for b in bars if b["l"] == overall_low), None)
                if low_bar:
                    dt = datetime.fromtimestamp(low_bar["t"] / 1000)
                    low_time = dt.strftime("%H:%M")
                    hour = dt.hour
                    minute = dt.minute
                    time_decimal = hour + minute/60

                    if time_decimal < 9.5:
                        low_session = "premarket"
                    elif time_decimal >= 9.5 and time_decimal < 16:
                        low_session = "regular"
                    else:
                        low_session = "afterhours"

            # Calculate percent below NHOD and above NLOD
            nhod_percent = 0
            if overall_high > 0 and current_price > 0:
                nhod_percent = ((overall_high - current_price) / overall_high) * 100

            nlod_percent = 0
            if overall_low > 0 and current_price > 0:
                nlod_percent = ((current_price - overall_low) / overall_low) * 100

            logger.info(f"[NHOD-RETURN] {ticker}: Returning H=${overall_high:.2f} L=${overall_low:.2f} Current=${current_price:.2f}")

            result = {
                'overall_high': overall_high,
                'premarket_high': premarket_high,
                'regular_high': regular_high,
                'afterhours_high': afterhours_high,
                'overall_low': overall_low,
                'premarket_low': premarket_low,
                'regular_low': regular_low,
                'afterhours_low': afterhours_low,
                'current_price': current_price,
                'nhod_percent': round(nhod_percent, 2),
                'nlod_percent': round(nlod_percent, 2),
                'high_time': high_time,
                'low_time': low_time,
                'high_session': high_session,
                'low_session': low_session,
                # Legacy field for backward compatibility
                'session': high_session
            }

            # Save to database if requested
            if save_to_db and self.database:
                await self.save_ticker_to_database(ticker, result)

            # Cache the result
            if use_cache:
                # Memory cache
                self.nhod_cache[ticker] = (result, datetime.now(ET_TZ))

                # Redis cache
                if self.redis:
                    try:
                        await self.redis.set(cache_key, result, ttl=self.cache_ttl)
                    except Exception as e:
                        logger.warning(f"[NHOD] Failed to cache in Redis: {e}")

            logger.info(f"[NHOD] {ticker}: High=${overall_high:.2f} at {high_time} ({high_session}), Low=${overall_low:.2f} at {low_time} ({low_session}), Current=${current_price:.2f}, Below NHOD: {nhod_percent:.1f}%, Above NLOD: {nlod_percent:.1f}%")

            return result

        except Exception as e:
            logger.error(f"[NHOD] Error fetching data for {ticker}: {e}")
            return self._empty_nhod_data()

    def _empty_nhod_data(self) -> Dict[str, float]:
        """Return empty NHOD/NLOD data structure"""
        return {
            'overall_high': 0,
            'premarket_high': 0,
            'regular_high': 0,
            'afterhours_high': 0,
            'overall_low': 0,
            'premarket_low': 0,
            'regular_low': 0,
            'afterhours_low': 0,
            'current_price': 0,
            'nhod_percent': 0,
            'nlod_percent': 0,
            'high_time': '',
            'low_time': '',
            'high_session': '',
            'low_session': '',
            'session': ''  # Legacy field
        }

    async def _get_fallback_nhod_data(self, ticker: str) -> Dict[str, float]:
        """Fallback strategy using current quote + previous day data"""
        try:
            # Get current quote
            current_price = 0
            try:
                quote_url = f"/v1/last/stocks/{ticker}"
                quote_data = await self.polygon._get(quote_url, {})
                if quote_data and quote_data.get("last"):
                    current_price = quote_data["last"]["price"]
                    logger.info(f"[NHOD] Got current price for {ticker}: ${current_price}")
            except Exception as e:
                logger.warning(f"[NHOD] Failed to get current quote for {ticker}: {e}")

            # If we have a current price, use it as both high and low for now
            # This is better than having no data at all
            if current_price > 0:
                return {
                    'overall_high': current_price,
                    'premarket_high': 0,
                    'regular_high': current_price,
                    'afterhours_high': 0,
                    'overall_low': current_price,
                    'premarket_low': 0,
                    'regular_low': current_price,
                    'afterhours_low': 0,
                    'current_price': current_price,
                    'nhod_percent': 0,
                    'nlod_percent': 0,
                    'high_time': datetime.now(ET_TZ).strftime("%H:%M:%S"),
                    'low_time': datetime.now(ET_TZ).strftime("%H:%M:%S"),
                    'high_session': 'regular',
                    'low_session': 'regular',
                    'session': 'regular'
                }

        except Exception as e:
            logger.error(f"[NHOD] Fallback strategy failed for {ticker}: {e}")

        return self._empty_nhod_data()

    def _create_nhod_from_snapshot(self, ticker: str, snapshot: dict) -> Dict[str, float]:
        """Create NHOD data from snapshot when no minute bars available"""
        try:
            day_data = snapshot.get("day", {})
            current_price = 0

            # Try to get current price from multiple sources
            if snapshot.get("lastQuote", {}).get("p"):
                current_price = snapshot["lastQuote"]["p"]
            elif snapshot.get("lastTrade", {}).get("p"):
                current_price = snapshot["lastTrade"]["p"]
            elif day_data.get("c"):
                current_price = day_data["c"]

            # Use day data for highs/lows
            day_high = day_data.get("h", current_price)
            day_low = day_data.get("l", current_price)

            logger.info(f"[NHOD] Created NHOD from snapshot for {ticker}: H=${day_high} L=${day_low} C=${current_price}")

            return {
                'overall_high': day_high,
                'premarket_high': 0,  # No premarket data available
                'regular_high': day_high,
                'afterhours_high': 0,
                'overall_low': day_low,
                'premarket_low': 0,
                'regular_low': day_low,
                'afterhours_low': 0,
                'current_price': current_price,
                'nhod_percent': ((day_high - current_price) / day_high * 100) if day_high > 0 else 0,
                'nlod_percent': ((current_price - day_low) / day_low * 100) if day_low > 0 else 0,
                'high_time': datetime.now(ET_TZ).strftime("%H:%M:%S"),
                'low_time': datetime.now(ET_TZ).strftime("%H:%M:%S"),
                'high_session': 'regular',
                'low_session': 'regular',
                'session': 'regular'
            }

        except Exception as e:
            logger.error(f"[NHOD] Failed to create NHOD from snapshot for {ticker}: {e}")
            return self._empty_nhod_data()

    async def update_with_realtime_price(self, ticker: str, current_price: float, volume: int = 0) -> Dict[str, Any]:
        """
        Update NHOD tracker with a real-time price update
        Returns any triggered alerts (new highs/lows)
        """
        try:
            # Get existing NHOD data (from cache if available, but fetch fresh data periodically)
            # Force fresh data fetch every 5 minutes to get accurate session data
            force_refresh = False
            if ticker in self.nhod_cache:
                cached_data, cache_time = self.nhod_cache[ticker]
                if (datetime.now(ET_TZ) - cache_time).total_seconds() > 300:  # 5 minutes
                    force_refresh = True
                    logger.info(f"[NHOD] Cache expired for {ticker}, fetching fresh session data")

            nhod_data = await self.get_true_nhod(ticker, use_cache=not force_refresh, save_to_db=force_refresh)

            # Determine current session
            now = datetime.now(ET_TZ)
            hour = now.hour
            minute = now.minute
            time_decimal = hour + minute/60

            session = "regular"
            if time_decimal < 9.5:  # Before 9:30 AM
                session = "premarket"
            elif time_decimal >= 16:  # After 4:00 PM
                session = "afterhours"

            alerts_triggered = []
            data_updated = False

            # Check for new daily high
            current_overall_high = nhod_data.get('overall_high', 0)
            if current_price > current_overall_high:
                nhod_data['overall_high'] = current_price
                nhod_data['high_time'] = now.strftime("%H:%M:%S")
                nhod_data['high_session'] = session
                data_updated = True

                alerts_triggered.append({
                    "type": "NEW_HIGH_OF_DAY",
                    "ticker": ticker,
                    "price": current_price,
                    "previous_high": current_overall_high,
                    "session": session,
                    "timestamp": now.isoformat()
                })

                # Update session-specific high
                if session == "premarket":
                    nhod_data['premarket_high'] = current_price
                elif session == "regular":
                    nhod_data['regular_high'] = current_price
                elif session == "afterhours":
                    nhod_data['afterhours_high'] = current_price

            # Check for new daily low
            current_overall_low = nhod_data.get('overall_low', float('inf'))
            if current_overall_low == 0:  # Handle zero as "no data"
                current_overall_low = float('inf')

            if current_price < current_overall_low:
                nhod_data['overall_low'] = current_price
                nhod_data['low_time'] = now.strftime("%H:%M:%S")
                nhod_data['low_session'] = session
                data_updated = True

                alerts_triggered.append({
                    "type": "NEW_LOW_OF_DAY",
                    "ticker": ticker,
                    "price": current_price,
                    "previous_low": current_overall_low if current_overall_low != float('inf') else 0,
                    "session": session,
                    "timestamp": now.isoformat()
                })

                # Update session-specific low
                if session == "premarket":
                    nhod_data['premarket_low'] = current_price
                elif session == "regular":
                    nhod_data['regular_low'] = current_price
                elif session == "afterhours":
                    nhod_data['afterhours_low'] = current_price

            # Always update current price and percentages
            nhod_data['current_price'] = current_price

            # Recalculate percentages
            if nhod_data['overall_high'] > 0:
                nhod_data['nhod_percent'] = ((nhod_data['overall_high'] - current_price) / nhod_data['overall_high']) * 100

            if nhod_data['overall_low'] > 0:
                nhod_data['nlod_percent'] = ((current_price - nhod_data['overall_low']) / nhod_data['overall_low']) * 100

            # Update cache if data changed
            if data_updated or True:  # Always update for current price
                # Update memory cache
                self.nhod_cache[ticker] = (nhod_data, datetime.now(ET_TZ))

                # Update Redis cache if available - with shorter TTL for real-time updates
                if self.redis:
                    try:
                        # Primary cache key with date
                        cache_key = f"nhod:{ticker}:{datetime.now(ET_TZ).strftime('%Y-%m-%d')}"
                        # Use shorter TTL (60s) for real-time updates to ensure freshness
                        await self.redis.set(cache_key, nhod_data, ttl=60)

                        # Also set a "latest" key that other services can monitor
                        latest_key = f"nhod:latest:{ticker}"
                        await self.redis.set(latest_key, nhod_data, ttl=60)

                        # Set a "realtime" flag to indicate this data is fresh
                        realtime_key = f"nhod:realtime:{ticker}"
                        await self.redis.set(realtime_key, {"updated_at": datetime.now(ET_TZ).isoformat(), "session": session}, ttl=10)

                        logger.debug(f"[NHOD] Updated Redis cache for {ticker} with real-time data (TTL=60s)")
                    except Exception as e:
                        logger.warning(f"[NHOD] Failed to update Redis cache for {ticker}: {e}")

                # Save to database if data was updated (new high/low)
                if data_updated and self.database:
                    try:
                        await self.save_ticker_to_database(ticker, nhod_data, scanner_type="alerts")
                        logger.info(f"[NHOD] Saved {ticker} session data to database")
                    except Exception as e:
                        logger.error(f"[NHOD] Failed to save {ticker} to database: {e}")

            logger.debug(f"[NHOD] Updated {ticker} with real-time price ${current_price} in {session} session")

            return {
                "nhod_data": nhod_data,
                "alerts": alerts_triggered,
                "session": session,
                "updated": data_updated
            }

        except Exception as e:
            logger.error(f"[NHOD] Error updating real-time price for {ticker}: {e}")
            return {
                "nhod_data": await self.get_true_nhod(ticker, use_cache=True),
                "alerts": [],
                "session": "unknown",
                "updated": False
            }

    async def get_batch_nhod(self, tickers: list) -> Dict[str, Dict[str, float]]:
        """
        Get NHOD data for multiple tickers concurrently

        Args:
            tickers: List of ticker symbols

        Returns:
            Dictionary mapping ticker to NHOD data
        """
        tasks = [self.get_true_nhod(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)

        return {ticker: result for ticker, result in zip(tickers, results)}