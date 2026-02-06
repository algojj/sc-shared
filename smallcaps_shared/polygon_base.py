"""Base Polygon client for microservices with Circuit Breaker"""

import os
import httpx
import logging
from typing import Optional, List, Dict, Any

from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitBreakerAlert,
    AlertType
)

logger = logging.getLogger(__name__)

# Default Polygon circuit breaker config
POLYGON_CB_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,           # Open after 5 failures
    success_threshold=3,           # Close after 3 successes
    timeout_seconds=60.0,          # Try recovery after 60s
    retry_attempts=3,              # 3 retries per request
    retry_base_delay=1.0,          # Start with 1s delay
    retry_max_delay=10.0,          # Max 10s between retries
    alert_cooldown_seconds=300.0   # 5 min between same alerts
)


class PolygonBaseClient:
    """Base Polygon API client with Circuit Breaker protection"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        redis_client: Optional[Any] = None,
        enable_circuit_breaker: bool = True
    ):
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable is required")

        self.base_url = "https://api.polygon.io"
        self.client = httpx.AsyncClient(timeout=30.0)
        self.redis_client = redis_client
        self.enable_circuit_breaker = enable_circuit_breaker

        # Initialize circuit breaker
        if enable_circuit_breaker:
            registry = CircuitBreakerRegistry.get_instance()
            self._circuit_breaker = registry.get_or_create(
                "polygon_api",
                POLYGON_CB_CONFIG,
                redis_client
            )
        else:
            self._circuit_breaker = None

    def get_circuit_breaker_status(self) -> Optional[Dict[str, Any]]:
        """Get circuit breaker status"""
        if self._circuit_breaker:
            return self._circuit_breaker.get_status()
        return None

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

    async def _raw_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Raw GET request without circuit breaker (internal use)"""
        if params is None:
            params = {}

        params["apiKey"] = self.api_key
        url = f"{self.base_url}{endpoint}"

        logger.warning(f"🔍 [POLYGON REQUEST] Endpoint: {endpoint}")
        logger.warning(f"🔍 [POLYGON REQUEST] Full URL: {url}")
        logger.warning(f"🔍 [POLYGON REQUEST] Params: {params}")

        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            ticker_count = len(data.get('tickers', [])) if 'tickers' in data else 'N/A'
            logger.warning(f"✅ [POLYGON RESPONSE] Status: OK, Tickers returned: {ticker_count}")
            if 'tickers' in data and ticker_count > 0:
                first_ticker = data['tickers'][0].get('ticker', 'UNKNOWN')
                logger.warning(f"✅ [POLYGON RESPONSE] First ticker in response: {first_ticker}")

        return data

    async def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None,
        cache_ttl: int = 60
    ) -> Optional[Dict]:
        """Make GET request to Polygon API with circuit breaker protection

        Args:
            endpoint: API endpoint
            params: Query parameters
            cache_key: Redis key for caching (optional)
            cache_ttl: Cache TTL in seconds (default 60s)
        """
        if self._circuit_breaker and self.enable_circuit_breaker:
            return await self._circuit_breaker.execute(
                self._raw_get,
                endpoint,
                params,
                cache_key=cache_key,
                cache_ttl=cache_ttl
            )
        else:
            try:
                return await self._raw_get(endpoint, params)
            except httpx.HTTPError as e:
                logger.error(f"❌ [POLYGON ERROR] HTTP error for {endpoint}: {e}")
                return None
            except Exception as e:
                logger.error(f"❌ [POLYGON ERROR] Unexpected error for {endpoint}: {e}")
                return None

    async def get_snapshot_gainers(self) -> List[Dict]:
        """Get snapshot gainers"""
        data = await self._get(
            "/v2/snapshot/locale/us/markets/stocks/gainers",
            cache_key="polygon:snapshot:gainers",
            cache_ttl=30
        )
        return data.get("tickers", []) if data else []

    async def get_snapshot_losers(self) -> List[Dict]:
        """Get snapshot losers"""
        data = await self._get(
            "/v2/snapshot/locale/us/markets/stocks/losers",
            cache_key="polygon:snapshot:losers",
            cache_ttl=30
        )
        return data.get("tickers", []) if data else []

    async def get_full_market_snapshot(self, include_otc: bool = False) -> List[Dict]:
        """Get FULL market snapshot - ALL actively traded tickers (10,000+)

        This is THE KEY endpoint to find ALL small cap movers, not just top 20.

        Args:
            include_otc: Include OTC securities (default: False)

        Returns:
            List of ticker snapshots with complete market data
        """
        params = {"include_otc": str(include_otc).lower()}
        cache_key = f"polygon:snapshot:full:{include_otc}"

        data = await self._get(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params,
            cache_key=cache_key,
            cache_ttl=30
        )

        if data and data.get("tickers"):
            tickers = data.get("tickers", [])
            logger.info(f"📊 [FULL SNAPSHOT] Retrieved {len(tickers):,} active tickers from market")
            return tickers

        logger.warning("⚠️ [FULL SNAPSHOT] No tickers returned from full market snapshot")
        return []

    async def get_snapshot_tickers(self, tickers: List[str]) -> List[Dict]:
        """Get snapshot for specific tickers"""
        if not tickers:
            return []

        # Polygon API limit: 100 tickers per request
        ticker_list = tickers[:100]
        ticker_string = ",".join(ticker_list)
        params = {"tickers": ticker_string}

        # Create cache key from sorted tickers (deterministic)
        cache_key = f"polygon:snapshot:tickers:{hash(tuple(sorted(ticker_list)))}"

        logger.warning(f"🎯 [get_snapshot_tickers] Requesting tickers: {tickers[:5]}..." if len(tickers) > 5 else f"🎯 [get_snapshot_tickers] Requesting tickers: {tickers}")

        data = await self._get(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params,
            cache_key=cache_key,
            cache_ttl=30
        )

        # Ensure we return tickers in the same order as requested
        if data and data.get("tickers"):
            tickers_data = data.get("tickers", [])
            logger.warning(f"📊 [get_snapshot_tickers] Got {len(tickers_data)} tickers back")

            # Create a map for quick lookup
            ticker_map = {t.get("ticker"): t for t in tickers_data}

            # Log any mismatches
            returned_tickers = set(ticker_map.keys())
            requested_set = set(tickers)
            if returned_tickers != requested_set:
                missing = requested_set - returned_tickers
                extra = returned_tickers - requested_set
                if missing:
                    logger.warning(f"⚠️ [get_snapshot_tickers] Missing tickers: {missing}")
                if extra:
                    logger.warning(f"⚠️ [get_snapshot_tickers] Extra tickers returned: {extra}")

            # Return in requested order, filtering out any missing tickers
            return [ticker_map[t] for t in tickers if t in ticker_map]

        return []

    async def get_daily_bars(self, ticker: str, from_date: str, to_date: str) -> Optional[Dict]:
        """Get daily bars (OHLCV) for a ticker between dates"""
        params = {
            "from": from_date,
            "to": to_date,
            "adjusted": "true",
            "sort": "asc",
            "limit": 1000
        }

        endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
        return await self._get(endpoint, params)

    def calculate_gap_percent(self, current_price: float, prev_close: float) -> float:
        """Calculate gap percentage"""
        if prev_close <= 0:
            return 0.0
        return ((current_price - prev_close) / prev_close) * 100

    def format_volume(self, volume: int) -> str:
        """Format volume for display"""
        if volume >= 1_000_000:
            return f"{volume / 1_000_000:.2f}M"
        elif volume >= 1_000:
            return f"{volume / 1_000:.0f}K"
        else:
            return str(volume)

    async def fetch_last_price(self, ticker: str) -> float:
        """Fetch last traded price for a ticker - useful for premarket/afterhours"""
        try:
            quote_url = f"/v1/last/stocks/{ticker}"
            quote_data = await self._get(quote_url, {})
            if quote_data and quote_data.get("last"):
                price = quote_data["last"].get("price", 0)
                if price > 0:
                    logger.debug(f"[POLYGON] Got last price for {ticker}: ${price:.2f}")
                    return price
        except Exception as e:
            logger.warning(f"[POLYGON] Failed to get last price for {ticker}: {e}")
        return 0

    async def get_ticker_details(self, ticker: str) -> Optional[Dict]:
        """Get detailed ticker information from Polygon v3 API"""
        endpoint = f"/v3/reference/tickers/{ticker}"
        params = {"date": ""}  # Empty date = most recent data

        try:
            data = await self._get(endpoint, params)
            if data and data.get("results"):
                return data["results"]
            logger.warning(f"[POLYGON] No ticker details found for {ticker}")
            return None
        except Exception as e:
            logger.error(f"[POLYGON] Error getting ticker details for {ticker}: {e}")
            return None

    async def get_related_companies(self, ticker: str) -> Optional[List[str]]:
        """Get related companies for a ticker"""
        endpoint = f"/v1/related-companies/{ticker}"

        try:
            data = await self._get(endpoint)
            if data and data.get("results"):
                results = data.get("results", [])
                # Extract just the ticker symbols
                related = [company.get("ticker") for company in results if company.get("ticker")]
                logger.info(f"[POLYGON] Found {len(related)} related companies for {ticker}")
                return related if related else None
            logger.warning(f"[POLYGON] No related companies found for {ticker}")
            return None
        except Exception as e:
            logger.warning(f"[POLYGON] Error fetching related companies for {ticker}: {e}")
            return None

    async def fetch_previous_close(self, ticker: str) -> float:
        """Fetch previous trading day's closing price"""
        try:
            prev_day_url = f"/v2/aggs/ticker/{ticker}/prev"
            prev_data = await self._get(prev_day_url, {"adjusted": "true"})
            if prev_data and prev_data.get("results"):
                results = prev_data["results"]
                if results and len(results) > 0:
                    prev_close = results[0].get("c", 0)
                    if prev_close > 0:
                        logger.debug(f"[POLYGON] Got prev_close for {ticker}: ${prev_close:.2f}")
                        return prev_close
        except Exception as e:
            logger.warning(f"[POLYGON] Failed to get prev_close for {ticker}: {e}")
        return 0

    def extract_price_data(self, ticker_data: Dict) -> Dict[str, float]:
        """Extract price data from ticker snapshot

        PREMARKET HIGH/LOW FIX (2025-09-26):
        Problem: In premarket (4am-9:30am ET), Polygon's snapshot API returns
        day_high=0 and day_low=0, causing false NHOD/NLOD alerts.

        Solution: Store and track premarket highs/lows in memory when day values are 0.
        This allows accurate NHOD/NLOD detection during premarket hours.

        AFTERHOURS PRICE FIX (2025-11-07):
        Problem: In afterhours (4pm-8pm ET), day.c contains the RTH close price,
        not the current afterhours trading price. This causes false gap calculations.

        Solution: In afterhours, prioritize lastTrade.p over day.c to get real-time
        afterhours price instead of stale RTH close.
        """
        day_data = ticker_data.get("day", {})
        prev_day = ticker_data.get("prevDay", {})
        last_trade = ticker_data.get("lastTrade", {})
        last_quote = ticker_data.get("lastQuote", {})
        min_data = ticker_data.get("min", {})

        # For current price:
        # - In RTH: prefer day.c (official close)
        # - In afterhours/premarket: prefer lastTrade.p (real-time extended hours price)
        # Check if we have a recent trade (lastTrade timestamp within last 10 seconds suggests active trading)
        last_trade_price = last_trade.get("p", 0)
        day_close = day_data.get("c", 0)

        # Use lastTrade.p if available and non-zero (indicates active trading in extended hours)
        # Otherwise fall back to day.c (RTH close) or prev day close
        current_price = (
            last_trade_price or
            day_close or
            last_quote.get("p", 0) or
            last_quote.get("P", 0) or  # Sometimes uppercase in quotes
            prev_day.get("c", 0)
        )

        # For volume, prefer accumulated volume (min.av) over day volume for momentum scanning
        volume = min_data.get("av", 0) or day_data.get("v", 0)

        # Get ticker symbol for tracking
        ticker = ticker_data.get("ticker", "")

        # Get day high/low values
        day_high = day_data.get("h", 0) or 0
        day_low = day_data.get("l", 0) or 0

        # PREMARKET FIX: If day_high/day_low are 0 (premarket), track them manually
        if ticker and (day_high == 0 or day_low == 0):
            # Initialize tracking dict if not exists
            if not hasattr(self, '_premarket_tracker'):
                self._premarket_tracker = {}

            # Get or initialize tracker for this ticker
            tracker = self._premarket_tracker.get(ticker, {})

            # Initialize with current price if first time seeing ticker
            if not tracker:
                tracker = {
                    'high': current_price,
                    'low': current_price,
                    'first_seen': current_price
                }
                self._premarket_tracker[ticker] = tracker
                logger.debug(f"[PREMARKET] {ticker} initialized at ${current_price}")

            # Update high/low
            if current_price > tracker['high']:
                tracker['high'] = current_price
                logger.debug(f"[PREMARKET] {ticker} new high: ${current_price}")
            if current_price < tracker['low']:
                tracker['low'] = current_price
                logger.debug(f"[PREMARKET] {ticker} new low: ${current_price}")

            # Use tracked values if day values are 0
            if day_high == 0:
                day_high = tracker['high']
            if day_low == 0:
                day_low = tracker['low']

        # Get previous close with fallback
        prev_close = prev_day.get("c", 0)

        # If no previous close and we have a current price, calculate gap as 0
        # This is better than skipping the stock entirely during premarket
        if prev_close == 0 and current_price > 0:
            # Log this so we know it's happening
            if ticker:
                logger.warning(f"[POLYGON] {ticker} missing prev_close, will need fallback fetch")

        # Get exchange - Polygon provides this in the ticker details or snapshot
        # Common field names: 'exchange', 'primary_exchange', 'locale'
        exchange = (
            ticker_data.get("exchange") or
            ticker_data.get("primary_exchange") or
            ticker_data.get("primaryExchange") or
            None
        )

        return {
            "current_price": current_price,
            "prev_close": prev_close,
            "day_high": day_high,
            "day_low": day_low,
            "volume": volume,
            "vwap": day_data.get("vw", None),
            "exchange": exchange,
        }

    async def get_minute_bars(self, ticker: str, from_date: str, to_date: str) -> List[Dict]:
        """Fetch minute-level aggregated bars for a ticker

        Args:
            ticker: Stock symbol (e.g., 'AAPL')
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            List of bar dictionaries with 'h' (high), 'l' (low), 'v' (volume), etc.
        """
        try:
            # Polygon aggregates endpoint for minute bars
            endpoint = f"/v2/aggs/ticker/{ticker}/range/1/minute/{from_date}/{to_date}"
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000  # Max bars per request
            }

            data = await self._get(endpoint, params)

            if data and data.get("results"):
                bars = data["results"]
                logger.debug(f"[POLYGON] Got {len(bars)} minute bars for {ticker} from {from_date} to {to_date}")

                # Convert Polygon format to expected format
                # Polygon returns: {"o": open, "h": high, "l": low, "c": close, "v": volume, "t": timestamp}
                # We need: {"h": high, "l": low, "v": volume, ...}
                formatted_bars = []
                for bar in bars:
                    formatted_bars.append({
                        "o": bar.get("o", 0),  # open
                        "h": bar.get("h", 0),  # high
                        "l": bar.get("l", 0),  # low
                        "c": bar.get("c", 0),  # close
                        "v": bar.get("v", 0),  # volume
                        "t": bar.get("t", 0),  # timestamp
                        "vw": bar.get("vw", 0),  # volume weighted average
                        "n": bar.get("n", 0)   # number of transactions
                    })

                return formatted_bars
            else:
                logger.warning(f"[POLYGON] No minute bars found for {ticker} from {from_date} to {to_date}")
                return []

        except Exception as e:
            logger.error(f"[POLYGON] Failed to get minute bars for {ticker}: {e}")
            return []

    async def get_grouped_daily(self, date: str) -> Optional[Dict]:
        """Get grouped daily bars for all stocks on a specific date

        This endpoint returns aggregated bars for ALL tickers in a single call.
        Useful for discovering high volume tickers.

        Args:
            date: Date in YYYY-MM-DD format

        Returns:
            Dict with 'results' containing list of ticker data:
            {
                'T': ticker symbol,
                'o': open,
                'h': high,
                'l': low,
                'c': close,
                'v': volume,
                't': timestamp,
                'vw': volume weighted price,
                'n': number of transactions
            }
        """
        endpoint = f"/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {
            "adjusted": "true",
            "limit": 50000  # Get as many as possible
        }

        try:
            data = await self._get(endpoint, params)

            if data and data.get("status") == "OK":
                results_count = data.get("resultsCount", 0)
                logger.info(f"✅ [GROUPED DAILY] {date}: {results_count:,} tickers retrieved")
                return data
            else:
                logger.warning(f"⚠️ [GROUPED DAILY] No data for {date}")
                return None

        except Exception as e:
            logger.error(f"❌ [GROUPED DAILY] Error for {date}: {e}")
            return None

    async def get_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_date: str,
        to_date: str
    ) -> Optional[List[Dict]]:
        """Get aggregates (bars) for a ticker with flexible timespan

        Args:
            ticker: Stock symbol
            multiplier: Size of timespan multiplier (e.g., 1 for 1 day, 4 for 4 hours)
            timespan: Type of timespan (minute, hour, day, week, month, quarter, year)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            List of OHLCV bars
        """
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000
        }

        try:
            data = await self._get(endpoint, params)

            if data and data.get("results"):
                bars = data["results"]
                logger.debug(f"[POLYGON] Got {len(bars)} {timespan} bars for {ticker}")
                return bars
            else:
                logger.warning(f"[POLYGON] No bars found for {ticker} ({multiplier}{timespan})")
                return []

        except Exception as e:
            logger.error(f"[POLYGON] Failed to get aggregates for {ticker}: {e}")
            return []

    async def get_quote(self, ticker: str) -> Optional[Dict]:
        """Get real-time quote for a ticker

        Returns:
            Quote data with last trade info
        """
        endpoint = f"/v2/last/trade/{ticker}"

        try:
            data = await self._get(endpoint)

            if data and data.get("results"):
                return data["results"]
            else:
                logger.warning(f"[POLYGON] No quote found for {ticker}")
                return None

        except Exception as e:
            logger.error(f"[POLYGON] Failed to get quote for {ticker}: {e}")
            return None

    async def get_52_week_high_low(self, ticker: str) -> Dict[str, float]:
        """Get 52-week high and low for a ticker

        Fetches daily bars for the past 52 weeks and calculates the
        maximum high and minimum low from all bars.

        Args:
            ticker: Stock symbol

        Returns:
            Dict with 'high_52w' and 'low_52w' prices, or 0 if not available
        """
        from datetime import datetime, timedelta, timezone

        try:
            # Calculate date range (52 weeks = 364 days)
            to_date = datetime.now(timezone.utc)
            from_date = to_date - timedelta(days=364)

            from_str = from_date.strftime("%Y-%m-%d")
            to_str = to_date.strftime("%Y-%m-%d")

            # Fetch daily bars for 52 weeks
            bars = await self.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan="day",
                from_date=from_str,
                to_date=to_str
            )

            if not bars:
                logger.warning(f"[POLYGON] No 52-week data for {ticker}")
                return {"high_52w": 0, "low_52w": 0}

            # Calculate high (max of all highs) and low (min of all lows)
            high_52w = max(bar.get("h", 0) for bar in bars)
            low_52w = min(bar.get("l", float('inf')) for bar in bars if bar.get("l", 0) > 0)

            # Handle edge case where no valid lows found
            if low_52w == float('inf'):
                low_52w = 0

            logger.info(f"[POLYGON] {ticker} 52-week: High=${high_52w:.2f}, Low=${low_52w:.2f} (from {len(bars)} bars)")

            return {
                "high_52w": high_52w,
                "low_52w": low_52w
            }

        except Exception as e:
            logger.error(f"[POLYGON] Failed to get 52-week high/low for {ticker}: {e}")
            return {"high_52w": 0, "low_52w": 0}

    async def calculate_run_metrics(
        self,
        ticker: str,
        current_price: float,
        today_open: float = None,
        today_high: float = None,
        today_volume: int = None
    ) -> Dict[str, Any]:
        """Calculate multi-day run metrics for PRIMER_DIA_ROJO detection

        Analyzes the last 10+ trading days to detect:
        - Consecutive green days (close > open) before today
        - Extension % from run start price to peak
        - Whether today is the first red day after a run
        - Volume comparison vs yesterday

        Args:
            ticker: Stock symbol
            current_price: Current trading price (used to determine if today is red)
            today_open: Today's open price (if available)
            today_high: Today's high price (if available)
            today_volume: Today's volume (if available)

        Returns:
            Dict with:
                consecutive_green_days: int - Days with close > open before today
                extension_from_run_start: float - % extension from run start to peak
                is_first_red_day: bool - True if today is first red after >=2 green days
                volume_vs_yesterday: float - Ratio of today's volume to yesterday's
                never_recovered_open: bool - True if today's high <= open
                run_start_price: float - Open price of first green day
                run_peak_price: float - Highest high during the run
        """
        from datetime import datetime, timedelta, timezone

        default_result = {
            'consecutive_green_days': None,
            'extension_from_run_start': None,
            'is_first_red_day': None,
            'volume_vs_yesterday': None,
            'never_recovered_open': None,
            'run_start_price': None,
            'run_peak_price': None
        }

        try:
            # Fetch last 15 trading days (to account for weekends/holidays)
            to_date = datetime.now(timezone.utc)
            from_date = to_date - timedelta(days=21)  # ~15 trading days

            bars = await self.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan="day",
                from_date=from_date.strftime("%Y-%m-%d"),
                to_date=to_date.strftime("%Y-%m-%d")
            )

            if not bars or len(bars) < 2:
                logger.debug(f"[RUN-METRICS] {ticker}: Insufficient data ({len(bars) if bars else 0} bars)")
                return default_result

            # Sort by timestamp descending (most recent first)
            bars = sorted(bars, key=lambda x: x.get('t', 0), reverse=True)

            # Separate today from historical data
            # bars[0] should be today (most recent)
            today_bar = bars[0]
            historical_bars = bars[1:]  # Yesterday and before

            if not historical_bars:
                return default_result

            # 1. Count consecutive green days (starting from yesterday, going backwards)
            consecutive_green = 0
            run_start_idx = 0

            for i, bar in enumerate(historical_bars):
                bar_open = bar.get('o', 0)
                bar_close = bar.get('c', 0)

                if bar_open > 0 and bar_close > bar_open:  # GREEN day
                    consecutive_green += 1
                    run_start_idx = i
                else:  # RED day - stop counting
                    break

            # 2. Calculate extension from run start
            extension_pct = 0.0
            run_start_price = 0.0
            run_peak_price = 0.0

            if consecutive_green >= 2:
                # Get the open price of the first green day (run start)
                run_start_price = historical_bars[run_start_idx].get('o', 0)

                # Get peak price (max high) during the entire run
                run_bars = historical_bars[:run_start_idx + 1]  # All bars in the run
                run_peak_price = max(bar.get('h', 0) for bar in run_bars)

                if run_start_price > 0:
                    extension_pct = ((run_peak_price - run_start_price) / run_start_price) * 100

            # 3. Determine if today is first red day
            # Use provided today_open or bar data
            _today_open = today_open or today_bar.get('o', 0)
            is_red_today = False

            if _today_open > 0:
                # Today is red if current price < open
                is_red_today = current_price < _today_open

            is_first_red = consecutive_green >= 2 and is_red_today

            # 4. Calculate volume vs yesterday
            volume_vs_yesterday = None
            yesterday_bar = historical_bars[0] if historical_bars else None
            _today_volume = today_volume or today_bar.get('v', 0)

            if yesterday_bar and yesterday_bar.get('v', 0) > 0 and _today_volume:
                volume_vs_yesterday = _today_volume / yesterday_bar.get('v')

            # 5. Check if today never recovered open
            _today_high = today_high or today_bar.get('h', 0)
            never_recovered = None
            if _today_open > 0 and _today_high > 0:
                never_recovered = _today_high <= _today_open

            result = {
                'consecutive_green_days': consecutive_green if consecutive_green > 0 else None,
                'extension_from_run_start': round(extension_pct, 2) if extension_pct > 0 else None,
                'is_first_red_day': is_first_red if consecutive_green >= 2 else None,
                'volume_vs_yesterday': round(volume_vs_yesterday, 2) if volume_vs_yesterday else None,
                'never_recovered_open': never_recovered,
                'run_start_price': round(run_start_price, 2) if run_start_price > 0 else None,
                'run_peak_price': round(run_peak_price, 2) if run_peak_price > 0 else None
            }

            vol_ratio_str = f"{volume_vs_yesterday:.2f}" if volume_vs_yesterday else "N/A"
            logger.info(f"[RUN-METRICS] {ticker}: green_days={consecutive_green}, "
                       f"extension={extension_pct:.1f}%, first_red={is_first_red}, "
                       f"vol_ratio={vol_ratio_str}")

            return result

        except Exception as e:
            logger.error(f"[RUN-METRICS] Failed to calculate run metrics for {ticker}: {e}")
            import traceback
            logger.debug(f"[RUN-METRICS] Traceback: {traceback.format_exc()}")
            return default_result

    async def get_run_metrics_batch(
        self,
        tickers: List[str],
        current_prices: Dict[str, float],
        today_opens: Dict[str, float] = None,
        today_highs: Dict[str, float] = None,
        today_volumes: Dict[str, int] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate run metrics for multiple tickers in batch

        Args:
            tickers: List of ticker symbols
            current_prices: Dict mapping ticker to current price
            today_opens: Optional dict mapping ticker to today's open
            today_highs: Optional dict mapping ticker to today's high
            today_volumes: Optional dict mapping ticker to today's volume

        Returns:
            Dict mapping ticker to run metrics dict
        """
        import asyncio

        today_opens = today_opens or {}
        today_highs = today_highs or {}
        today_volumes = today_volumes or {}

        async def get_metrics(ticker: str):
            return ticker, await self.calculate_run_metrics(
                ticker=ticker,
                current_price=current_prices.get(ticker, 0),
                today_open=today_opens.get(ticker),
                today_high=today_highs.get(ticker),
                today_volume=today_volumes.get(ticker)
            )

        # Run all requests concurrently
        tasks = [get_metrics(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build result dict
        metrics_map = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"[RUN-METRICS-BATCH] Error: {result}")
                continue
            ticker, metrics = result
            metrics_map[ticker] = metrics

        logger.info(f"[RUN-METRICS-BATCH] Calculated metrics for {len(metrics_map)} tickers")
        return metrics_map