"""
Market Status Checker
Centralized utility for checking market status using Polygon API.
Includes caching to avoid excessive API calls.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, date, timezone
from typing import Dict, Any, Optional, List
import aiohttp
import pytz

logger = logging.getLogger(__name__)

ET_TZ = pytz.timezone('America/New_York')


class MarketStatusChecker:
    """
    Centralized market status checker using Polygon API.

    Features:
    - Real-time market status from Polygon API
    - Caching to reduce API calls (configurable TTL)
    - Upcoming holidays tracking
    - Methods: is_market_open(), is_trading_day(), get_session()
    """

    # Cache TTL in seconds
    STATUS_CACHE_TTL = 300  # 5 minutes for current status
    HOLIDAYS_CACHE_TTL = 86400  # 24 hours for holidays list

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        self.base_url = "https://api.polygon.io/v1/marketstatus"

        # Cache storage
        self._status_cache: Optional[Dict[str, Any]] = None
        self._status_cache_time: Optional[datetime] = None
        self._holidays_cache: Optional[List[Dict[str, Any]]] = None
        self._holidays_cache_time: Optional[datetime] = None

    async def get_market_status(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get current market status from Polygon API.

        Returns:
            Dict with keys: market, exchanges, afterHours, earlyHours, serverTime
        """
        # Check cache
        if use_cache and self._status_cache and self._status_cache_time:
            cache_age = (datetime.now(timezone.utc) - self._status_cache_time).total_seconds()
            if cache_age < self.STATUS_CACHE_TTL:
                logger.debug(f"[MARKET_STATUS] Using cached status (age: {cache_age:.0f}s)")
                return self._status_cache

        # Fetch from API
        try:
            url = f"{self.base_url}/now?apiKey={self.api_key}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Update cache
                        self._status_cache = data
                        self._status_cache_time = datetime.now(timezone.utc)

                        logger.info(f"[MARKET_STATUS] Fetched: market={data.get('market')}, "
                                   f"NYSE={data.get('exchanges', {}).get('nyse')}")
                        return data
                    else:
                        logger.error(f"[MARKET_STATUS] API error: {response.status}")
                        return self._get_fallback_status()

        except Exception as e:
            logger.error(f"[MARKET_STATUS] Error fetching status: {e}")
            return self._get_fallback_status()

    async def get_upcoming_holidays(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get upcoming market holidays from Polygon API.

        Returns:
            List of holiday dicts with: date, exchange, name, status, open, close
        """
        # Check cache
        if use_cache and self._holidays_cache and self._holidays_cache_time:
            cache_age = (datetime.now(timezone.utc) - self._holidays_cache_time).total_seconds()
            if cache_age < self.HOLIDAYS_CACHE_TTL:
                logger.debug(f"[MARKET_STATUS] Using cached holidays (age: {cache_age:.0f}s)")
                return self._holidays_cache

        # Fetch from API
        try:
            url = f"{self.base_url}/upcoming?apiKey={self.api_key}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Update cache
                        self._holidays_cache = data
                        self._holidays_cache_time = datetime.now(timezone.utc)

                        logger.info(f"[MARKET_STATUS] Fetched {len(data)} upcoming holidays")
                        return data
                    else:
                        logger.error(f"[MARKET_STATUS] Holidays API error: {response.status}")
                        return []

        except Exception as e:
            logger.error(f"[MARKET_STATUS] Error fetching holidays: {e}")
            return []

    async def is_market_open(self) -> bool:
        """
        Check if market is currently open (including premarket and afterhours).

        Returns:
            True if market is open, premarket, or afterhours; False if closed
        """
        status = await self.get_market_status()
        market = status.get('market', 'closed')
        early_hours = status.get('earlyHours', False)
        after_hours = status.get('afterHours', False)

        return market == 'open' or early_hours or after_hours

    async def is_trading_day(self, target_date: Optional[date] = None) -> bool:
        """
        Check if a given date is a trading day (not weekend or holiday).

        Args:
            target_date: Date to check (defaults to today ET)

        Returns:
            True if it's a trading day
        """
        if target_date is None:
            target_date = datetime.now(ET_TZ).date()

        # Check weekend
        if target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False

        # Check hardcoded holiday calendar first (always accurate, even for past dates)
        try:
            from smallcaps_shared.sessionizer import HolidayCalendar
            is_holiday, holiday_name = HolidayCalendar.is_holiday(target_date)
            if is_holiday:
                logger.info(f"[MARKET_STATUS] {target_date} is a holiday (calendar): {holiday_name}")
                return False
        except Exception as e:
            logger.debug(f"[MARKET_STATUS] HolidayCalendar fallback failed: {e}")

        # Check Polygon API upcoming holidays (only has future dates)
        holidays = await self.get_upcoming_holidays()
        target_str = target_date.strftime('%Y-%m-%d')

        for holiday in holidays:
            if holiday.get('date') == target_str and holiday.get('status') == 'closed':
                logger.info(f"[MARKET_STATUS] {target_date} is a holiday (API): {holiday.get('name')}")
                return False

        return True

    async def get_session(self) -> str:
        """
        Get current trading session.

        Returns:
            One of: 'premarket', 'regular', 'afterhours', 'closed'
        """
        status = await self.get_market_status()
        market = status.get('market', 'closed')
        early_hours = status.get('earlyHours', False)
        after_hours = status.get('afterHours', False)

        if market == 'open':
            return 'regular'
        elif early_hours:
            return 'premarket'
        elif after_hours:
            return 'afterhours'
        else:
            return 'closed'

    async def get_holidays_in_range(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get holidays within the next N days.

        Args:
            days: Number of days to look ahead

        Returns:
            List of holiday dicts within the range
        """
        holidays = await self.get_upcoming_holidays()
        today = datetime.now(ET_TZ).date()
        end_date = today + timedelta(days=days)

        upcoming = []
        for holiday in holidays:
            holiday_date = datetime.strptime(holiday.get('date', ''), '%Y-%m-%d').date()
            if today <= holiday_date <= end_date:
                # Dedupe by date (NYSE and NASDAQ both listed)
                if not any(h.get('date') == holiday.get('date') for h in upcoming):
                    upcoming.append(holiday)

        return upcoming

    async def get_next_holiday(self) -> Optional[Dict[str, Any]]:
        """
        Get the next upcoming market holiday.

        Returns:
            Holiday dict or None if no upcoming holidays
        """
        holidays = await self.get_upcoming_holidays()
        today = datetime.now(ET_TZ).date()

        for holiday in holidays:
            holiday_date = datetime.strptime(holiday.get('date', ''), '%Y-%m-%d').date()
            if holiday_date >= today and holiday.get('status') == 'closed':
                return holiday

        return None

    async def days_until_next_holiday(self) -> Optional[int]:
        """
        Get number of days until next market holiday.

        Returns:
            Number of days, or None if no upcoming holidays
        """
        next_holiday = await self.get_next_holiday()
        if next_holiday:
            holiday_date = datetime.strptime(next_holiday.get('date', ''), '%Y-%m-%d').date()
            today = datetime.now(ET_TZ).date()
            return (holiday_date - today).days
        return None

    def _get_fallback_status(self) -> Dict[str, Any]:
        """
        Fallback status calculation when API is unavailable.
        Uses local time-based logic.
        """
        now_et = datetime.now(ET_TZ)
        hour = now_et.hour
        weekday = now_et.weekday()

        # Weekend check
        if weekday >= 5:
            return {
                'market': 'closed',
                'exchanges': {'nyse': 'closed', 'nasdaq': 'closed'},
                'earlyHours': False,
                'afterHours': False,
                'serverTime': now_et.isoformat(),
                '_source': 'fallback'
            }

        # Time-based session (simplified)
        if 4 <= hour < 9.5:
            return {
                'market': 'extended-hours',
                'exchanges': {'nyse': 'extended-hours', 'nasdaq': 'extended-hours'},
                'earlyHours': True,
                'afterHours': False,
                'serverTime': now_et.isoformat(),
                '_source': 'fallback'
            }
        elif 9.5 <= hour < 16:
            return {
                'market': 'open',
                'exchanges': {'nyse': 'open', 'nasdaq': 'open'},
                'earlyHours': False,
                'afterHours': False,
                'serverTime': now_et.isoformat(),
                '_source': 'fallback'
            }
        elif 16 <= hour < 20:
            return {
                'market': 'extended-hours',
                'exchanges': {'nyse': 'extended-hours', 'nasdaq': 'extended-hours'},
                'earlyHours': False,
                'afterHours': True,
                'serverTime': now_et.isoformat(),
                '_source': 'fallback'
            }
        else:
            return {
                'market': 'closed',
                'exchanges': {'nyse': 'closed', 'nasdaq': 'closed'},
                'earlyHours': False,
                'afterHours': False,
                'serverTime': now_et.isoformat(),
                '_source': 'fallback'
            }


# Singleton instance for easy access
_market_checker: Optional[MarketStatusChecker] = None


def get_market_checker(api_key: Optional[str] = None) -> MarketStatusChecker:
    """Get or create singleton MarketStatusChecker instance."""
    global _market_checker
    if _market_checker is None:
        _market_checker = MarketStatusChecker(api_key)
    return _market_checker


# Convenience async functions
async def is_market_open() -> bool:
    """Check if market is currently open."""
    return await get_market_checker().is_market_open()


async def is_trading_day(target_date: Optional[date] = None) -> bool:
    """Check if given date is a trading day."""
    return await get_market_checker().is_trading_day(target_date)


async def get_session() -> str:
    """Get current trading session."""
    return await get_market_checker().get_session()


async def get_market_status() -> Dict[str, Any]:
    """Get full market status."""
    return await get_market_checker().get_market_status()
