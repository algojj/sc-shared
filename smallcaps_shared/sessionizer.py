"""
Sessionizer - Robust Session Classification with Holiday Support

Classifies timestamps into trading sessions (Premarket, RTH, After-hours)
considering US/Eastern timezone, NYSE/NASDAQ holidays, and early closes.

Sessions (US/Eastern):
- Premarket: 04:00:00 - 09:29:59 ET
- RTH (Regular Trading Hours): 09:30:00 - 15:59:59 ET
- After-hours: 16:00:00 - 19:59:59 ET

Holiday Support:
- NYSE/NASDAQ official holidays (no trading)
- Early close days (1:00 PM ET close)
"""

import logging
from datetime import datetime, date, time, timezone
from typing import Dict, Literal, Optional, Set
import pytz
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Timezone constants
ET_TZ = pytz.timezone('America/New_York')

# Session time boundaries (US/Eastern)
PREMARKET_START = time(4, 0, 0)   # 04:00:00 ET
RTH_START = time(9, 30, 0)        # 09:30:00 ET
RTH_END = time(16, 0, 0)          # 16:00:00 ET
AFTERHOURS_END = time(20, 0, 0)   # 20:00:00 ET

# Early close time
EARLY_CLOSE_TIME = time(13, 0, 0)  # 13:00:00 ET (1:00 PM)

SessionType = Literal['premarket', 'rth', 'afterhours', 'closed']


@dataclass
class SessionInfo:
    """Session information for a given timestamp"""
    session: SessionType
    is_trading_day: bool
    is_early_close: bool
    timestamp_et: datetime
    reason: Optional[str] = None  # e.g., "holiday: New Year's Day"


class HolidayCalendar:
    """NYSE/NASDAQ holiday calendar"""

    # Fixed holidays (always on these dates)
    FIXED_HOLIDAYS = {
        (1, 1): "New Year's Day",
        (7, 4): "Independence Day",
        (12, 25): "Christmas Day"
    }

    # Observable holidays (if falls on weekend, observed on Friday/Monday)
    OBSERVABLE_HOLIDAYS = {
        (1, 1): "New Year's Day",
        (7, 4): "Independence Day",
        (12, 25): "Christmas Day"
    }

    # Special holidays (year-specific, need to be calculated)
    # Format: year -> [(month, day, name), ...]
    SPECIAL_HOLIDAYS = {
        2024: [
            (1, 15, "Martin Luther King Jr. Day"),
            (2, 19, "Presidents' Day"),
            (3, 29, "Good Friday"),
            (5, 27, "Memorial Day"),
            (6, 19, "Juneteenth National Independence Day"),
            (9, 2, "Labor Day"),
            (11, 28, "Thanksgiving Day"),
        ],
        2025: [
            (1, 1, "New Year's Day"),
            (1, 20, "Martin Luther King Jr. Day"),
            (2, 17, "Presidents' Day"),
            (4, 18, "Good Friday"),
            (5, 26, "Memorial Day"),
            (6, 19, "Juneteenth National Independence Day"),
            (7, 4, "Independence Day"),
            (9, 1, "Labor Day"),
            (11, 27, "Thanksgiving Day"),
            (12, 25, "Christmas Day"),
        ],
        2026: [
            (1, 1, "New Year's Day"),
            (1, 19, "Martin Luther King Jr. Day"),
            (2, 16, "Presidents' Day"),
            (4, 3, "Good Friday"),
            (5, 25, "Memorial Day"),
            (6, 19, "Juneteenth National Independence Day"),
            (7, 3, "Independence Day (observed)"),
            (9, 7, "Labor Day"),
            (11, 26, "Thanksgiving Day"),
            (12, 25, "Christmas Day"),
        ],
        2027: [
            (1, 1, "New Year's Day"),
            (1, 18, "Martin Luther King Jr. Day"),
            (2, 15, "Presidents' Day"),
            (3, 26, "Good Friday"),
            (5, 31, "Memorial Day"),
            (6, 18, "Juneteenth National Independence Day (observed)"),
            (7, 5, "Independence Day (observed)"),
            (9, 6, "Labor Day"),
            (11, 25, "Thanksgiving Day"),
            (12, 24, "Christmas Day (observed)"),
        ]
    }

    # Early close days (day before major holidays) — hardcoded fallback
    # Format: year -> [(month, day), ...]
    EARLY_CLOSE_DAYS = {
        2024: [
            (7, 3),   # July 3 (before July 4th)
            (11, 29), # Day after Thanksgiving
            (12, 24), # Christmas Eve
        ],
        2025: [
            (7, 3),   # July 3 (before July 4th)
            (11, 28), # Day after Thanksgiving
            (12, 24), # Christmas Eve
        ],
        2026: [
            (7, 2),   # July 2 (before July 3rd observed)
            (11, 27), # Day after Thanksgiving
            (12, 24), # Christmas Eve
        ],
        2027: [
            (7, 2),   # July 2 (before July 5th observed)
            (11, 26), # Day after Thanksgiving
        ]
    }

    # Dynamic holidays fetched from Polygon API at runtime
    _dynamic_holidays: Dict[date, str] = {}
    _dynamic_early_closes: Set[date] = set()
    _last_refresh: Optional[datetime] = None

    @classmethod
    async def refresh_from_api(cls):
        """Fetch upcoming holidays from Polygon API and populate dynamic cache.
        Call at service startup. Hardcoded calendars serve as fallback if API fails."""
        try:
            from .market_status import get_market_checker
            checker = get_market_checker()
            holidays = await checker.get_upcoming_holidays()

            new_holidays = {}
            new_early_closes = set()

            for h in holidays:
                try:
                    h_date = datetime.strptime(h['date'], '%Y-%m-%d').date()
                    h_name = h.get('name', 'Market Holiday')
                    h_status = h.get('status', 'closed')

                    if h_status == 'closed':
                        new_holidays[h_date] = h_name
                    elif h_status == 'early-close':
                        new_early_closes.add(h_date)
                except (KeyError, ValueError):
                    continue

            cls._dynamic_holidays = new_holidays
            cls._dynamic_early_closes = new_early_closes
            cls._last_refresh = datetime.now(timezone.utc)
            logger.info(f"Refreshed holidays from Polygon API: {len(new_holidays)} holidays, {len(new_early_closes)} early closes")
        except Exception as e:
            logger.warning(f"Could not refresh holidays from Polygon API: {e} — using hardcoded fallback")

    @classmethod
    def is_holiday(cls, check_date: date) -> tuple[bool, Optional[str]]:
        """
        Check if date is a market holiday.
        Checks Polygon API cache first, falls back to hardcoded calendar.

        Returns:
            (is_holiday, holiday_name)
        """
        # Weekend
        if check_date.weekday() >= 5:  # Saturday=5, Sunday=6
            return (True, "Weekend")

        # Dynamic holidays from Polygon API (populated by refresh_from_api)
        if check_date in cls._dynamic_holidays:
            return (True, cls._dynamic_holidays[check_date])

        # Fallback: check hardcoded special holidays for this year
        year_holidays = cls.SPECIAL_HOLIDAYS.get(check_date.year, [])
        for month, day, name in year_holidays:
            if check_date.month == month and check_date.day == day:
                return (True, name)

        # Check fixed holidays
        month_day = (check_date.month, check_date.day)
        if month_day in cls.FIXED_HOLIDAYS:
            return (True, cls.FIXED_HOLIDAYS[month_day])

        return (False, None)

    @classmethod
    def is_early_close(cls, check_date: date) -> bool:
        """Check if date is an early close day (market closes at 1:00 PM ET).
        Checks Polygon API cache first, falls back to hardcoded calendar."""
        # Weekend can't be early close
        if check_date.weekday() >= 5:
            return False

        # Dynamic early closes from Polygon API
        if check_date in cls._dynamic_early_closes:
            return True

        # Fallback: hardcoded early close days
        year_early_closes = cls.EARLY_CLOSE_DAYS.get(check_date.year, [])
        month_day = (check_date.month, check_date.day)
        return month_day in year_early_closes


class Sessionizer:
    """
    Robust session classification with holiday support

    Usage:
        sessionizer = Sessionizer()

        # From UTC timestamp
        info = sessionizer.get_session(utc_timestamp)

        # From ET timestamp
        info = sessionizer.get_session_from_et(et_timestamp)

        # Check if specific session
        if info.session == 'rth':
            # Process RTH logic
            pass
    """

    def __init__(self):
        self.holiday_calendar = HolidayCalendar()
        logger.info("✅ Sessionizer initialized with holiday calendar")

    def get_session(
        self,
        timestamp: datetime,
        source_tz: Optional[pytz.timezone] = None
    ) -> SessionInfo:
        """
        Get session info for a timestamp

        Args:
            timestamp: datetime object (can be naive or aware)
            source_tz: If timestamp is naive, assume this timezone
                      If None and naive, assumes UTC

        Returns:
            SessionInfo with session classification
        """
        # Convert to ET
        if timestamp.tzinfo is None:
            # Naive timestamp - assume source_tz or UTC
            tz = source_tz or pytz.UTC
            timestamp = tz.localize(timestamp)

        # Convert to ET
        et_time = timestamp.astimezone(ET_TZ)

        return self.get_session_from_et(et_time)

    def get_session_from_et(self, et_timestamp: datetime) -> SessionInfo:
        """
        Get session info for an ET timestamp

        Args:
            et_timestamp: datetime in US/Eastern timezone

        Returns:
            SessionInfo with session classification
        """
        check_date = et_timestamp.date()
        check_time = et_timestamp.time()

        # Check if holiday
        is_holiday, holiday_name = self.holiday_calendar.is_holiday(check_date)
        if is_holiday:
            return SessionInfo(
                session='closed',
                is_trading_day=False,
                is_early_close=False,
                timestamp_et=et_timestamp,
                reason=f"holiday: {holiday_name}"
            )

        # Check if early close day
        is_early_close = self.holiday_calendar.is_early_close(check_date)

        # Determine session based on time
        if check_time < PREMARKET_START:
            # Before 4:00 AM - closed
            return SessionInfo(
                session='closed',
                is_trading_day=True,
                is_early_close=is_early_close,
                timestamp_et=et_timestamp,
                reason="before premarket (before 04:00 ET)"
            )

        elif check_time < RTH_START:
            # 04:00 - 09:29:59 - Premarket
            return SessionInfo(
                session='premarket',
                is_trading_day=True,
                is_early_close=is_early_close,
                timestamp_et=et_timestamp
            )

        elif check_time < (EARLY_CLOSE_TIME if is_early_close else RTH_END):
            # RTH: 09:30 - 16:00 (or 13:00 on early close)
            return SessionInfo(
                session='rth',
                is_trading_day=True,
                is_early_close=is_early_close,
                timestamp_et=et_timestamp
            )

        elif check_time < AFTERHOURS_END:
            # 16:00 - 20:00 - After-hours (unless it was early close)
            if is_early_close and check_time >= EARLY_CLOSE_TIME:
                # After 1:00 PM on early close day
                return SessionInfo(
                    session='afterhours',
                    is_trading_day=True,
                    is_early_close=True,
                    timestamp_et=et_timestamp
                )
            else:
                return SessionInfo(
                    session='afterhours',
                    is_trading_day=True,
                    is_early_close=False,
                    timestamp_et=et_timestamp
                )

        else:
            # After 20:00 - closed
            return SessionInfo(
                session='closed',
                is_trading_day=True,
                is_early_close=is_early_close,
                timestamp_et=et_timestamp,
                reason="after hours (after 20:00 ET)"
            )

    def is_trading_session(self, session_info: SessionInfo) -> bool:
        """Check if session is a trading session (not closed)"""
        return session_info.session in ['premarket', 'rth', 'afterhours']

    def is_rth(self, session_info: SessionInfo) -> bool:
        """Check if session is RTH"""
        return session_info.session == 'rth'

    def is_premarket(self, session_info: SessionInfo) -> bool:
        """Check if session is premarket"""
        return session_info.session == 'premarket'

    def is_afterhours(self, session_info: SessionInfo) -> bool:
        """Check if session is after-hours"""
        return session_info.session == 'afterhours'


# Singleton instance
_sessionizer = None

def get_sessionizer() -> Sessionizer:
    """Get singleton Sessionizer instance"""
    global _sessionizer
    if _sessionizer is None:
        _sessionizer = Sessionizer()
    return _sessionizer
