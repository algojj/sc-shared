#!/usr/bin/env python3
"""
Session-specific configuration for scanners
Different thresholds for premarket, RTH, and afterhours
"""

from datetime import datetime
import pytz

ET_TZ = pytz.timezone('America/New_York')


class SessionManager:
    """Manages market session detection"""

    def get_current_session(self) -> str:
        """
        Determine current market session based on time

        Returns:
            'premarket', 'rth', 'afterhours', or 'closed'
        """
        now = datetime.now(ET_TZ)
        hour = now.hour
        minute = now.minute
        time_decimal = hour + minute/60.0

        # Market hours (Eastern Time)
        # Premarket: 4:00 AM - 9:30 AM
        # RTH: 9:30 AM - 4:00 PM
        # Afterhours: 4:00 PM - 8:00 PM
        # Closed: 8:00 PM - 4:00 AM

        if 4 <= time_decimal < 9.5:
            return 'premarket'
        elif 9.5 <= time_decimal < 16:
            return 'rth'
        elif 16 <= time_decimal < 20:
            return 'afterhours'
        else:
            return 'closed'

    def calculate_gap_percent(self, current_price: float, ticker_data: dict, session: str = None) -> float:
        """
        Calculate gap percentage using prev_close as reference

        Args:
            current_price: Current stock price
            ticker_data: Dictionary with price data (prev_close, day_low, pm_low)
            session: Market session (not used in this simple implementation)

        Returns:
            Gap percentage from previous close
        """
        prev_close = ticker_data.get('prev_close')

        if not prev_close or prev_close <= 0:
            # No valid prev_close — return 0 instead of falling back to intraday values
            # (day_low/pm_low are today's prices, not yesterday's close)
            return 0.0

        # Calculate gap percentage
        if prev_close > 0:
            return ((current_price - prev_close) / prev_close) * 100

        return 0.0
