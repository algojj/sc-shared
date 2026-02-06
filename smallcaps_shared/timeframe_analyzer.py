"""
Multi-Timeframe Analysis Module
Shared across services for technical analysis across different timeframes
"""
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import pytz
from dataclasses import dataclass

from .polygon_base import PolygonBaseClient

logger = logging.getLogger(__name__)

ET_TZ = pytz.timezone('America/New_York')

@dataclass
class TrendData:
    """Trend information for a timeframe"""
    timeframe: str
    trend: str  # 'up', 'down', 'neutral'
    sma20: float
    current_price: float
    strength: float  # 0-100
    bars_analyzed: int

@dataclass
class SwingPoint:
    """Swing high/low point"""
    price: float
    timestamp: datetime
    type: str  # 'high' or 'low'
    volume: int

class TimeframeAnalyzer:
    """Multi-timeframe technical analysis"""

    def __init__(self, polygon_client: PolygonBaseClient):
        self.polygon = polygon_client

    async def get_trend(self, ticker: str, timeframe: str, lookback_bars: int = 20) -> Optional[TrendData]:
        """
        Get trend for a specific timeframe

        Args:
            ticker: Stock ticker
            timeframe: 'weekly', 'daily', '4h', '1h'
            lookback_bars: Number of bars to analyze

        Returns:
            TrendData or None
        """
        try:
            # Map timeframe to Polygon API params
            multiplier, timespan = self._map_timeframe(timeframe)

            # Get bars
            bars = await self._fetch_bars(ticker, multiplier, timespan, lookback_bars)

            if not bars or len(bars) < lookback_bars:
                logger.warning(f"Insufficient data for {ticker} {timeframe}: got {len(bars) if bars else 0} bars")
                return None

            # Calculate SMA20
            sma20 = sum(bar['c'] for bar in bars[-20:]) / 20
            current_price = bars[-1]['c']

            # Determine trend
            # Bullish: price > SMA20 AND price making higher highs
            # Bearish: price < SMA20 AND price making lower lows
            if current_price > sma20:
                # Check if making higher highs
                recent_high = max(bar['h'] for bar in bars[-5:])
                prev_high = max(bar['h'] for bar in bars[-10:-5])

                if recent_high > prev_high:
                    trend = 'up'
                    strength = min(((current_price - sma20) / sma20) * 100, 100)
                else:
                    trend = 'neutral'
                    strength = 50
            else:
                # Check if making lower lows
                recent_low = min(bar['l'] for bar in bars[-5:])
                prev_low = min(bar['l'] for bar in bars[-10:-5])

                if recent_low < prev_low:
                    trend = 'down'
                    strength = min(((sma20 - current_price) / current_price) * 100, 100)
                else:
                    trend = 'neutral'
                    strength = 50

            return TrendData(
                timeframe=timeframe,
                trend=trend,
                sma20=sma20,
                current_price=current_price,
                strength=strength,
                bars_analyzed=len(bars)
            )

        except Exception as e:
            logger.error(f"Error getting trend for {ticker} {timeframe}: {e}")
            return None

    async def detect_break_of_structure(
        self,
        ticker: str,
        timeframe: str,
        lookback_bars: int = 50
    ) -> Optional[Dict]:
        """
        Detect Break of Structure (BOS)

        BOS Bullish: Price breaks above recent swing high
        BOS Bearish: Price breaks below recent swing low

        Returns:
            {
                'type': 'bullish' | 'bearish' | None,
                'break_price': float,
                'break_time': datetime,
                'swing_point': SwingPoint,
                'confirmation': bool
            }
        """
        try:
            multiplier, timespan = self._map_timeframe(timeframe)
            bars = await self._fetch_bars(ticker, multiplier, timespan, lookback_bars)

            if not bars or len(bars) < 10:
                return None

            # Find swing points
            swing_highs = self._find_swing_highs(bars, window=5)
            swing_lows = self._find_swing_lows(bars, window=5)

            if not swing_highs and not swing_lows:
                return None

            current_bar = bars[-1]
            current_price = current_bar['c']

            # Check for bullish BOS (break above swing high)
            if swing_highs:
                most_recent_high = swing_highs[-1]

                # BOS if current price > swing high
                if current_price > most_recent_high.price:
                    # Check volume confirmation (volume > average)
                    avg_volume = sum(bar['v'] for bar in bars[-10:]) / 10
                    confirmation = current_bar['v'] > avg_volume

                    return {
                        'type': 'bullish',
                        'break_price': current_price,
                        'break_time': datetime.fromtimestamp(current_bar['t'] / 1000, tz=ET_TZ),
                        'swing_point': most_recent_high,
                        'confirmation': confirmation,
                        'volume_ratio': current_bar['v'] / avg_volume if avg_volume > 0 else 0
                    }

            # Check for bearish BOS (break below swing low)
            if swing_lows:
                most_recent_low = swing_lows[-1]

                if current_price < most_recent_low.price:
                    avg_volume = sum(bar['v'] for bar in bars[-10:]) / 10
                    confirmation = current_bar['v'] > avg_volume

                    return {
                        'type': 'bearish',
                        'break_price': current_price,
                        'break_time': datetime.fromtimestamp(current_bar['t'] / 1000, tz=ET_TZ),
                        'swing_point': most_recent_low,
                        'confirmation': confirmation,
                        'volume_ratio': current_bar['v'] / avg_volume if avg_volume > 0 else 0
                    }

            return None

        except Exception as e:
            logger.error(f"Error detecting BOS for {ticker} {timeframe}: {e}")
            return None

    async def find_demand_supply_zones(
        self,
        ticker: str,
        timeframe: str,
        zone_type: str = 'demand',  # 'demand' or 'supply'
        lookback_bars: int = 50
    ) -> Optional[Dict]:
        """
        Find demand/supply zones (Order Blocks)

        Demand Zone: Area where price found support (last swing low area)
        Supply Zone: Area where price found resistance (last swing high area)

        Returns:
            {
                'zone_type': 'demand' | 'supply',
                'low': float,
                'high': float,
                'strength': float (0-100),
                'touches': int,
                'last_touch_time': datetime
            }
        """
        try:
            multiplier, timespan = self._map_timeframe(timeframe)
            bars = await self._fetch_bars(ticker, multiplier, timespan, lookback_bars)

            if not bars or len(bars) < 10:
                return None

            if zone_type == 'demand':
                # Find last swing low
                swing_lows = self._find_swing_lows(bars, window=5)

                if not swing_lows:
                    return None

                last_swing = swing_lows[-1]

                # Define zone as swing low +/- 2%
                zone_low = last_swing.price * 0.98
                zone_high = last_swing.price * 1.02

                # Count touches (how many times price came to zone)
                touches = 0
                last_touch = None

                for bar in bars:
                    if bar['l'] <= zone_high and bar['h'] >= zone_low:
                        touches += 1
                        last_touch = datetime.fromtimestamp(bar['t'] / 1000, tz=ET_TZ)

                # Strength based on touches and volume at zone
                strength = min(touches * 20, 100)  # More touches = stronger zone

                return {
                    'zone_type': 'demand',
                    'low': zone_low,
                    'high': zone_high,
                    'swing_point': last_swing.price,
                    'strength': strength,
                    'touches': touches,
                    'last_touch_time': last_touch
                }

            else:  # supply
                swing_highs = self._find_swing_highs(bars, window=5)

                if not swing_highs:
                    return None

                last_swing = swing_highs[-1]

                zone_low = last_swing.price * 0.98
                zone_high = last_swing.price * 1.02

                touches = 0
                last_touch = None

                for bar in bars:
                    if bar['l'] <= zone_high and bar['h'] >= zone_low:
                        touches += 1
                        last_touch = datetime.fromtimestamp(bar['t'] / 1000, tz=ET_TZ)

                strength = min(touches * 20, 100)

                return {
                    'zone_type': 'supply',
                    'low': zone_low,
                    'high': zone_high,
                    'swing_point': last_swing.price,
                    'strength': strength,
                    'touches': touches,
                    'last_touch_time': last_touch
                }

        except Exception as e:
            logger.error(f"Error finding {zone_type} zones for {ticker}: {e}")
            return None

    def _find_swing_highs(self, bars: List[Dict], window: int = 5) -> List[SwingPoint]:
        """Find swing high points"""
        swing_highs = []

        for i in range(window, len(bars) - window):
            current = bars[i]

            # Check if this is a swing high (higher than bars before and after)
            is_swing_high = True

            for j in range(i - window, i + window + 1):
                if j == i:
                    continue
                if bars[j]['h'] >= current['h']:
                    is_swing_high = False
                    break

            if is_swing_high:
                swing_highs.append(SwingPoint(
                    price=current['h'],
                    timestamp=datetime.fromtimestamp(current['t'] / 1000, tz=ET_TZ),
                    type='high',
                    volume=current['v']
                ))

        return swing_highs

    def _find_swing_lows(self, bars: List[Dict], window: int = 5) -> List[SwingPoint]:
        """Find swing low points"""
        swing_lows = []

        for i in range(window, len(bars) - window):
            current = bars[i]

            # Check if this is a swing low (lower than bars before and after)
            is_swing_low = True

            for j in range(i - window, i + window + 1):
                if j == i:
                    continue
                if bars[j]['l'] <= current['l']:
                    is_swing_low = False
                    break

            if is_swing_low:
                swing_lows.append(SwingPoint(
                    price=current['l'],
                    timestamp=datetime.fromtimestamp(current['t'] / 1000, tz=ET_TZ),
                    type='low',
                    volume=current['v']
                ))

        return swing_lows

    def _map_timeframe(self, timeframe: str) -> Tuple[int, str]:
        """Map timeframe string to Polygon API parameters"""
        mapping = {
            'weekly': (1, 'week'),
            'daily': (1, 'day'),
            '4h': (4, 'hour'),
            '1h': (1, 'hour'),
            '15m': (15, 'minute'),
            '5m': (5, 'minute')
        }

        if timeframe not in mapping:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        return mapping[timeframe]

    async def _fetch_bars(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        limit: int
    ) -> Optional[List[Dict]]:
        """Fetch OHLCV bars from Polygon"""
        try:
            # Calculate date range
            to_date = datetime.now(ET_TZ)

            # Calculate from_date based on timespan
            if timespan == 'week':
                from_date = to_date - timedelta(weeks=limit + 5)
            elif timespan == 'day':
                from_date = to_date - timedelta(days=limit + 10)
            elif timespan == 'hour':
                from_date = to_date - timedelta(hours=limit * multiplier + 50)
            else:  # minute
                from_date = to_date - timedelta(minutes=limit * multiplier + 100)

            # Fetch aggregates
            bars = await self.polygon.get_aggregates(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_date=from_date.strftime('%Y-%m-%d'),
                to_date=to_date.strftime('%Y-%m-%d')
            )

            if not bars:
                return None

            # Return most recent bars
            return bars[-limit:] if len(bars) >= limit else bars

        except Exception as e:
            logger.error(f"Error fetching bars for {ticker}: {e}")
            return None
