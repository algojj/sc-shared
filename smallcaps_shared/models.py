"""Shared data models for all microservices"""

from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, model_validator


class ScannerType(str, Enum):
    MARKET = "market"
    MOMENTUM = "momentum"
    GAPFADE = "gapfade"
    BREAKOUT = "breakout"


class ScanResult(BaseModel):
    """Standard scan result model used by all scanners"""
    ticker: str = Field(..., description="Stock ticker symbol")
    price: float = Field(..., description="Current stock price")
    current_price: Optional[float] = Field(None, description="Current price (alias for compatibility)")
    change_percent: Optional[float] = Field(None, description="Price change percentage from previous close")
    gap_percent: Optional[float] = Field(None, description="Gap percentage from previous close")
    volume: str = Field(..., description="Current volume (formatted as string)")
    volume_premarket: Optional[int] = Field(None, description="Premarket volume")
    float_shares: Optional[float] = Field(None, description="Float shares in millions")
    market_cap_millions: Optional[float] = Field(None, description="Market cap in millions")
    prev_close: Optional[float] = Field(None, description="Previous close price")
    high_spike_percent: Optional[float] = Field(None, description="Percentage below day high")
    low_bounce_percent: Optional[float] = Field(None, description="Percentage above day low")
    country_code: Optional[str] = Field(None, description="Country code (US, CN, etc.)")
    sector: Optional[str] = Field(None, description="Company sector/industry from SIC code")

    # Additional market data fields
    day_high: Optional[float] = Field(None, description="Day high price")
    day_low: Optional[float] = Field(None, description="Day low price")
    exchange: Optional[str] = Field(None, description="Exchange where stock is traded")
    shares_outstanding: Optional[float] = Field(None, description="Shares outstanding")
    vwap: Optional[float] = Field(None, description="Volume Weighted Average Price from Polygon")

    # Scanner-specific fields
    momentum_score: Optional[float] = Field(None, description="Momentum score (momentum scanner)")
    breakout_signal: Optional[str] = Field(None, description="Breakout signal type")
    fade_probability: Optional[float] = Field(None, description="Gap fade probability")
    change_5m: Optional[float] = Field(None, description="5-minute price change percentage")

    # DilutionTracker / Ownership fields
    short_interest: Optional[float] = Field(None, description="Short interest percentage from DilutionTracker")
    institutional_ownership: Optional[float] = Field(None, description="Institutional ownership percentage")
    insider_ownership: Optional[float] = Field(None, description="Insider ownership percentage")

    # 10-minute change fields
    chg_10m: Optional[float] = Field(None, description="Price change percentage over last 10 minutes")
    vol_10m: Optional[int] = Field(None, description="Volume over last 10 minutes")

    # Timestamp fields
    timestamp: Optional[str] = Field(None, description="ISO timestamp when the data was generated")
    alert_time: Optional[str] = Field(None, description="Time when alert was triggered")

    # Session-specific fields
    session: Optional[str] = Field(None, description="Market session (premarket, rth, afterhours)")
    session_gap_percent: Optional[float] = Field(None, description="Gap percentage calculated for current session")
    alert_type: Optional[str] = Field(None, description="Alert type (standard, super_gap, extreme_gap)")

    # Multi-day run context fields (for PRIMER_DIA_ROJO detection)
    consecutive_green_days: Optional[int] = Field(None, description="Number of consecutive green days before today")
    extension_from_run_start: Optional[float] = Field(None, description="Extension % from start of multi-day run")
    is_first_red_day: Optional[bool] = Field(None, description="True if today is first red day after >=2 green days")
    volume_vs_yesterday: Optional[float] = Field(None, description="Volume ratio vs yesterday (e.g., 0.8 = 80% of yesterday)")
    never_recovered_open: Optional[bool] = Field(None, description="True if intraday high never exceeded open")
    has_lower_highs: Optional[bool] = Field(None, description="True if making lower highs intraday")

    # Gap statistics (historical gap performance)
    gap_stats: Optional[Dict[str, Any]] = Field(None, description="Historical gap statistics (gaps above threshold, avg gap, etc.)")

    @model_validator(mode='after')
    def set_current_price(self):
        """Auto-populate current_price from price if not set"""
        if self.current_price is None:
            self.current_price = self.price
        return self


class HealthStatus(BaseModel):
    """Health check response model"""
    service: str
    status: str
    timestamp: str
    version: str = "1.0.0"
    last_update: Optional[str] = None
    data_count: int = 0