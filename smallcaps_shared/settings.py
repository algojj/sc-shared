"""Base settings for microservices"""

import os
import logging
from typing import Optional
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseSettings(BaseModel):
    """Base settings that all scanners inherit"""
    min_price: float = 0.5
    max_price: float = 30.0
    min_volume: int = 100000
    min_gap_percent: float = 5.0
    max_results: int = 50
    exclude_otc: bool = True
    exclude_warrants: bool = True


class GapScannerSettings(BaseSettings):
    """Settings specific to gap scanner"""
    min_price: float = 0.5
    max_price: float = 30.0
    min_volume: int = 1000000
    min_premarket_volume: int = 350000
    min_gap_percent: float = 10.0
    max_float_millions: float = 200.0


class MomentumScannerSettings(BaseSettings):
    """Settings specific to momentum scanner"""
    min_price: float = 1.0
    max_price: float = 25.0
    min_volume: int = 1000000  # 1M minimum like traditional momentum
    max_volume: int = None  # No max volume limit
    min_float: int = 2000000  # 2M float minimum
    max_float: int = 30000000  # 30M float maximum
    min_gap_percent: float = 3.0  # Lower threshold for momentum
    max_spread: float = None  # No spread filter
    break_high_only: bool = False


class BreakoutScannerSettings(BaseSettings):
    """Settings specific to breakout scanner"""
    min_price: float = 0.5
    max_price: float = 30.0
    min_volume: int = 500000
    min_gap_percent: float = 10.0
    max_fade_percent: float = 30.0


class GapFadeScannerSettings(BaseSettings):
    """Settings specific to gap-fade scanner (Day 2/3 shorts)"""
    min_price: float = 1.0
    max_price: float = 30.0
    min_volume: int = 500000
    min_gap_percent: float = 15.0  # Must have gapped up significantly yesterday
    max_fade_percent: float = 50.0  # Maximum fade percentage allowed
    min_fade_percent: float = 3.0   # Minimum fade to show weakness
    max_float_millions: float = 100.0  # Small-mid cap focus for better fades


class DatabaseSettings(BaseModel):
    """Database connection settings"""
    db_host: str = os.getenv("DB_HOST", "postgres")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_user: str = os.getenv("DB_USER", "scanner_user")
    db_password: str = os.getenv("DB_PASSWORD", "scanner_pass")
    db_name: str = os.getenv("DB_NAME", "scanner")


def get_scanner_settings(scanner_type: str) -> BaseSettings:
    """Get settings for specific scanner type"""
    if scanner_type == "gap" or scanner_type == "market":
        return GapScannerSettings()
    elif scanner_type == "momentum":
        return MomentumScannerSettings()
    elif scanner_type == "breakout":
        return BreakoutScannerSettings()
    elif scanner_type == "gapfade":
        return GapFadeScannerSettings()
    else:
        logger.warning(f"Unknown scanner type: {scanner_type}, using base settings")
        return BaseSettings()