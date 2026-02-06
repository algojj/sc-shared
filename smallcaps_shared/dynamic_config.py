"""
Dynamic Configuration System with Redis
Allows hot reload of configuration without container restarts
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone

from .redis_client import RedisClient

logger = logging.getLogger(__name__)


class DynamicConfig:
    """
    Dynamic configuration system that loads from Redis with hot reload
    Maintains current hardcoded values as defaults if Redis is unavailable
    """

    def __init__(self, redis_client: RedisClient, config_namespace: str = "config"):
        self.redis_client = redis_client
        self.config_namespace = config_namespace
        self.config_cache: Dict[str, Any] = {}
        self.subscribers: Dict[str, list] = {}
        self.last_update = None

        # Default values extracted from current codebase - DO NOT CHANGE
        self.defaults = {
            # GapFade Scanner defaults (from scanner.py hardcoded values)
            "gapfade": {
                "process_limit": 50,                 # Line 75: losers[:50]
                "min_fade_percent": 3.0,            # Line 107/121: settings.min_fade_percent
                "min_gap_percent": 15.0,            # Line 121: settings.min_gap_percent
                "cache_ttl": 300,                   # Line 158: ttl=300
                "scan_interval": 180,               # Line 25: interval=180
                "retry_delay": 10,                  # Line 36: sleep(10)
                "max_results": 50,                  # Line 152: settings.max_results

                # Scoring thresholds from _calculate_fade_probability (lines 184-220)
                "scoring": {
                    "gap_thresholds": [50, 30, 20, 15],     # Lines 184-191
                    "gap_scores": [40, 30, 20, 15],
                    "fade_thresholds": [15, 10, 7, 5, 3],   # Lines 195-204
                    "fade_scores": [30, 25, 20, 15, 10],
                    "volume_thresholds": [2000000, 1000000, 500000],  # Lines 207-212
                    "volume_scores": [20, 15, 10],
                    "price_sweet_spot": [5, 15],            # Line 215: 5 <= price <= 15
                    "price_acceptable": [2, 20],            # Line 217: 2 <= price <= 20
                    "price_scores": [10, 5]
                }
            },

            # Settings from settings.py - preserve exact values
            "base_settings": {
                "min_price": 0.5,
                "max_price": 30.0,
                "min_volume": 100000,
                "min_gap_percent": 5.0,
                "max_results": 50,
                "exclude_otc": True,
                "exclude_warrants": True
            },

            "gap_scanner": {
                "min_price": 0.5,
                "max_price": 30.0,
                "min_volume": 1000000,
                "min_premarket_volume": 350000,
                "min_gap_percent": 10.0,
                "max_float_millions": 200.0
            },

            # Gap scanner specific configs (from gap-scanner scanner.py)
            "gap": {
                "movers_process_limit": 100,         # Line 43: all_movers[:100]
                "nhod_batch_limit": 50,             # Line 47: ticker_list[:50]
                "max_results": 50,                  # Uses settings.max_results, but configurable
                "sample_results_count": 3           # Line 122: results[:3]
            },

            # Momentum scanner specific configs (from momentum-scanner scanner.py)
            "momentum": {
                "movers_process_limit": 100,        # Line 46: all_movers[:100]
                "nhod_batch_limit": 50,             # Line 50: ticker_list[:50]
                "sample_results_count": 3,          # Line 146: results[:3]
                "time_window_seconds": 300,         # 5 minutes in seconds
                "min_data_window_seconds": 270,     # 4.5 minutes minimum for calculation
                "scoring": {
                    "volume_weight": 0.7,           # Line 214: volume_millions * 0.7
                    "gap_weight": 0.3,              # Line 214: gap_percent * 0.3
                    "max_score": 100.0              # Line 216: Cap at 100
                }
            },

            # Breakout scanner specific configs (from breakout-scanner scanner.py)
            "breakout": {
                "max_results": 20,                  # Line 78: signals[:20]
                "min_score_threshold": 40,          # Line 140: score < 40
                "scan_interval": 60,                # Default interval in seconds
                "error_delay": 10,                  # Line 367: sleep(10) on error
                "cache_ttl": 300,                   # Line 408: ttl=300
                "data_sources": ["gap", "momentum"], # Line 443-449: data source priority
                "scoring": {
                    "gap_thresholds": [20, 15, 10],     # Lines 181-186: gap scoring
                    "gap_scores": [25, 20, 15],
                    "volume_thresholds": [5, 3, 2, 1.5], # Lines 189-196: volume scoring
                    "volume_scores": [25, 20, 15, 10],
                    "float_thresholds": [100, 50, 25, 10], # Lines 199-206: float scoring
                    "float_scores": [20, 15, 10, 5],
                    "distance_thresholds": [5, 10, 20],   # Lines 210-215: distance scoring
                    "distance_scores": [15, 10, 5],
                    "market_cap_sweet_min": 10_000_000,   # Line 220: sweet spot min
                    "market_cap_sweet_max": 500_000_000,  # Line 220: sweet spot max
                    "market_cap_good_max": 1_000_000_000, # Line 222: good max
                    "market_cap_ok_max": 2_000_000_000,   # Line 224: ok max
                    "market_cap_scores": [15, 10, 5]      # Lines 221-225: mcap scores
                }
            },

            "momentum_scanner": {
                "min_price": 1.0,
                "max_price": 25.0,
                "min_volume": 1000000,
                "max_volume": None,
                "min_float": 2000000,
                "max_float": 30000000,
                "min_gap_percent": 3.0,
                "max_spread": None,
                "break_high_only": False
            },

            "breakout_scanner": {
                "min_price": 0.5,
                "max_price": 30.0,
                "min_volume": 500000,
                "min_gap_percent": 10.0,
                "max_fade_percent": 30.0
            },

            "gapfade_scanner": {
                "min_price": 1.0,
                "max_price": 30.0,
                "min_volume": 500000,
                "min_gap_percent": 15.0,
                "max_fade_percent": 50.0,
                "min_fade_percent": 3.0,
                "max_float_millions": 100.0
            },

            # Alerts service (from alerts-service main.py)
            "alerts": {
                "scan_interval": 1,                  # Line 35: SCAN_INTERVAL = 1
                "premarket_gap_threshold": 20.0,     # Line 50
                "rth_gap_threshold": 50.0,          # Line 51
                "rapid_move_threshold": 3.0,        # Line 52
                "gap_check_interval": 60,           # Line 200: sleep(60)
                "gap_retry_delay": 60,              # Line 204: sleep(60)
                "process_alerts_interval": 1,       # Line 226: sleep(1)
                "alert_retry_delay": 5,             # Line 230: sleep(5)
                "monitor_retry_delay": 10,          # Line 144: sleep(10)
                "daily_ttl": 86400,                 # Line 242: expire 86400
                "latest_alert_ttl": 3600            # Line 246: ttl=3600
            }
        }

    async def initialize(self):
        """Initialize config system and load from Redis"""
        try:
            await self._load_config_from_redis()
            logger.info(f"✅ Dynamic config initialized for namespace: {self.config_namespace}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from Redis, using defaults: {e}")
            self.config_cache = self.defaults.copy()

    async def get(self, path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation path
        Example: get('gapfade.min_fade_percent') returns 3.0
        """
        try:
            keys = path.split('.')
            value = self.config_cache

            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    # Fall back to defaults
                    value = self.defaults
                    for k in keys:
                        if isinstance(value, dict) and k in value:
                            value = value[k]
                        else:
                            return default
                    break

            return value
        except Exception as e:
            logger.error(f"❌ Error getting config {path}: {e}")
            return default

    async def set(self, path: str, value: Any) -> bool:
        """
        Set configuration value and update Redis
        Example: set('gapfade.min_fade_percent', 5.0)
        """
        try:
            # Update local cache
            self._set_nested_dict(self.config_cache, path.split('.'), value)

            # Update Redis
            await self._save_config_to_redis()

            # Notify subscribers
            await self._notify_subscribers(path, value)

            logger.info(f"✅ Config updated: {path} = {value}")
            return True

        except Exception as e:
            logger.error(f"❌ Error setting config {path}: {e}")
            return False

    async def subscribe(self, path: str, callback: Callable[[str, Any], None]):
        """Subscribe to configuration changes"""
        if path not in self.subscribers:
            self.subscribers[path] = []
        self.subscribers[path].append(callback)
        logger.info(f"📡 Subscribed to config changes: {path}")

    async def reload(self) -> bool:
        """Force reload configuration from Redis"""
        try:
            await self._load_config_from_redis()
            logger.info("🔄 Config reloaded from Redis")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to reload config: {e}")
            return False

    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration"""
        return self.config_cache.copy()

    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration (for debugging)"""
        return self.defaults.copy()

    async def reset_to_defaults(self) -> bool:
        """Reset all configuration to defaults"""
        try:
            self.config_cache = self.defaults.copy()
            await self._save_config_to_redis()
            logger.info("🔄 Config reset to defaults")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to reset config: {e}")
            return False

    # Internal methods

    async def _load_config_from_redis(self):
        """Load configuration from Redis"""
        try:
            config_key = f"{self.config_namespace}:settings"
            config_data = await self.redis_client.get(config_key)

            if config_data:
                self.config_cache = config_data
                self.last_update = datetime.now(timezone.utc)
                logger.info(f"📥 Loaded config from Redis: {config_key}")
            else:
                # Initialize with defaults
                self.config_cache = self.defaults.copy()
                await self._save_config_to_redis()
                logger.info("📝 Initialized Redis with default config")

        except Exception as e:
            logger.warning(f"⚠️ Redis config load failed, using defaults: {e}")
            self.config_cache = self.defaults.copy()

    async def _save_config_to_redis(self):
        """Save configuration to Redis"""
        config_key = f"{self.config_namespace}:settings"
        await self.redis_client.set(config_key, self.config_cache, ttl=None)  # No expiry
        self.last_update = datetime.now(timezone.utc)

    async def _notify_subscribers(self, path: str, value: Any):
        """Notify subscribers of configuration changes"""
        # Exact path subscribers
        if path in self.subscribers:
            for callback in self.subscribers[path]:
                try:
                    callback(path, value)
                except Exception as e:
                    logger.error(f"❌ Subscriber callback error: {e}")

        # Wildcard subscribers (e.g., "gapfade.*")
        path_parts = path.split('.')
        for i in range(len(path_parts)):
            wildcard_path = '.'.join(path_parts[:i+1]) + '.*'
            if wildcard_path in self.subscribers:
                for callback in self.subscribers[wildcard_path]:
                    try:
                        callback(path, value)
                    except Exception as e:
                        logger.error(f"❌ Wildcard subscriber callback error: {e}")

    def _set_nested_dict(self, d: dict, keys: list, value: Any):
        """Set nested dictionary value"""
        for key in keys[:-1]:
            if key not in d:
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value


class ConfigManager:
    """
    Singleton config manager for easy access across services
    """
    _instance: Optional[DynamicConfig] = None

    @classmethod
    async def get_instance(cls, redis_client: RedisClient = None, namespace: str = "config") -> DynamicConfig:
        """Get or create config manager instance"""
        if cls._instance is None:
            if redis_client is None:
                raise ValueError("Redis client required for first initialization")
            cls._instance = DynamicConfig(redis_client, namespace)
            await cls._instance.initialize()
        return cls._instance

    @classmethod
    def reset():
        """Reset instance (for testing)"""
        cls._instance = None


# Convenience functions for common operations
async def get_config(path: str, default: Any = None, redis_client: RedisClient = None) -> Any:
    """Quick config getter"""
    config = await ConfigManager.get_instance(redis_client)
    return await config.get(path, default)

async def set_config(path: str, value: Any, redis_client: RedisClient = None) -> bool:
    """Quick config setter"""
    config = await ConfigManager.get_instance(redis_client)
    return await config.set(path, value)