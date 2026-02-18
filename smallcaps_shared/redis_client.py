"""Lightweight Redis client for microservices with Sentinel support"""

import json
import os
from typing import Any, Optional, List
from datetime import datetime, timedelta, timezone
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel
import logging

logger = logging.getLogger(__name__)


class CacheKeys:
    """Cache key constants"""

    @staticmethod
    def scan_results(scanner_type: str = "market") -> str:
        return f"scan_results:{scanner_type}"

    @staticmethod
    def health_check(service: str) -> str:
        return f"health:{service}"

    @staticmethod
    def seen_tickers(scanner_type: str) -> str:
        """Cache key for tracking seen tickers to avoid duplicate alerts"""
        return f"seen_tickers:{scanner_type}"


class RedisClient:
    """Lightweight Redis client for microservices with Sentinel support

    Supports two modes:
    1. Direct connection: REDIS_HOST + REDIS_PORT (default)
    2. Sentinel mode: REDIS_SENTINEL_HOSTS + REDIS_MASTER_NAME

    Environment variables:
        REDIS_HOST: Direct Redis host (default: localhost)
        REDIS_PORT: Direct Redis port (default: 6379)
        REDIS_DB: Redis database number (default: 0)
        REDIS_USE_SENTINEL: Set to "true" to use Sentinel (default: false)
        REDIS_SENTINEL_HOSTS: Comma-separated sentinel hosts (e.g., "sentinel1:26379,sentinel2:26379")
        REDIS_MASTER_NAME: Sentinel master name (default: mymaster)
    """

    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.sentinel: Optional[Sentinel] = None
        self.is_connected = False

        # Get Redis config from environment
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", "6379"))
        self.db = int(os.getenv("REDIS_DB", "0"))

        # Sentinel config
        self.use_sentinel = os.getenv("REDIS_USE_SENTINEL", "false").lower() == "true"
        self.sentinel_hosts_str = os.getenv("REDIS_SENTINEL_HOSTS", "redis-sentinel:26379")
        self.master_name = os.getenv("REDIS_MASTER_NAME", "mymaster")

    async def connect(self) -> bool:
        """Connect to Redis (automatically uses Sentinel if configured)"""
        try:
            if self.use_sentinel:
                return await self._connect_sentinel()
            else:
                return await self._connect_direct()

        except Exception as e:
            logger.error(f"[REDIS] Connection failed: {e}", exc_info=True)
            self.is_connected = False
            return False

    async def _connect_direct(self) -> bool:
        """Connect directly to Redis host"""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=30
            )

            # Test connection
            await self.redis_client.ping()
            self.is_connected = True
            logger.info(f"[REDIS] Connected directly to {self.host}:{self.port}")
            return True

        except Exception as e:
            logger.error(f"[REDIS] Direct connection failed to {self.host}:{self.port}: {e}", exc_info=True)
            self.is_connected = False
            return False

    async def _connect_sentinel(self) -> bool:
        """Connect via Sentinel for automatic master discovery"""
        try:
            # Parse sentinel hosts
            sentinel_hosts = []
            for host_port in self.sentinel_hosts_str.split(","):
                host_port = host_port.strip()
                if ":" in host_port:
                    host, port = host_port.rsplit(":", 1)
                    sentinel_hosts.append((host, int(port)))
                else:
                    sentinel_hosts.append((host_port, 26379))

            logger.info(f"[REDIS-SENTINEL] Connecting to sentinels: {sentinel_hosts}")
            logger.info(f"[REDIS-SENTINEL] Master name: {self.master_name}")

            # Create sentinel instance
            self.sentinel = Sentinel(
                sentinel_hosts,
                socket_timeout=5.0,
                decode_responses=True
            )

            # Get master connection
            self.redis_client = self.sentinel.master_for(
                self.master_name,
                socket_timeout=5.0,
                db=self.db
            )

            # Test connection and log master info
            await self.redis_client.ping()

            # Get master address for logging
            try:
                master_info = await self.sentinel.discover_master(self.master_name)
                logger.info(f"[REDIS-SENTINEL] Connected to master at {master_info[0]}:{master_info[1]}")
            except Exception:
                logger.info("[REDIS-SENTINEL] Connected to master (address discovery failed)")

            self.is_connected = True
            return True

        except Exception as e:
            logger.error(f"[REDIS-SENTINEL] Connection failed to {self.sentinel_hosts_str}: {e}", exc_info=True)
            self.is_connected = False
            return False

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            self.is_connected = False
            logger.info("[REDIS] Connection closed")

    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Set value with TTL (use ttl=0 or None for no expiry)"""
        if not self.is_connected:
            return False

        try:
            # Handle None values
            if value is None:
                logger.warning(f"[REDIS] Attempted to set None value for key {key}, skipping")
                return False

            if isinstance(value, (dict, list)):
                value = json.dumps(value, default=str)

            # Handle TTL - None or 0 means no expiry
            if ttl is None or ttl == 0:
                await self.redis_client.set(key, value)
                logger.debug(f"[REDIS] Set {key} (no expiry)")
            else:
                await self.redis_client.setex(key, ttl, value)
                logger.debug(f"[REDIS] Set {key} (TTL: {ttl}s)")
            return True

        except Exception as e:
            logger.error(f"[REDIS] SET failed for {key}: {e}", exc_info=True)
            return False

    async def get(self, key: str) -> Optional[Any]:
        """Get value and deserialize if JSON"""
        if not self.is_connected:
            return None

        try:
            value = await self.redis_client.get(key)
            if value is None:
                return None

            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value

        except Exception as e:
            logger.error(f"[REDIS] GET failed for {key}: {e}", exc_info=True)
            return None

    async def publish(self, channel: str, message: Any) -> bool:
        """Publish message to channel"""
        if not self.is_connected:
            return False

        try:
            if isinstance(message, (dict, list)):
                message = json.dumps(message)

            await self.redis_client.publish(channel, message)
            logger.debug(f"[REDIS] Published to {channel}")
            return True

        except Exception as e:
            logger.error(f"[REDIS] PUBLISH failed to {channel}: {e}", exc_info=True)
            return False

    async def health_update(self, service: str, data_count: int = 0) -> bool:
        """Update service health status"""
        health_data = {
            "service": service,
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "last_update": datetime.now(timezone.utc).isoformat(),
            "data_count": data_count
        }

        return await self.set(
            CacheKeys.health_check(service),
            health_data,
            ttl=300  # 5 minutes
        )

    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern using SCAN (non-blocking)"""
        if not self.is_connected:
            return []

        try:
            result = []
            cursor = 0
            while True:
                cursor, batch = await self.redis_client.scan(cursor, match=pattern, count=100)
                result.extend(key.decode() if isinstance(key, bytes) else key for key in batch)
                if cursor == 0:
                    break
            return result
        except Exception as e:
            logger.error(f"[REDIS] SCAN failed for pattern {pattern}: {e}", exc_info=True)
            return []

    async def lpush(self, key: str, *values) -> int:
        """Left push values to list"""
        if not self.is_connected:
            return 0

        try:
            # Convert values to JSON if they are dicts/lists
            json_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    json_values.append(json.dumps(value, default=str))
                else:
                    json_values.append(str(value))

            result = await self.redis_client.lpush(key, *json_values)
            logger.debug(f"[REDIS] LPUSH to {key}: {len(json_values)} items")
            return result

        except Exception as e:
            logger.error(f"[REDIS] LPUSH failed for {key}: {e}", exc_info=True)
            return 0

    async def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration"""
        if not self.is_connected:
            return False

        try:
            result = await self.redis_client.expire(key, seconds)
            logger.debug(f"[REDIS] Set expiration for {key}: {seconds}s")
            return bool(result)

        except Exception as e:
            logger.error(f"[REDIS] EXPIRE failed for {key}: {e}", exc_info=True)
            return False

    async def lrange(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get range of list elements"""
        if not self.is_connected:
            return []

        try:
            result = await self.redis_client.lrange(key, start, end)
            # Try to parse JSON for each item
            parsed_result = []
            for item in result:
                try:
                    parsed_result.append(json.loads(item))
                except (json.JSONDecodeError, TypeError):
                    parsed_result.append(item)

            logger.debug(f"[REDIS] LRANGE {key}: {len(parsed_result)} items")
            return parsed_result

        except Exception as e:
            logger.error(f"[REDIS] LRANGE failed for {key}: {e}", exc_info=True)
            return []

    async def sadd(self, key: str, *members) -> int:
        """Add members to a set"""
        if not self.is_connected:
            return 0

        try:
            return await self.redis_client.sadd(key, *members)
        except Exception as e:
            logger.error(f"[REDIS] SADD failed for {key}: {e}", exc_info=True)
            return 0

    async def smembers(self, key: str) -> set:
        """Get all members of a set"""
        if not self.is_connected:
            return set()

        try:
            members = await self.redis_client.smembers(key)
            return {m.decode() if isinstance(m, bytes) else m for m in members}
        except Exception as e:
            logger.error(f"[REDIS] SMEMBERS failed for {key}: {e}", exc_info=True)
            return set()

    async def zadd(self, key: str, mapping: dict) -> int:
        """Add members with scores to a sorted set"""
        if not self.is_connected:
            return 0

        try:
            return await self.redis_client.zadd(key, mapping)
        except Exception as e:
            logger.error(f"[REDIS] ZADD failed for {key}: {e}", exc_info=True)
            return 0

    async def zrange(self, key: str, start: int, end: int, withscores: bool = False) -> list:
        """Get range of members from sorted set"""
        if not self.is_connected:
            return []

        try:
            results = await self.redis_client.zrange(key, start, end, withscores=withscores)
            if withscores:
                return [(m.decode() if isinstance(m, bytes) else m, s) for m, s in results]
            else:
                return [m.decode() if isinstance(m, bytes) else m for m in results]
        except Exception as e:
            logger.error(f"[REDIS] ZRANGE failed for {key}: {e}", exc_info=True)
            return []

    async def rpush(self, key: str, *values) -> int:
        """Push one or more values to the end of a list"""
        if not self.is_connected:
            return 0

        try:
            # Convert values to JSON strings if they are dicts/lists
            processed_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    processed_values.append(json.dumps(value, default=str))
                else:
                    processed_values.append(str(value))

            result = await self.redis_client.rpush(key, *processed_values)
            logger.debug(f"[REDIS] RPUSH to {key}: {len(processed_values)} items")
            return result
        except Exception as e:
            logger.error(f"[REDIS] RPUSH failed for {key}: {e}", exc_info=True)
            return 0

    async def xadd(self, stream: str, fields: dict, message_id: str = "*") -> str:
        """Add an entry to a Redis stream"""
        if not self.is_connected:
            return ""

        try:
            # Convert dict/list values to JSON strings
            processed_fields = {}
            for key, value in fields.items():
                if isinstance(value, (dict, list)):
                    processed_fields[key] = json.dumps(value, default=str)
                else:
                    processed_fields[key] = str(value)

            result = await self.redis_client.xadd(stream, processed_fields, id=message_id)
            logger.debug(f"[REDIS] XADD to stream {stream}: {len(processed_fields)} fields")
            return result
        except Exception as e:
            logger.error(f"[REDIS] XADD failed for stream {stream}: {e}", exc_info=True)
            return ""

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim a list to the specified range"""
        if not self.is_connected:
            return False

        try:
            await self.redis_client.ltrim(key, start, stop)
            logger.debug(f"[REDIS] LTRIM {key} to range [{start}, {stop}]")
            return True
        except Exception as e:
            logger.error(f"[REDIS] LTRIM failed for {key}: {e}", exc_info=True)
            return False

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment the value of a key by amount"""
        if not self.is_connected:
            return 0

        try:
            result = await self.redis_client.incrby(key, amount)
            logger.debug(f"[REDIS] INCR {key} by {amount}")
            return result
        except Exception as e:
            logger.error(f"[REDIS] INCR failed for {key}: {e}", exc_info=True)
            return 0

    async def llen(self, key: str) -> int:
        """Get the length of a list"""
        if not self.is_connected:
            return 0

        try:
            result = await self.redis_client.llen(key)
            logger.debug(f"[REDIS] LLEN {key}: {result}")
            return result
        except Exception as e:
            logger.error(f"[REDIS] LLEN failed for {key}: {e}", exc_info=True)
            return 0

    async def brpop(self, key: str, timeout: int = 0):
        """Blocking right pop from list. Returns (key, value) tuple or None on timeout."""
        if not self.is_connected:
            return None

        try:
            result = await self.redis_client.brpop(key, timeout=timeout)
            if result:
                logger.debug(f"[REDIS] BRPOP {key}: got value")
                return result
            return None
        except (redis.TimeoutError, TimeoutError):
            # BRPOP timeout is expected when queue is empty — not an error
            logger.debug(f"[REDIS] BRPOP timeout for {key} (queue empty)")
            return None
        except Exception as e:
            logger.error(f"[REDIS] BRPOP failed for {key}: {e}", exc_info=True)
            return None

    # ========================================================================
    # HASH OPERATIONS
    # ========================================================================

    async def hset(self, key: str, field: str, value: Any) -> bool:
        """Set the value of a hash field"""
        if not self.is_connected:
            return False

        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value, default=str)

            await self.redis_client.hset(key, field, value)
            logger.debug(f"[REDIS] HSET {key}:{field}")
            return True
        except Exception as e:
            logger.error(f"[REDIS] HSET failed for {key}:{field}: {e}", exc_info=True)
            return False

    async def hget(self, key: str, field: str) -> Optional[Any]:
        """Get the value of a hash field"""
        if not self.is_connected:
            return None

        try:
            value = await self.redis_client.hget(key, field)
            if value is None:
                return None

            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"[REDIS] HGET failed for {key}:{field}: {e}", exc_info=True)
            return None

    async def hgetall(self, key: str) -> dict:
        """Get all fields and values of a hash"""
        if not self.is_connected:
            return {}

        try:
            result = await self.redis_client.hgetall(key)
            if not result:
                return {}

            # Try to deserialize JSON values
            parsed = {}
            for field, value in result.items():
                field_str = field.decode() if isinstance(field, bytes) else field
                value_str = value.decode() if isinstance(value, bytes) else value
                try:
                    parsed[field_str] = json.loads(value_str)
                except (json.JSONDecodeError, TypeError):
                    parsed[field_str] = value_str

            logger.debug(f"[REDIS] HGETALL {key}: {len(parsed)} fields")
            return parsed
        except Exception as e:
            logger.error(f"[REDIS] HGETALL failed for {key}: {e}", exc_info=True)
            return {}

    async def hdel(self, key: str, *fields) -> int:
        """Delete one or more hash fields"""
        if not self.is_connected:
            return 0

        try:
            result = await self.redis_client.hdel(key, *fields)
            logger.debug(f"[REDIS] HDEL {key}: {len(fields)} fields")
            return result
        except Exception as e:
            logger.error(f"[REDIS] HDEL failed for {key}: {e}", exc_info=True)
            return 0

    async def hexists(self, key: str, field: str) -> bool:
        """Check if a hash field exists"""
        if not self.is_connected:
            return False

        try:
            return await self.redis_client.hexists(key, field)
        except Exception as e:
            logger.error(f"[REDIS] HEXISTS failed for {key}:{field}: {e}", exc_info=True)
            return False

    async def hkeys(self, key: str) -> List[str]:
        """Get all field names in a hash"""
        if not self.is_connected:
            return []

        try:
            keys = await self.redis_client.hkeys(key)
            return [k.decode() if isinstance(k, bytes) else k for k in keys]
        except Exception as e:
            logger.error(f"[REDIS] HKEYS failed for {key}: {e}", exc_info=True)
            return []