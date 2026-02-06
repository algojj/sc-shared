"""Circuit Breaker pattern for API resilience

Implements:
- Circuit breaker with CLOSED, OPEN, HALF_OPEN states
- Retry with exponential backoff
- Fallback to cache when circuit is open
- Automatic recovery
- Alert notifications on fallback/circuit open
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Optional, Any, Callable, TypeVar, Dict, List
from dataclasses import dataclass, field
from functools import wraps
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class AlertType(Enum):
    """Types of circuit breaker alerts"""
    CIRCUIT_OPENED = "circuit_opened"
    CIRCUIT_CLOSED = "circuit_closed"
    CACHE_FALLBACK = "cache_fallback"
    ALL_RETRIES_FAILED = "all_retries_failed"
    NO_CACHE_AVAILABLE = "no_cache_available"


@dataclass
class CircuitBreakerAlert:
    """Alert data structure"""
    alert_type: AlertType
    circuit_name: str
    message: str
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 3          # Successes to close from half-open
    timeout_seconds: float = 60.0       # Time before trying half-open
    retry_attempts: int = 3             # Max retry attempts
    retry_base_delay: float = 1.0       # Base delay for exponential backoff
    retry_max_delay: float = 30.0       # Max delay between retries
    half_open_max_calls: int = 3        # Max concurrent calls in half-open
    alert_cooldown_seconds: float = 300.0  # Min time between same alert type


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    cache_hits: int = 0
    retries: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None


class CircuitBreaker:
    """Circuit breaker for API calls with retry and cache fallback"""

    # Class-level alert handlers
    _alert_handlers: List[Callable[['CircuitBreakerAlert'], None]] = []

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        redis_client: Optional[Any] = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.redis_client = redis_client

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

        # Track last alert time per type to implement cooldown
        self._last_alert_time: Dict[AlertType, float] = {}

        self.stats = CircuitBreakerStats()

        logger.info(f"[CB:{name}] Initialized (threshold={self.config.failure_threshold}, "
                   f"timeout={self.config.timeout_seconds}s)")

    @classmethod
    def add_alert_handler(cls, handler: Callable[['CircuitBreakerAlert'], None]):
        """Register a global alert handler for all circuit breakers"""
        cls._alert_handlers.append(handler)
        logger.info(f"[CB] Registered alert handler: {handler.__name__}")

    @classmethod
    def clear_alert_handlers(cls):
        """Clear all alert handlers"""
        cls._alert_handlers.clear()

    def _send_alert(self, alert_type: AlertType, message: str, details: Dict[str, Any] = None):
        """Send alert if cooldown has passed"""
        now = time.time()
        last_time = self._last_alert_time.get(alert_type, 0)

        if now - last_time < self.config.alert_cooldown_seconds:
            logger.debug(f"[CB:{self.name}] Alert {alert_type.value} suppressed (cooldown)")
            return

        self._last_alert_time[alert_type] = now

        alert = CircuitBreakerAlert(
            alert_type=alert_type,
            circuit_name=self.name,
            message=message,
            timestamp=datetime.now(timezone.utc),
            details=details or {}
        )

        # Log the alert
        log_level = logging.ERROR if alert_type in [
            AlertType.CIRCUIT_OPENED,
            AlertType.NO_CACHE_AVAILABLE
        ] else logging.WARNING

        logger.log(log_level, f"[CB:{self.name}] ALERT: {alert_type.value} - {message}")

        # Call all registered handlers
        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"[CB:{self.name}] Alert handler error: {e}")

    @property
    def state(self) -> CircuitState:
        """Get current state, auto-transition from OPEN to HALF_OPEN if timeout passed"""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.config.timeout_seconds:
                    logger.info(f"[CB:{self.name}] Transitioning OPEN -> HALF_OPEN after {elapsed:.1f}s")
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self.stats.state_changes += 1
        return self._state

    def _record_success(self):
        """Record successful call"""
        self._failure_count = 0
        self.stats.successful_calls += 1
        self.stats.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                logger.info(f"[CB:{self.name}] Transitioning HALF_OPEN -> CLOSED after "
                           f"{self._success_count} successes")
                self._state = CircuitState.CLOSED
                self._success_count = 0
                self.stats.state_changes += 1
                self._send_alert(
                    AlertType.CIRCUIT_CLOSED,
                    f"Circuit recovered after {self.config.success_threshold} successful calls",
                    {"success_count": self.config.success_threshold}
                )

    def _record_failure(self):
        """Record failed call"""
        self._failure_count += 1
        self._success_count = 0
        self.stats.failed_calls += 1
        self.stats.last_failure_time = time.time()
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            logger.warning(f"[CB:{self.name}] Failure in HALF_OPEN, returning to OPEN")
            self._state = CircuitState.OPEN
            self._failure_count = 0
            self.stats.state_changes += 1
            self._send_alert(
                AlertType.CIRCUIT_OPENED,
                f"Circuit re-opened after failure in HALF_OPEN state",
                {"previous_state": "half_open"}
            )
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                logger.error(f"[CB:{self.name}] Opening circuit after {self._failure_count} failures")
                self._state = CircuitState.OPEN
                self.stats.state_changes += 1
                self._send_alert(
                    AlertType.CIRCUIT_OPENED,
                    f"Circuit opened after {self._failure_count} consecutive failures",
                    {"failure_count": self._failure_count, "threshold": self.config.failure_threshold}
                )

    def _can_execute(self) -> bool:
        """Check if call can be executed"""
        state = self.state  # This may auto-transition OPEN -> HALF_OPEN

        if state == CircuitState.CLOSED:
            return True

        if state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

        # OPEN state
        return False

    async def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Try to get value from cache"""
        if not self.redis_client or not cache_key:
            return None

        try:
            value = await self.redis_client.get(cache_key)
            if value is not None:
                self.stats.cache_hits += 1
                logger.info(f"[CB:{self.name}] Cache hit for {cache_key}")
            return value
        except Exception as e:
            logger.warning(f"[CB:{self.name}] Cache get failed: {e}")
            return None

    async def _set_cache(self, cache_key: str, value: Any, ttl: int = 300):
        """Store value in cache"""
        if not self.redis_client or not cache_key or value is None:
            return

        try:
            await self.redis_client.set(cache_key, value, ttl=ttl)
            logger.debug(f"[CB:{self.name}] Cached {cache_key} (TTL={ttl}s)")
        except Exception as e:
            logger.warning(f"[CB:{self.name}] Cache set failed: {e}")

    async def execute(
        self,
        func: Callable[..., T],
        *args,
        cache_key: Optional[str] = None,
        cache_ttl: int = 300,
        **kwargs
    ) -> Optional[T]:
        """Execute function with circuit breaker protection

        Args:
            func: Async function to execute
            *args: Positional arguments for func
            cache_key: Optional Redis key for caching result
            cache_ttl: Cache TTL in seconds (default 5 min)
            **kwargs: Keyword arguments for func

        Returns:
            Function result or cached value on failure
        """
        self.stats.total_calls += 1

        async with self._lock:
            can_execute = self._can_execute()

        if not can_execute:
            self.stats.rejected_calls += 1
            logger.warning(f"[CB:{self.name}] Circuit OPEN, request rejected")

            # Try cache fallback
            cached = await self._get_from_cache(cache_key)
            if cached is not None:
                logger.info(f"[CB:{self.name}] Returning cached value")
                return cached

            return None

        # Try with retries
        last_error = None
        for attempt in range(self.config.retry_attempts):
            try:
                result = await func(*args, **kwargs)

                async with self._lock:
                    self._record_success()

                # Cache successful result
                if cache_key and result is not None:
                    await self._set_cache(cache_key, result, cache_ttl)

                return result

            except Exception as e:
                last_error = e
                self.stats.retries += 1

                if attempt < self.config.retry_attempts - 1:
                    delay = min(
                        self.config.retry_base_delay * (2 ** attempt),
                        self.config.retry_max_delay
                    )
                    logger.warning(f"[CB:{self.name}] Attempt {attempt + 1} failed: {e}. "
                                 f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[CB:{self.name}] All {self.config.retry_attempts} attempts failed")

        # All retries failed
        async with self._lock:
            self._record_failure()

        self._send_alert(
            AlertType.ALL_RETRIES_FAILED,
            f"All {self.config.retry_attempts} retry attempts failed",
            {"last_error": str(last_error), "cache_key": cache_key}
        )

        # Try cache fallback
        cached = await self._get_from_cache(cache_key)
        if cached is not None:
            logger.info(f"[CB:{self.name}] API failed, returning cached value")
            self._send_alert(
                AlertType.CACHE_FALLBACK,
                f"Using cached data after API failure",
                {"cache_key": cache_key}
            )
            return cached

        self._send_alert(
            AlertType.NO_CACHE_AVAILABLE,
            f"API failed and no cache available - DATA LOSS",
            {"cache_key": cache_key, "last_error": str(last_error)}
        )
        logger.error(f"[CB:{self.name}] No cache available, returning None. Last error: {last_error}")
        return None

    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status"""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "stats": {
                "total_calls": self.stats.total_calls,
                "successful_calls": self.stats.successful_calls,
                "failed_calls": self.stats.failed_calls,
                "rejected_calls": self.stats.rejected_calls,
                "cache_hits": self.stats.cache_hits,
                "retries": self.stats.retries,
                "state_changes": self.stats.state_changes,
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "success_threshold": self.config.success_threshold,
                "timeout_seconds": self.config.timeout_seconds,
                "retry_attempts": self.config.retry_attempts,
            }
        }

    def reset(self):
        """Reset circuit breaker to initial state"""
        logger.info(f"[CB:{self.name}] Manual reset")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers"""

    _instance: Optional['CircuitBreakerRegistry'] = None
    _breakers: Dict[str, CircuitBreaker] = {}

    @classmethod
    def get_instance(cls) -> 'CircuitBreakerRegistry':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        redis_client: Optional[Any] = None
    ) -> CircuitBreaker:
        """Get existing circuit breaker or create new one"""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, config, redis_client)
        return self._breakers[name]

    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers"""
        return {name: cb.get_status() for name, cb in self._breakers.items()}

    def reset_all(self):
        """Reset all circuit breakers"""
        for cb in self._breakers.values():
            cb.reset()


def with_circuit_breaker(
    name: str,
    cache_key_fn: Optional[Callable[..., str]] = None,
    cache_ttl: int = 300,
    config: Optional[CircuitBreakerConfig] = None
):
    """Decorator to wrap async function with circuit breaker

    Args:
        name: Circuit breaker name
        cache_key_fn: Function to generate cache key from args/kwargs
        cache_ttl: Cache TTL in seconds
        config: Circuit breaker config

    Example:
        @with_circuit_breaker("polygon_api", cache_key_fn=lambda ticker: f"snapshot:{ticker}")
        async def get_snapshot(ticker: str):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            registry = CircuitBreakerRegistry.get_instance()
            cb = registry.get_or_create(name, config)

            cache_key = None
            if cache_key_fn:
                try:
                    cache_key = cache_key_fn(*args, **kwargs)
                except Exception:
                    pass

            return await cb.execute(func, *args, cache_key=cache_key, cache_ttl=cache_ttl, **kwargs)

        return wrapper
    return decorator
