"""SmallCaps Shared Library for Microservices"""

__version__ = "1.1.6"

from .models import ScanResult, ScannerType, HealthStatus
from .redis_client import RedisClient, CacheKeys
from .polygon_base import PolygonBaseClient
from .settings import BaseSettings, DatabaseSettings
from .error_tracking import ErrorTracker, ErrorMonitor, error_handler
from .nhod_tracker import NHODTracker
from .ticker_tracker import TickerTracker
from .database_service import DatabaseService
from .dynamic_config import DynamicConfig, ConfigManager, get_config, set_config
from .symbols_service import SymbolsService, SymbolInfo, symbols_service
from .session_manager import SessionManager
from .stream_subscriber import StreamSubscriber, PriceEventHandler
from .event_stream import EventProducer, EventConsumer, PriceStreamHandler
from .country_utils import detect_country_from_address, detect_country_from_url, detect_country_from_description, get_flag_emoji, enrich_with_country_codes
from .email_notifier import EmailNotifier
from .telegram_notifier import TelegramNotifier
from .discord_notifier import DiscordNotifier
from .github_issue_creator import GitHubIssueCreator
from .market_cap_classifier import MarketCapClassifier
from .price_formatter import format_price, format_dollar, get_price_precision
from .job_logger import JobLogger, run_with_logging
from .sessionizer import Sessionizer, SessionInfo, HolidayCalendar, get_sessionizer
from .market_status import (
    MarketStatusChecker,
    get_market_checker,
    is_market_open,
    is_trading_day,
    get_session,
    get_market_status
)
from .logging_config import StructuredLogger, JsonFormatter, get_logger as get_structured_logger, get_db_wrapper, setup_json_logging
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitBreakerAlert,
    AlertType,
    CircuitState,
    with_circuit_breaker
)

# Optional imports - only available if fastapi/prometheus_client are installed
try:
    from .health_server import HealthServer
except ImportError:
    HealthServer = None

try:
    from .prometheus_metrics import PrometheusMetrics
except ImportError:
    PrometheusMetrics = None

try:
    from .prometheus_middleware import (
        setup_prometheus_metrics,
        create_db_metrics,
        create_redis_metrics,
        create_external_api_metrics
    )
except ImportError:
    setup_prometheus_metrics = None
    create_db_metrics = None
    create_redis_metrics = None
    create_external_api_metrics = None

__all__ = [
    "ScanResult",
    "ScannerType",
    "HealthStatus",
    "RedisClient",
    "CacheKeys",
    "PolygonBaseClient",
    "BaseSettings",
    "DatabaseSettings",
    "ErrorTracker",
    "ErrorMonitor",
    "error_handler",
    "NHODTracker",
    "TickerTracker",
    "DatabaseService",
    "DynamicConfig",
    "ConfigManager",
    "get_config",
    "set_config",
    "SymbolsService",
    "SymbolInfo",
    "symbols_service",
    "HealthServer",
    "PrometheusMetrics",
    "SessionManager",
    "StreamSubscriber",
    "PriceEventHandler",
    "EventProducer",
    "EventConsumer",
    "PriceStreamHandler",
    "EmailNotifier",
    "TelegramNotifier",
    "GitHubIssueCreator",
    "MarketCapClassifier",
    "format_price",
    "format_dollar",
    "get_price_precision",
    "JobLogger",
    "run_with_logging",
    "Sessionizer",
    "SessionInfo",
    "HolidayCalendar",
    "get_sessionizer",
    "MarketStatusChecker",
    "get_market_checker",
    "is_market_open",
    "is_trading_day",
    "get_session",
    "get_market_status",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitBreakerAlert",
    "AlertType",
    "CircuitState",
    "with_circuit_breaker",
    "setup_prometheus_metrics",
    "create_db_metrics",
    "create_redis_metrics",
    "create_external_api_metrics",
    "StructuredLogger",
    "JsonFormatter",
    "get_structured_logger",
    "get_db_wrapper",
    "setup_json_logging",
]