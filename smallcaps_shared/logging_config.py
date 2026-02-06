#!/usr/bin/env python3
"""
Centralized Logging Configuration for SmallCaps Scanner
Provides structured logging with database error tracking
"""

import logging
import json
import traceback
import asyncio
import asyncpg
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
import os

class DatabaseErrorLogger:
    """
    Logs database errors to a dedicated table for monitoring
    """
    def __init__(self, conn_pool: Optional[asyncpg.Pool] = None):
        self.conn_pool = conn_pool
        self.pending_logs = []
        self.initialized = False

    async def init(self, conn_pool: asyncpg.Pool):
        """Initialize with connection pool and create logging table"""
        self.conn_pool = conn_pool

        # Create error logging table if not exists
        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_errors (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        service_name VARCHAR(50),
                        error_type VARCHAR(100),
                        error_message TEXT,
                        query TEXT,
                        parameters JSONB,
                        stack_trace TEXT,
                        severity VARCHAR(20),
                        resolved BOOLEAN DEFAULT FALSE,
                        notes TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_system_errors_timestamp
                    ON system_errors(timestamp DESC);

                    CREATE INDEX IF NOT EXISTS idx_system_errors_service
                    ON system_errors(service_name, timestamp DESC);

                    CREATE INDEX IF NOT EXISTS idx_system_errors_unresolved
                    ON system_errors(resolved, timestamp DESC);
                """)
                self.initialized = True

                # Log any pending errors
                if self.pending_logs:
                    for log in self.pending_logs:
                        await self._write_log(**log)
                    self.pending_logs.clear()

        except Exception as e:
            print(f"Failed to initialize error logging table: {e}")

    async def log_database_error(
        self,
        service_name: str,
        error: Exception,
        query: str = None,
        parameters: Any = None,
        severity: str = "ERROR"
    ):
        """Log a database error with full context"""

        error_data = {
            "service_name": service_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "query": query,
            "parameters": json.dumps(parameters) if parameters else None,
            "stack_trace": traceback.format_exc(),
            "severity": severity
        }

        if not self.initialized or not self.conn_pool:
            # Store for later if not initialized
            self.pending_logs.append(error_data)
            return

        await self._write_log(**error_data)

    async def _write_log(self, **kwargs):
        """Write error log to database"""
        try:
            async with self.conn_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO system_errors
                    (service_name, error_type, error_message, query, parameters, stack_trace, severity)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                kwargs['service_name'],
                kwargs['error_type'],
                kwargs['error_message'],
                kwargs['query'],
                kwargs.get('parameters'),
                kwargs['stack_trace'],
                kwargs['severity']
                )
        except Exception as e:
            # Fallback to file if DB logging fails
            self._log_to_file(kwargs, e)

    def _log_to_file(self, error_data: dict, db_error: Exception):
        """Fallback to file logging if database logging fails"""
        log_dir = Path("/tmp/smallcaps_errors")
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"db_error_{timestamp}.json"

        error_data['db_logging_error'] = str(db_error)
        error_data['timestamp'] = datetime.now(timezone.utc).isoformat()

        with open(log_file, 'w') as f:
            json.dump(error_data, f, indent=2, default=str)


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured log output (ELK/Loki/CloudWatch compatible)"""

    def __init__(self, service_name: str = "unknown"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "message": record.getMessage(),
        }
        extra_data = getattr(record, "extra_data", "")
        if extra_data and extra_data.startswith("DATA: "):
            try:
                entry["data"] = json.loads(extra_data[6:])
            except (json.JSONDecodeError, ValueError):
                entry["data_raw"] = extra_data[6:]
        if record.exc_info and record.exc_info[1]:
            entry["error"] = str(record.exc_info[1])
            entry["traceback"] = traceback.format_exception(*record.exc_info)
        return json.dumps(entry, default=str)


class StructuredLogger:
    """
    Enhanced logger with structured output for better debugging.
    Set JSON_LOGGING=true env var for JSON-lines output (log aggregation ready).
    """
    def __init__(self, service_name: str, db_logger: Optional[DatabaseErrorLogger] = None):
        self.service_name = service_name
        self.db_logger = db_logger
        self.logger = logging.getLogger(service_name)

        # Configure handler — JSON or pipe-separated format
        handler = logging.StreamHandler()
        if os.getenv("JSON_LOGGING", "false").lower() == "true":
            handler.setFormatter(JsonFormatter(service_name))
        else:
            formatter = logging.Formatter(
                '%(asctime)s | %(name)s | %(levelname)s | %(message)s | %(extra_data)s',
                defaults={"extra_data": ""}
            )
            handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        # Enable SQL debug if configured
        if os.getenv("SQL_DEBUG", "false").lower() == "true":
            self.logger.setLevel(logging.DEBUG)

    def _format_extra(self, **kwargs) -> str:
        """Format extra data as JSON string"""
        if kwargs:
            return f"DATA: {json.dumps(kwargs, default=str)}"
        return ""

    def info(self, message: str, **kwargs):
        """Log info with structured data"""
        extra = self._format_extra(**kwargs)
        self.logger.info(message, extra={"extra_data": extra})

    def warning(self, message: str, **kwargs):
        """Log warning with structured data"""
        extra = self._format_extra(**kwargs)
        self.logger.warning(message, extra={"extra_data": extra})

    def error(self, message: str, error: Exception = None, **kwargs):
        """Log error with structured data and optional database logging"""
        extra = self._format_extra(**kwargs)
        self.logger.error(message, extra={"extra_data": extra})

        # Also log to database if available
        if self.db_logger and error:
            asyncio.create_task(
                self.db_logger.log_database_error(
                    self.service_name,
                    error,
                    query=kwargs.get('query'),
                    parameters=kwargs.get('parameters'),
                    severity="ERROR"
                )
            )

    def debug(self, message: str, **kwargs):
        """Log debug with structured data"""
        extra = self._format_extra(**kwargs)
        self.logger.debug(message, extra={"extra_data": extra})

    def critical(self, message: str, error: Exception = None, **kwargs):
        """Log critical error that requires immediate attention"""
        extra = self._format_extra(**kwargs)
        self.logger.critical(message, extra={"extra_data": extra})

        # Always log critical errors to database
        if self.db_logger and error:
            asyncio.create_task(
                self.db_logger.log_database_error(
                    self.service_name,
                    error,
                    query=kwargs.get('query'),
                    parameters=kwargs.get('parameters'),
                    severity="CRITICAL"
                )
            )


class DatabaseOperationWrapper:
    """
    Wrapper for database operations with automatic error handling and logging
    """
    def __init__(self, logger: StructuredLogger, conn_pool: asyncpg.Pool):
        self.logger = logger
        self.conn_pool = conn_pool
        self.retry_delays = [1, 2, 5]  # Seconds between retries

    async def execute_with_retry(
        self,
        query: str,
        *args,
        operation_name: str = "database_operation",
        max_retries: int = 3
    ):
        """
        Execute a database query with automatic retry and logging
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                async with self.conn_pool.acquire() as conn:
                    result = await conn.execute(query, *args)

                    if attempt > 0:
                        self.logger.info(
                            f"Database operation succeeded after {attempt + 1} attempts",
                            operation=operation_name
                        )

                    return result

            except asyncpg.PostgresConnectionError as e:
                last_error = e
                self.logger.warning(
                    f"Database connection error on attempt {attempt + 1}/{max_retries}",
                    operation=operation_name,
                    error_type=type(e).__name__,
                    attempt=attempt + 1
                )

                if attempt < max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    await asyncio.sleep(delay)

            except asyncpg.PostgresError as e:
                # Log but don't retry for SQL errors (bad query, constraints, etc)
                self.logger.error(
                    f"Database SQL error in {operation_name}",
                    error=e,
                    query=query[:500],  # First 500 chars of query
                    parameters=args[:5] if args else None  # First 5 params
                )
                raise  # Re-raise SQL errors immediately

            except Exception as e:
                last_error = e
                self.logger.error(
                    f"Unexpected error in {operation_name}",
                    error=e,
                    query=query[:500],
                    parameters=args[:5] if args else None,
                    attempt=attempt + 1
                )

                if attempt < max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    await asyncio.sleep(delay)

        # All retries failed
        self.logger.critical(
            f"Database operation failed after {max_retries} attempts",
            error=last_error,
            operation=operation_name,
            query=query[:500],
            parameters=args[:5] if args else None
        )

        raise last_error

    async def fetch_with_retry(
        self,
        query: str,
        *args,
        operation_name: str = "database_fetch",
        max_retries: int = 3
    ):
        """
        Fetch data with automatic retry and logging
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                async with self.conn_pool.acquire() as conn:
                    result = await conn.fetch(query, *args)

                    if attempt > 0:
                        self.logger.info(
                            f"Database fetch succeeded after {attempt + 1} attempts",
                            operation=operation_name,
                            rows_returned=len(result)
                        )

                    return result

            except Exception as e:
                last_error = e
                self.logger.warning(
                    f"Database fetch error on attempt {attempt + 1}/{max_retries}",
                    operation=operation_name,
                    error_type=type(e).__name__,
                    attempt=attempt + 1
                )

                if attempt < max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    await asyncio.sleep(delay)

        # All retries failed - log and return empty result
        self.logger.error(
            f"Database fetch failed after {max_retries} attempts - returning empty result",
            error=last_error,
            operation=operation_name
        )

        return []  # Return empty result instead of crashing


# Singleton instances
_db_error_logger = None
_loggers = {}

def get_logger(service_name: str, conn_pool: Optional[asyncpg.Pool] = None) -> StructuredLogger:
    """
    Get or create a logger instance for a service
    """
    global _db_error_logger, _loggers

    if service_name not in _loggers:
        # Create database error logger if not exists
        if conn_pool and not _db_error_logger:
            _db_error_logger = DatabaseErrorLogger()
            asyncio.create_task(_db_error_logger.init(conn_pool))

        _loggers[service_name] = StructuredLogger(service_name, _db_error_logger)

    return _loggers[service_name]


def setup_json_logging(service_name: str, level: int = logging.INFO):
    """
    Quick setup for JSON logging on the root logger.
    Call once at service startup:
        from smallcaps_shared.logging_config import setup_json_logging
        setup_json_logging("gap-scanner")
    """
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service_name))
    root.handlers = [handler]


def get_db_wrapper(
    service_name: str,
    conn_pool: asyncpg.Pool
) -> DatabaseOperationWrapper:
    """
    Get a database operation wrapper with logging
    """
    logger = get_logger(service_name, conn_pool)
    return DatabaseOperationWrapper(logger, conn_pool)