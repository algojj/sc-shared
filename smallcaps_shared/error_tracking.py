#!/usr/bin/env python3
"""
Enhanced Error Tracking System with Stack Trace Persistence
Captures, stores and analyzes errors across all microservices
"""

import logging
import traceback
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import asyncio
from functools import wraps
import inspect
import os

from .redis_client import RedisClient

logger = logging.getLogger(__name__)

class ErrorTracker:
    """Centralized error tracking system"""

    def __init__(self, service_name: str, redis_client: RedisClient):
        self.service_name = service_name
        self.redis = redis_client
        self.error_count = 0
        self.session_id = f"{service_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    async def log_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        severity: str = "ERROR"
    ) -> str:
        """
        Log error with full stack trace and context
        Returns error_id for tracking
        """
        error_id = f"error:{self.service_name}:{datetime.now(timezone.utc).timestamp()}"

        # Get full stack trace
        tb_lines = traceback.format_exception(type(error), error, error.__traceback__)
        stack_trace = ''.join(tb_lines)

        # Get current frame info
        frame = inspect.currentframe()
        caller_frame = frame.f_back if frame else None

        error_data = {
            "error_id": error_id,
            "service": self.service_name,
            "session_id": self.session_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": stack_trace,
            "context": context or {},
            "environment": {
                "python_version": sys.version,
                "platform": sys.platform,
                "hostname": os.environ.get("HOSTNAME", "unknown"),
                "container_id": os.environ.get("CONTAINER_ID", "unknown")
            }
        }

        # Add caller info if available
        if caller_frame:
            error_data["caller"] = {
                "filename": caller_frame.f_code.co_filename,
                "function": caller_frame.f_code.co_name,
                "line_number": caller_frame.f_lineno
            }

        try:
            # Store in Redis with expiry (7 days)
            await self.redis.set(error_id, error_data, ttl=604800)

            # Add to error list for this service
            error_list_key = f"errors:{self.service_name}:list"
            await self.redis.lpush(error_list_key, error_id)
            await self.redis.ltrim(error_list_key, 0, 999)  # Keep last 1000 errors

            # Increment error counters
            await self._increment_counters(severity)

            # Store critical errors separately
            if severity in ["CRITICAL", "FATAL"]:
                critical_key = f"errors:critical:{self.service_name}"
                await self.redis.lpush(critical_key, error_id)
                await self.redis.ltrim(critical_key, 0, 99)  # Keep last 100 critical

            logger.error(f"[ERROR-TRACKER] Logged error {error_id}: {error_data['error_message']}")

        except Exception as redis_error:
            # Fallback to file logging if Redis fails
            logger.error(f"Failed to store error in Redis: {redis_error}")
            self._fallback_log(error_data)

        return error_id

    async def _increment_counters(self, severity: str):
        """Increment error counters for monitoring"""
        # Daily counter
        daily_key = f"errors:count:{self.service_name}:{datetime.now(timezone.utc).strftime('%Y%m%d')}"
        await self.redis.incr(daily_key)
        await self.redis.expire(daily_key, 86400 * 7)  # Keep for 7 days

        # Hourly counter
        hourly_key = f"errors:count:{self.service_name}:{datetime.now(timezone.utc).strftime('%Y%m%d%H')}"
        await self.redis.incr(hourly_key)
        await self.redis.expire(hourly_key, 86400)  # Keep for 1 day

        # Severity counter
        severity_key = f"errors:severity:{self.service_name}:{severity}"
        await self.redis.incr(severity_key)

    def _fallback_log(self, error_data: Dict[str, Any]):
        """Fallback logging to file when Redis is unavailable"""
        try:
            log_dir = "/tmp/error_logs"
            os.makedirs(log_dir, exist_ok=True)

            filename = f"{log_dir}/{self.service_name}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
            with open(filename, 'a') as f:
                f.write(json.dumps(error_data) + '\n')
        except Exception as e:
            logger.critical(f"Failed to write error log to file: {e}")

    async def get_recent_errors(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent errors for this service"""
        error_list_key = f"errors:{self.service_name}:list"
        error_ids = await self.redis.lrange(error_list_key, 0, limit - 1)

        errors = []
        for error_id in error_ids:
            if isinstance(error_id, bytes):
                error_id = error_id.decode('utf-8')
            error_data = await self.redis.get(error_id)
            if error_data:
                errors.append(error_data)

        return errors

    async def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics for this service"""
        today = datetime.now(timezone.utc).strftime('%Y%m%d')
        hour = datetime.now(timezone.utc).strftime('%Y%m%d%H')

        stats = {
            "service": self.service_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "errors_today": await self.redis.get(f"errors:count:{self.service_name}:{today}") or 0,
            "errors_this_hour": await self.redis.get(f"errors:count:{self.service_name}:{hour}") or 0,
            "critical_errors": await self.redis.llen(f"errors:critical:{self.service_name}"),
            "total_errors": await self.redis.llen(f"errors:{self.service_name}:list")
        }

        # Get severity breakdown
        for severity in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"]:
            count = await self.redis.get(f"errors:severity:{self.service_name}:{severity}")
            stats[f"severity_{severity.lower()}"] = count or 0

        return stats

def error_handler(error_tracker: ErrorTracker, severity: str = "ERROR"):
    """
    Decorator to automatically catch and log errors with stack traces

    Usage:
        @error_handler(error_tracker, severity="CRITICAL")
        async def my_function():
            ...
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                context = {
                    "function": func.__name__,
                    "args": str(args)[:500],  # Limit size
                    "kwargs": str(kwargs)[:500]
                }
                error_id = await error_tracker.log_error(e, context, severity)
                logger.error(f"Error in {func.__name__}: {e} (Error ID: {error_id})")
                raise  # Re-raise to maintain original behavior

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                context = {
                    "function": func.__name__,
                    "args": str(args)[:500],
                    "kwargs": str(kwargs)[:500]
                }
                # Run async in sync context
                loop = asyncio.get_event_loop()
                error_id = loop.run_until_complete(
                    error_tracker.log_error(e, context, severity)
                )
                logger.error(f"Error in {func.__name__}: {e} (Error ID: {error_id})")
                raise

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

class ErrorMonitor:
    """Monitor and analyze errors across all services"""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client

    async def get_all_service_stats(self) -> Dict[str, Any]:
        """Get error statistics for all services"""
        # Find all services with errors
        services = set()
        keys = []
        cursor = 0
        while True:
            cursor, batch = await self.redis.scan(cursor, match="errors:*:list", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            # Extract service name from key
            parts = key.split(':')
            if len(parts) >= 3:
                services.add(parts[1])

        stats = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {}
        }

        for service in services:
            tracker = ErrorTracker(service, self.redis)
            stats["services"][service] = await tracker.get_error_stats()

        # Calculate totals
        stats["totals"] = {
            "total_errors": sum(s.get("total_errors", 0) for s in stats["services"].values()),
            "critical_errors": sum(s.get("critical_errors", 0) for s in stats["services"].values()),
            "errors_today": sum(s.get("errors_today", 0) for s in stats["services"].values()),
            "errors_this_hour": sum(s.get("errors_this_hour", 0) for s in stats["services"].values())
        }

        return stats

    async def get_critical_errors(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all critical errors across services"""
        critical_errors = []

        # Find all critical error keys (SCAN instead of KEYS)
        keys = []
        cursor = 0
        while True:
            cursor, batch = await self.redis.scan(cursor, match="errors:critical:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('utf-8')

            # Get error IDs from this key
            error_ids = await self.redis.lrange(key, 0, limit // len(keys) if keys else limit)

            for error_id in error_ids:
                if isinstance(error_id, bytes):
                    error_id = error_id.decode('utf-8')
                error_data = await self.redis.get(error_id)
                if error_data:
                    critical_errors.append(error_data)

        # Sort by timestamp
        critical_errors.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        return critical_errors[:limit]

    async def search_errors(
        self,
        service: Optional[str] = None,
        error_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search for errors based on criteria"""
        results = []

        # Determine which services to search
        if service:
            services = [service]
        else:
            # Find all services (SCAN instead of KEYS)
            services = set()
            keys = []
            cursor = 0
            while True:
                cursor, batch = await self.redis.scan(cursor, match="errors:*:list", count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                parts = key.split(':')
                if len(parts) >= 3:
                    services.add(parts[1])

        # Search each service
        for svc in services:
            tracker = ErrorTracker(svc, self.redis)
            errors = await tracker.get_recent_errors(limit=500)

            for error in errors:
                # Apply filters
                if error_type and error.get("error_type") != error_type:
                    continue
                if severity and error.get("severity") != severity:
                    continue

                # Check timestamp
                if "timestamp" in error:
                    error_time = datetime.fromisoformat(error["timestamp"])
                    if start_time and error_time < start_time:
                        continue
                    if end_time and error_time > end_time:
                        continue

                results.append(error)

        # Sort by timestamp
        results.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        return results

# Export main classes
__all__ = ['ErrorTracker', 'ErrorMonitor', 'error_handler']