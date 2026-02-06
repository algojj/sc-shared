#!/usr/bin/env python3
"""
Health check server for background services
Minimal HTTP server to expose health status and Prometheus metrics
"""

from fastapi import FastAPI, Response
from datetime import datetime, timezone
import asyncio
import uvicorn
from typing import Optional

try:
    from .prometheus_metrics import PrometheusMetrics
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

class HealthServer:
    def __init__(self, service_name: str, port: int = 8001, requires_redis: bool = True, requires_scanning: bool = True, scan_timeout: int = 120, enable_metrics: bool = True):
        self.app = FastAPI(title=f"{service_name} Health")
        self.service_name = service_name
        self.port = port
        self.requires_redis = requires_redis
        self.requires_scanning = requires_scanning
        self.scan_timeout = scan_timeout  # Configurable timeout for scan health check
        self.start_time = datetime.now(timezone.utc)
        self.last_scan_time = None
        self.scan_count = 0
        self.error_count = 0
        self.is_healthy = True
        self.redis_connected = False
        self.db_connected = False
        self.tickers_monitored = 0
        self.alerts_generated = 0

        # Initialize Prometheus metrics if available and enabled
        self.metrics: Optional[PrometheusMetrics] = None
        if enable_metrics and PROMETHEUS_AVAILABLE:
            self.metrics = PrometheusMetrics(service_name)
            self.metrics.register_with_app(self.app)

        # Register health endpoint
        @self.app.get("/health")
        async def health_check():
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()

            # Service is healthy if:
            # - Redis is connected (if required)
            # - DB is connected
            # - Last scan was within 2 minutes (for scanners with 15-60s intervals)
            # - Error rate is < 10%

            time_since_last_scan = None
            if self.last_scan_time:
                time_since_last_scan = (datetime.now(timezone.utc) - self.last_scan_time).total_seconds()

            is_scanning_healthy = True
            if time_since_last_scan and time_since_last_scan > self.scan_timeout:
                is_scanning_healthy = False

            error_rate = (self.error_count / max(self.scan_count, 1)) * 100
            is_error_rate_ok = error_rate < 10

            # Check Redis only if required
            redis_ok = self.redis_connected if self.requires_redis else True

            # Check scanning only if required (schedulers don't need active scanning)
            scanning_ok = is_scanning_healthy if self.requires_scanning else True

            overall_health = (
                redis_ok and
                self.db_connected and
                scanning_ok and
                is_error_rate_ok
            )

            status = {
                "service": self.service_name,
                "status": "healthy" if overall_health else "unhealthy",
                "uptime_seconds": int(uptime),
                "connections": {
                    "redis": self.redis_connected,
                    "database": self.db_connected
                },
                "scanning": {
                    "last_scan_seconds_ago": time_since_last_scan,
                    "is_healthy": is_scanning_healthy,
                    "total_scans": self.scan_count,
                    "errors": self.error_count,
                    "error_rate_percent": round(error_rate, 2)
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Return 503 if unhealthy
            if not overall_health:
                return status, 503

            return status

    def update_scan_status(self, ticker_count: int = 0, duration: float = 0.0):
        """Call this after each successful scan"""
        self.last_scan_time = datetime.now(timezone.utc)
        self.scan_count += 1
        if self.metrics:
            self.metrics.record_scan(success=True, ticker_count=ticker_count, duration=duration)

    def update_error_count(self):
        """Call this when a scan fails"""
        self.error_count += 1
        if self.metrics:
            self.metrics.record_scan(success=False)

    def set_redis_status(self, connected: bool):
        """Update Redis connection status"""
        self.redis_connected = connected
        if self.metrics:
            self.metrics.set_connection_status("redis", connected)

    def set_db_status(self, connected: bool):
        """Update database connection status"""
        self.db_connected = connected
        if self.metrics:
            self.metrics.set_connection_status("database", connected)

    def record_alert(self, alert_type: str):
        """Record an alert generation"""
        self.alerts_generated += 1
        if self.metrics:
            self.metrics.record_alert(alert_type)

    async def start(self):
        """Start the health check server"""
        config = uvicorn.Config(
            app=self.app,
            host="0.0.0.0",
            port=self.port,
            log_level="error"  # Quiet to avoid spam
        )
        server = uvicorn.Server(config)
        await server.serve()
