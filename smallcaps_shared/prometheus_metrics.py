#!/usr/bin/env python3
"""
Prometheus metrics for SmallCaps microservices
Provides standard metrics and easy integration with HealthServer
"""

from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import CollectorRegistry
from functools import wraps
import time
from typing import Optional, Callable
from fastapi import FastAPI, Response


class PrometheusMetrics:
    """
    Centralized Prometheus metrics for SmallCaps microservices

    Usage:
        metrics = PrometheusMetrics("alerts-service")
        metrics.register_with_app(app)  # Register /metrics endpoint

        # Track requests
        with metrics.track_request("process_alert"):
            process_alert()

        # Track scans
        metrics.record_scan(success=True, ticker_count=50)

        # Track DB operations
        with metrics.track_db_query("get_alerts"):
            db.execute(query)
    """

    def __init__(self, service_name: str, registry: Optional[CollectorRegistry] = None):
        self.service_name = service_name
        self.registry = registry or CollectorRegistry()

        # Service info
        self.info = Info(
            'service',
            'Service information',
            registry=self.registry
        )
        self.info.info({
            'name': service_name,
            'version': '3.11.654'
        })

        # Request metrics
        self.request_count = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['service', 'method', 'endpoint', 'status'],
            registry=self.registry
        )

        self.request_latency = Histogram(
            'http_request_duration_seconds',
            'HTTP request latency in seconds',
            ['service', 'method', 'endpoint'],
            buckets=[.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0],
            registry=self.registry
        )

        # Scan metrics (for scanners)
        self.scan_count = Counter(
            'scanner_scans_total',
            'Total scans performed',
            ['service', 'status'],
            registry=self.registry
        )

        self.scan_duration = Histogram(
            'scanner_scan_duration_seconds',
            'Scan duration in seconds',
            ['service'],
            buckets=[.1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
            registry=self.registry
        )

        self.tickers_processed = Counter(
            'scanner_tickers_processed_total',
            'Total tickers processed',
            ['service'],
            registry=self.registry
        )

        # Alert metrics
        self.alerts_generated = Counter(
            'alerts_generated_total',
            'Total alerts generated',
            ['service', 'alert_type'],
            registry=self.registry
        )

        # Database metrics
        self.db_query_count = Counter(
            'db_queries_total',
            'Total database queries',
            ['service', 'operation', 'status'],
            registry=self.registry
        )

        self.db_query_duration = Histogram(
            'db_query_duration_seconds',
            'Database query duration in seconds',
            ['service', 'operation'],
            buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5],
            registry=self.registry
        )

        # Redis metrics
        self.redis_operations = Counter(
            'redis_operations_total',
            'Total Redis operations',
            ['service', 'operation', 'status'],
            registry=self.registry
        )

        # Connection status
        self.connection_status = Gauge(
            'connection_status',
            'Connection status (1=connected, 0=disconnected)',
            ['service', 'target'],
            registry=self.registry
        )

        # Active processing
        self.active_requests = Gauge(
            'active_requests',
            'Number of requests currently being processed',
            ['service'],
            registry=self.registry
        )

        # Polygon API metrics
        self.polygon_requests = Counter(
            'polygon_api_requests_total',
            'Total Polygon API requests',
            ['service', 'endpoint', 'status'],
            registry=self.registry
        )

        self.polygon_latency = Histogram(
            'polygon_api_duration_seconds',
            'Polygon API request latency',
            ['service', 'endpoint'],
            buckets=[.05, .1, .25, .5, 1.0, 2.5, 5.0],
            registry=self.registry
        )

    def register_with_app(self, app: FastAPI):
        """Register /metrics endpoint with FastAPI app"""
        @app.get("/metrics")
        async def metrics():
            return Response(
                content=generate_latest(self.registry),
                media_type=CONTENT_TYPE_LATEST
            )

    def track_request(self, endpoint: str, method: str = "GET"):
        """Context manager to track request metrics"""
        return RequestTracker(self, endpoint, method)

    def track_db_query(self, operation: str):
        """Context manager to track database query metrics"""
        return DBQueryTracker(self, operation)

    def track_polygon_request(self, endpoint: str):
        """Context manager to track Polygon API request metrics"""
        return PolygonRequestTracker(self, endpoint)

    def record_scan(self, success: bool, ticker_count: int = 0, duration: float = 0.0):
        """Record a scan execution"""
        status = "success" if success else "error"
        self.scan_count.labels(service=self.service_name, status=status).inc()
        if ticker_count > 0:
            self.tickers_processed.labels(service=self.service_name).inc(ticker_count)
        if duration > 0:
            self.scan_duration.labels(service=self.service_name).observe(duration)

    def record_alert(self, alert_type: str):
        """Record an alert generation"""
        self.alerts_generated.labels(
            service=self.service_name,
            alert_type=alert_type
        ).inc()

    def set_connection_status(self, target: str, connected: bool):
        """Set connection status for a target (redis, database, etc)"""
        self.connection_status.labels(
            service=self.service_name,
            target=target
        ).set(1 if connected else 0)

    def record_redis_operation(self, operation: str, success: bool):
        """Record a Redis operation"""
        status = "success" if success else "error"
        self.redis_operations.labels(
            service=self.service_name,
            operation=operation,
            status=status
        ).inc()


class RequestTracker:
    """Context manager for tracking request metrics"""

    def __init__(self, metrics: PrometheusMetrics, endpoint: str, method: str):
        self.metrics = metrics
        self.endpoint = endpoint
        self.method = method
        self.start_time = None
        self.status = "200"

    def __enter__(self):
        self.start_time = time.time()
        self.metrics.active_requests.labels(service=self.metrics.service_name).inc()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if exc_type is not None:
            self.status = "500"

        self.metrics.request_count.labels(
            service=self.metrics.service_name,
            method=self.method,
            endpoint=self.endpoint,
            status=self.status
        ).inc()

        self.metrics.request_latency.labels(
            service=self.metrics.service_name,
            method=self.method,
            endpoint=self.endpoint
        ).observe(duration)

        self.metrics.active_requests.labels(service=self.metrics.service_name).dec()

    def set_status(self, status: str):
        """Set the response status code"""
        self.status = status


class DBQueryTracker:
    """Context manager for tracking database query metrics"""

    def __init__(self, metrics: PrometheusMetrics, operation: str):
        self.metrics = metrics
        self.operation = operation
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        status = "success" if exc_type is None else "error"

        self.metrics.db_query_count.labels(
            service=self.metrics.service_name,
            operation=self.operation,
            status=status
        ).inc()

        self.metrics.db_query_duration.labels(
            service=self.metrics.service_name,
            operation=self.operation
        ).observe(duration)


class PolygonRequestTracker:
    """Context manager for tracking Polygon API request metrics"""

    def __init__(self, metrics: PrometheusMetrics, endpoint: str):
        self.metrics = metrics
        self.endpoint = endpoint
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        status = "success" if exc_type is None else "error"

        self.metrics.polygon_requests.labels(
            service=self.metrics.service_name,
            endpoint=self.endpoint,
            status=status
        ).inc()

        self.metrics.polygon_latency.labels(
            service=self.metrics.service_name,
            endpoint=self.endpoint
        ).observe(duration)
