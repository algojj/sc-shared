#!/usr/bin/env python3
"""
Prometheus Middleware for FastAPI services
Drop-in solution for automatic metrics instrumentation

Usage:
    from smallcaps_shared import setup_prometheus_metrics

    app = FastAPI()
    setup_prometheus_metrics(app, "api-gateway")  # One line!

This will:
- Add /metrics endpoint
- Track all HTTP requests (count, latency, status)
- Track active requests
- Expose service info
"""

import time
from typing import Callable
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import CollectorRegistry


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Middleware that tracks HTTP metrics for all requests"""

    def __init__(self, app, service_name: str, registry: CollectorRegistry):
        super().__init__(app)
        self.service_name = service_name
        self.registry = registry

        # Request counter
        self.request_count = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['service', 'method', 'path', 'status'],
            registry=registry
        )

        # Request latency histogram
        self.request_latency = Histogram(
            'http_request_duration_seconds',
            'HTTP request latency in seconds',
            ['service', 'method', 'path'],
            buckets=[.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0],
            registry=registry
        )

        # Active requests gauge
        self.active_requests = Gauge(
            'http_requests_active',
            'Number of active HTTP requests',
            ['service'],
            registry=registry
        )

        # Request size
        self.request_size = Histogram(
            'http_request_size_bytes',
            'HTTP request size in bytes',
            ['service', 'method', 'path'],
            buckets=[100, 1000, 10000, 100000, 1000000],
            registry=registry
        )

        # Response size
        self.response_size = Histogram(
            'http_response_size_bytes',
            'HTTP response size in bytes',
            ['service', 'method', 'path'],
            buckets=[100, 1000, 10000, 100000, 1000000, 10000000],
            registry=registry
        )

        # Errors counter
        self.errors = Counter(
            'http_errors_total',
            'Total HTTP errors (4xx and 5xx)',
            ['service', 'method', 'path', 'status'],
            registry=registry
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip metrics endpoint to avoid recursion
        if request.url.path == "/metrics":
            return await call_next(request)

        # Normalize path (remove query params, replace IDs with placeholders)
        path = self._normalize_path(request.url.path)
        method = request.method

        # Track request size
        content_length = request.headers.get('content-length')
        if content_length:
            self.request_size.labels(
                service=self.service_name,
                method=method,
                path=path
            ).observe(int(content_length))

        # Track active requests
        self.active_requests.labels(service=self.service_name).inc()

        start_time = time.time()

        try:
            response = await call_next(request)
            status = str(response.status_code)
        except Exception as e:
            status = "500"
            self.errors.labels(
                service=self.service_name,
                method=method,
                path=path,
                status=status
            ).inc()
            raise
        finally:
            # Record metrics
            duration = time.time() - start_time

            self.request_count.labels(
                service=self.service_name,
                method=method,
                path=path,
                status=status
            ).inc()

            self.request_latency.labels(
                service=self.service_name,
                method=method,
                path=path
            ).observe(duration)

            self.active_requests.labels(service=self.service_name).dec()

        # Track errors
        if response.status_code >= 400:
            self.errors.labels(
                service=self.service_name,
                method=method,
                path=path,
                status=status
            ).inc()

        # Track response size
        response_size = response.headers.get('content-length')
        if response_size:
            self.response_size.labels(
                service=self.service_name,
                method=method,
                path=path
            ).observe(int(response_size))

        return response

    def _normalize_path(self, path: str) -> str:
        """Normalize path to avoid high cardinality from dynamic segments"""
        parts = path.split('/')
        normalized = []
        for part in parts:
            # Replace UUIDs
            if len(part) == 36 and part.count('-') == 4:
                normalized.append('{id}')
            # Replace numeric IDs
            elif part.isdigit():
                normalized.append('{id}')
            # Replace ticker symbols (all uppercase, 1-5 chars)
            elif part.isupper() and 1 <= len(part) <= 5:
                normalized.append('{ticker}')
            else:
                normalized.append(part)
        return '/'.join(normalized)


def setup_prometheus_metrics(app: FastAPI, service_name: str, version: str = "3.11.668") -> CollectorRegistry:
    """
    Setup Prometheus metrics for a FastAPI application

    Args:
        app: FastAPI application instance
        service_name: Name of the service (e.g., "api-gateway")
        version: Service version

    Returns:
        CollectorRegistry for custom metrics

    Usage:
        app = FastAPI()
        registry = setup_prometheus_metrics(app, "api-gateway")

        # Optional: Add custom metrics
        my_counter = Counter('my_custom_metric', 'Description', registry=registry)
    """
    registry = CollectorRegistry()

    # Service info
    info = Info('service', 'Service information', registry=registry)
    info.info({
        'name': service_name,
        'version': version
    })

    # Up gauge (always 1 when service is running)
    up_gauge = Gauge('up', 'Service is up', ['service'], registry=registry)
    up_gauge.labels(service=service_name).set(1)

    # Add middleware
    app.add_middleware(PrometheusMiddleware, service_name=service_name, registry=registry)

    # Add /metrics endpoint
    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(registry),
            media_type=CONTENT_TYPE_LATEST
        )

    return registry


# Additional utility functions for custom metrics

def create_db_metrics(registry: CollectorRegistry, service_name: str):
    """Create database-related metrics"""
    return {
        'query_count': Counter(
            'db_queries_total',
            'Total database queries',
            ['service', 'operation', 'status'],
            registry=registry
        ),
        'query_duration': Histogram(
            'db_query_duration_seconds',
            'Database query duration',
            ['service', 'operation'],
            buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5],
            registry=registry
        ),
        'pool_size': Gauge(
            'db_pool_connections',
            'Database connection pool size',
            ['service', 'state'],
            registry=registry
        )
    }


def create_redis_metrics(registry: CollectorRegistry, service_name: str):
    """Create Redis-related metrics"""
    return {
        'operations': Counter(
            'redis_operations_total',
            'Total Redis operations',
            ['service', 'operation', 'status'],
            registry=registry
        ),
        'latency': Histogram(
            'redis_operation_duration_seconds',
            'Redis operation duration',
            ['service', 'operation'],
            buckets=[.001, .005, .01, .025, .05, .1],
            registry=registry
        )
    }


def create_external_api_metrics(registry: CollectorRegistry, service_name: str):
    """Create external API metrics (Polygon, etc.)"""
    return {
        'requests': Counter(
            'external_api_requests_total',
            'Total external API requests',
            ['service', 'api', 'endpoint', 'status'],
            registry=registry
        ),
        'latency': Histogram(
            'external_api_duration_seconds',
            'External API request duration',
            ['service', 'api', 'endpoint'],
            buckets=[.05, .1, .25, .5, 1.0, 2.5, 5.0],
            registry=registry
        )
    }
