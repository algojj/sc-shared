"""Base scanner class for all microservice scanners"""

import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional

from .redis_client import RedisClient, CacheKeys
from .models import ScanResult, HealthStatus

logger = logging.getLogger(__name__)


class BaseScannerService(ABC):
    """Base class for all scanner microservices"""

    def __init__(self, service_name: str, scanner_type: str, scan_interval: int = 30):
        self.service_name = service_name
        self.scanner_type = scanner_type
        self.scan_interval = scan_interval
        self.redis = RedisClient()
        self.running = False
        self.scanner = self._create_scanner()

    @abstractmethod
    def _create_scanner(self):
        """Create the specific scanner instance"""
        pass

    async def start(self):
        """Start the scanner service"""
        logger.info(f"[{self.service_name.upper()}] Starting...")

        # Connect to Redis
        await self.redis.connect()
        logger.info(f"[{self.service_name.upper()}] Connected to Redis")

        # Start scanning loop
        self.running = True
        await self._scan_loop()

    async def stop(self):
        """Stop the scanner service"""
        logger.info(f"[{self.service_name.upper()}] Stopping...")
        self.running = False
        if self.redis:
            await self.redis.close()

    async def _scan_loop(self):
        """Main scanning loop"""
        while self.running:
            try:
                start_time = time.time()

                logger.info(f"[{self.service_name.upper()}] Starting {self.scanner_type} scan...")
                results = await self.scanner.scan()

                if results:
                    await self._cache_results(results)
                    await self._log_scan_summary(results)
                else:
                    logger.warning(f"[{self.service_name.upper()}] No results found")

                # Update health status
                await self._update_health_status()

                # Calculate scan duration and log
                scan_duration = time.time() - start_time
                logger.info(f"[{self.service_name.upper()}] Scan completed in {scan_duration:.2f}s")

                # Wait for next scan
                await asyncio.sleep(self.scan_interval)

            except Exception as e:
                logger.error(f"[{self.service_name.upper()}] Scan error: {e}")
                await asyncio.sleep(10)  # Wait before retrying

    async def _enrich_with_vwap(self, results: List[ScanResult]) -> List[ScanResult]:
        """Enrich scan results with VWAP from Redis"""
        try:
            for result in results:
                try:
                    # Get VWAP from Redis (stored by polygon-stream-service)
                    vwap_str = await self.redis.get(f'vwap:{result.ticker}')
                    if vwap_str:
                        result.vwap = float(vwap_str)
                        logger.debug(f"[{self.service_name.upper()}] Enriched {result.ticker} with VWAP ${result.vwap:.2f}")
                except Exception as e:
                    logger.debug(f"[{self.service_name.upper()}] Could not get VWAP for {result.ticker}: {e}")
                    # Continue without VWAP (not critical)

            return results

        except Exception as e:
            logger.error(f"[{self.service_name.upper()}] Error enriching with VWAP: {e}")
            return results  # Return original results if enrichment fails

    async def _cache_results(self, results: List[ScanResult]):
        """Cache scan results in Redis (with VWAP enrichment)"""
        try:
            # Enrich results with VWAP before caching
            enriched_results = await self._enrich_with_vwap(results)

            cache_key = CacheKeys.scan_results(self.scanner_type)
            cache_data = [result.model_dump() for result in enriched_results]

            success = await self.redis.set(
                cache_key,
                cache_data,
                ttl=60  # 1 minute TTL
            )

            if success:
                logger.info(f"[{self.service_name.upper()}] ✅ Cached {len(enriched_results)} {self.scanner_type} stocks (with VWAP)")
            else:
                logger.error(f"[{self.service_name.upper()}] ❌ Failed to cache results")

        except Exception as e:
            logger.error(f"[{self.service_name.upper()}] Error caching results: {e}")

    async def _log_scan_summary(self, results: List[ScanResult]):
        """Log summary of scan results"""
        if not results:
            return

        if hasattr(self, '_log_custom_summary'):
            await self._log_custom_summary(results)
        else:
            # Default summary logging
            top_result = results[0]
            logger.info(f"[{self.service_name.upper()}] Top result: {top_result.ticker} @ ${top_result.price:.2f}")

    async def _update_health_status(self):
        """Update health status in Redis"""
        try:
            health_key = CacheKeys.health_check(self.service_name)
            health_status = HealthStatus(
                service=self.service_name,
                status="healthy",
                timestamp=datetime.now(timezone.utc).isoformat()
            )

            await self.redis.set(
                health_key,
                health_status.model_dump(),
                ttl=120  # 2 minutes TTL
            )

        except Exception as e:
            logger.error(f"[{self.service_name.upper()}] Error updating health: {e}")