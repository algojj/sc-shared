"""
Stream Subscriber - Subscribe to real-time price events from Redis Pub/Sub
DEPRECATED: Use event_stream.EventConsumer for Redis Streams instead
"""

import asyncio
import json
import logging
from typing import Callable, Set, Optional
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class StreamSubscriber:
    """Subscribe to real-time price streams via Redis Pub/Sub"""

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis = None
        self.pubsub = None
        self.subscribed_channels = set()
        self.running = False

    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True
            )
            await self.redis.ping()
            logger.info(f"[STREAM-SUB] Connected to Redis at {self.redis_host}:{self.redis_port}")
            return True
        except Exception as e:
            logger.error(f"[STREAM-SUB] Failed to connect to Redis: {e}")
            return False

    async def subscribe_ticker(self, ticker: str, callback: Callable):
        """
        Subscribe to price updates for a specific ticker

        Args:
            ticker: Stock ticker symbol
            callback: Async function to call with price data (dict)
        """
        channel = f'prices:{ticker}'

        if not self.redis:
            await self.connect()

        try:
            # Create pubsub channel
            channels = await self.redis.subscribe(channel)
            self.subscribed_channels.add(channel)

            logger.info(f"[STREAM-SUB] Subscribed to {channel}")

            # Listen for messages in background
            asyncio.create_task(self._listen_channel(channels[0], callback))

        except Exception as e:
            logger.error(f"[STREAM-SUB] Error subscribing to {ticker}: {e}")

    async def subscribe_all_tickers(self, callback: Callable):
        """
        Subscribe to ALL price updates

        Args:
            callback: Async function to call with price data (dict)
        """
        channel = 'prices:all'

        if not self.redis:
            await self.connect()

        try:
            channels = await self.redis.subscribe(channel)
            self.subscribed_channels.add(channel)

            logger.info(f"[STREAM-SUB] Subscribed to all tickers")

            asyncio.create_task(self._listen_channel(channels[0], callback))

        except Exception as e:
            logger.error(f"[STREAM-SUB] Error subscribing to all tickers: {e}")

    async def _listen_channel(self, channel, callback: Callable):
        """Listen to a specific channel and call callback on messages"""
        self.running = True

        try:
            while self.running and await channel.wait_message():
                msg = await channel.get_json()

                if msg:
                    # Call the callback with the price data
                    try:
                        await callback(msg)
                    except Exception as e:
                        logger.error(f"[STREAM-SUB] Error in callback: {e}")

        except asyncio.CancelledError:
            logger.info("[STREAM-SUB] Channel listener cancelled")
        except Exception as e:
            logger.error(f"[STREAM-SUB] Error listening to channel: {e}")

    async def unsubscribe_all(self):
        """Unsubscribe from all channels"""
        if self.redis:
            for channel in self.subscribed_channels:
                await self.redis.unsubscribe(channel)
            self.subscribed_channels.clear()
            logger.info("[STREAM-SUB] Unsubscribed from all channels")

    async def close(self):
        """Close Redis connection"""
        self.running = False
        await self.unsubscribe_all()

        if self.redis:
            await self.redis.close()
            logger.info("[STREAM-SUB] Redis connection closed")


class PriceEventHandler:
    """Helper class to handle price events with NHOD tracking"""

    def __init__(self, nhod_tracker):
        self.nhod_tracker = nhod_tracker
        self.price_callbacks = []

    def on_price_update(self, callback: Callable):
        """Register a callback for price updates"""
        self.price_callbacks.append(callback)

    async def handle_price_event(self, event: dict):
        """
        Handle incoming price event

        Event format:
        {
            'ticker': 'AAPL',
            'price': 150.25,
            'volume': 1000,
            'timestamp': 1234567890,
            'event_type': 'trade'
        }
        """
        try:
            ticker = event.get('ticker')
            price = event.get('price')
            volume = event.get('volume', 0)

            if not ticker or not price:
                return

            # Update NHOD tracker with real-time price
            nhod_update = await self.nhod_tracker.update_with_realtime_price(
                ticker, price, volume
            )

            # Enhanced event with NHOD data
            enhanced_event = {
                **event,
                'nhod_data': nhod_update.get('nhod_data', {}) if nhod_update else {}
            }

            # Call all registered callbacks
            for callback in self.price_callbacks:
                try:
                    await callback(enhanced_event)
                except Exception as e:
                    logger.error(f"[PRICE-HANDLER] Error in callback: {e}")

        except Exception as e:
            logger.error(f"[PRICE-HANDLER] Error handling price event: {e}")
