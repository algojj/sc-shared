"""
Event Stream - Redis Streams para distribución confiable de eventos
Mucho más robusto que Pub/Sub - garantiza entrega y no pierde mensajes
"""

import asyncio
import json
import logging
from typing import Callable, Dict, Any, Optional, List
from datetime import datetime, timezone
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class EventProducer:
    """Producer de eventos usando Redis Streams - NUNCA pierde mensajes"""

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis = None

    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=False
            )
            await self.redis.ping()
            logger.info(f"[EVENT-PRODUCER] Connected to Redis Streams at {self.redis_host}:{self.redis_port}")
            return True
        except Exception as e:
            logger.error(f"[EVENT-PRODUCER] Failed to connect to Redis: {e}")
            return False

    async def publish_price_event(self, ticker: str, price: float, volume: int, timestamp: int, **extra_data):
        """
        Publica evento de precio a Redis Stream

        VENTAJA sobre Pub/Sub:
        - ✅ El mensaje se GUARDA hasta que sea consumido
        - ✅ Si un servicio está caído, lo recibe cuando vuelva
        - ✅ No se pierden eventos
        """
        if not self.redis:
            await self.connect()

        try:
            # Stream principal de precios
            stream_key = 'price_events'

            # Preparar datos del evento
            event_data = {
                'ticker': ticker,
                'price': str(price),  # Redis Streams requiere strings
                'volume': str(volume),
                'timestamp': str(timestamp),
                'event_type': 'trade',
                'published_at': datetime.now(timezone.utc).isoformat(),
                **{k: str(v) for k, v in extra_data.items()}
            }

            # XADD - Agregar a stream (PERSISTE el mensaje)
            message_id = await self.redis.xadd(stream_key, event_data, maxlen=10000)

            # También publicar a stream específico de ticker (para servicios que solo quieren 1 ticker)
            ticker_stream = f'price_events:{ticker}'
            await self.redis.xadd(ticker_stream, event_data, maxlen=1000)

            logger.debug(f"📨 Published to stream: {ticker} @ ${price} (ID: {message_id.decode()})")
            return message_id

        except Exception as e:
            logger.error(f"[EVENT-PRODUCER] Error publishing event: {e}")
            return None

    async def close(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            logger.info("[EVENT-PRODUCER] Connection closed")


class EventConsumer:
    """
    Consumer de eventos usando Redis Streams con Consumer Groups

    GARANTÍAS:
    - ✅ At-least-once delivery (cada mensaje se entrega AL MENOS una vez)
    - ✅ No pierde mensajes aunque el servicio se caiga
    - ✅ Múltiples consumidores en paralelo (load balancing automático)
    - ✅ Reintento automático de mensajes fallidos
    """

    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        consumer_group: str = 'default',
        consumer_name: str = 'consumer-1'
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis = None
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.running = False

    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=False
            )
            await self.redis.ping()
            logger.info(f"[EVENT-CONSUMER] Connected as {self.consumer_name} in group {self.consumer_group}")
            return True
        except Exception as e:
            logger.error(f"[EVENT-CONSUMER] Failed to connect to Redis: {e}")
            return False

    async def create_consumer_group(self, stream_key: str):
        """
        Crear consumer group si no existe

        Consumer Groups permiten:
        - Múltiples servicios leyendo el mismo stream
        - Load balancing automático entre consumidores
        - Tracking de qué mensajes ya fueron procesados
        """
        try:
            # XGROUP CREATE - Crear grupo de consumidores
            # '0' significa "leer desde el inicio del stream"
            # '$' significa "leer solo mensajes nuevos desde ahora"
            await self.redis.xgroup_create(
                name=stream_key,
                groupname=self.consumer_group,
                id='$',
                mkstream=True
            )
            logger.info(f"[EVENT-CONSUMER] Created consumer group: {self.consumer_group}")
        except Exception as e:
            # Error BUSYGROUP significa que ya existe - está OK
            if 'BUSYGROUP' not in str(e):
                logger.error(f"[EVENT-CONSUMER] Error creating consumer group: {e}")

    async def consume_events(self, stream_key: str, callback: Callable, block_ms: int = 1000):
        """
        Consume eventos del stream con garantía de entrega

        Args:
            stream_key: Nombre del stream (ej: 'price_events')
            callback: Función async a llamar con cada evento
            block_ms: Tiempo de espera por nuevos mensajes (ms)
        """
        if not self.redis:
            await self.connect()

        # Crear consumer group si no existe
        await self.create_consumer_group(stream_key)

        self.running = True
        logger.info(f"[EVENT-CONSUMER] Started consuming from {stream_key}")

        while self.running:
            try:
                # XREADGROUP - Leer mensajes del stream
                # '>' significa "dame mensajes nuevos que nadie ha procesado"
                # COUNT 10 - Procesar hasta 10 mensajes por batch
                # BLOCK - Esperar por nuevos mensajes
                messages = await self.redis.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams={stream_key: '>'},
                    count=10,
                    block=block_ms
                )

                if messages:
                    for stream_name, stream_messages in messages:
                        for message_id, message_data in stream_messages:
                            event = None
                            try:
                                # Decodificar mensaje
                                event = self._decode_message(message_data)

                                # Procesar evento
                                await callback(event)

                                # XACK - Acknowledge que procesamos el mensaje
                                # Esto le dice a Redis que el mensaje fue procesado exitosamente
                                await self.redis.xack(stream_key, self.consumer_group, message_id)

                                logger.debug(f"✅ Processed event {message_id.decode()}: {event.get('ticker')}")

                            except Exception as e:
                                logger.error(f"❌ Error processing message {message_id}: {e}")
                                logger.error(f"   Exception type: {type(e).__name__}")
                                logger.error(f"   Message data: {message_data}")
                                if event:
                                    logger.error(f"   Decoded event: {event}")
                                import traceback
                                logger.error(f"   Traceback: {traceback.format_exc()}")
                                # No hacemos ACK - el mensaje se reintentará después

            except asyncio.CancelledError:
                logger.info("[EVENT-CONSUMER] Consumer cancelled")
                break
            except Exception as e:
                logger.error(f"[EVENT-CONSUMER] Error in consume loop: {e}")
                await asyncio.sleep(5)

    async def claim_pending_messages(self, stream_key: str, callback: Callable, idle_time_ms: int = 60000):
        """
        Recuperar mensajes pendientes (que otros consumidores no procesaron)

        IMPORTANTE: Esto maneja el caso donde:
        - Un consumidor se cayó antes de hacer ACK
        - Un mensaje falló y necesita reintento
        - Mensajes "stuck" por más de idle_time_ms
        """
        try:
            # XPENDING - Ver mensajes pendientes
            pending = await self.redis.xpending(stream_key, self.consumer_group)

            if pending and pending['pending'] > 0:  # Hay mensajes pendientes
                logger.warning(f"⚠️ Found {pending['pending']} pending messages, claiming them...")

                # XAUTOCLAIM - Reclamar mensajes idle
                claimed = await self.redis.xautoclaim(
                    name=stream_key,
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    min_idle_time=idle_time_ms,
                    start_id='0-0',
                    count=100
                )

                if claimed and len(claimed) > 1:
                    for message_id, message_data in claimed[1]:
                        try:
                            event = self._decode_message(message_data)
                            await callback(event)
                            await self.redis.xack(stream_key, self.consumer_group, message_id)
                            logger.info(f"♻️ Reclaimed and processed: {message_id}")
                        except Exception as e:
                            logger.error(f"Error processing claimed message: {e}")

        except Exception as e:
            logger.error(f"Error claiming pending messages: {e}")

    def _decode_message(self, message_data) -> Dict[str, Any]:
        """Decodificar mensaje de Redis Stream a dict Python

        Args:
            message_data: Puede ser una lista (formato antiguo) o dict (formato redis.asyncio)
        """
        event = {}

        # Manejar ambos formatos: lista o dict
        if isinstance(message_data, dict):
            # Formato dict (redis.asyncio devuelve dict)
            for key_bytes, value_bytes in message_data.items():
                key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
                value = value_bytes.decode() if isinstance(value_bytes, bytes) else value_bytes

                # Convertir strings numéricos de vuelta a números
                if key in ['price', 'volume', 'timestamp']:
                    try:
                        event[key] = float(value) if '.' in value else int(value)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.debug(f"Could not convert '{key}' value '{value}' to number: {e}")
                        event[key] = value
                else:
                    event[key] = value
        else:
            # Formato lista (formato antiguo)
            for i in range(0, len(message_data), 2):
                key = message_data[i].decode() if isinstance(message_data[i], bytes) else message_data[i]
                value = message_data[i+1].decode() if isinstance(message_data[i+1], bytes) else message_data[i+1]

                # Convertir strings numéricos de vuelta a números
                if key in ['price', 'volume', 'timestamp']:
                    try:
                        event[key] = float(value) if '.' in value else int(value)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.debug(f"Could not convert '{key}' value '{value}' to number: {e}")
                        event[key] = value
                else:
                    event[key] = value

        return event

    async def close(self):
        """Close consumer"""
        self.running = False
        if self.redis:
            await self.redis.close()
            logger.info("[EVENT-CONSUMER] Connection closed")


class PriceStreamHandler:
    """
    Handler especializado para eventos de precio con NHOD tracking
    Wrapper conveniente sobre EventConsumer
    """

    def __init__(self, nhod_tracker, consumer_group: str = 'alerts-service'):
        self.nhod_tracker = nhod_tracker
        self.consumer_group = consumer_group
        self.consumer = None
        self.price_callbacks = []

    async def start(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """Iniciar consumer de precios"""
        import socket
        consumer_name = f"{self.consumer_group}-{socket.gethostname()}"

        self.consumer = EventConsumer(
            redis_host=redis_host,
            redis_port=redis_port,
            consumer_group=self.consumer_group,
            consumer_name=consumer_name
        )

        await self.consumer.connect()

        # Crear consumer group
        await self.consumer.create_consumer_group('price_events')

        # Iniciar consumo
        asyncio.create_task(
            self.consumer.consume_events('price_events', self._handle_price_event)
        )

        # Iniciar recuperación de mensajes pendientes cada 1 min
        asyncio.create_task(self._claim_pending_loop())

        logger.info(f"✅ PriceStreamHandler started as {consumer_name}")

    async def _claim_pending_loop(self):
        """Loop para recuperar mensajes pendientes periódicamente"""
        while True:
            await asyncio.sleep(60)  # Cada 1 minuto
            if self.consumer:
                await self.consumer.claim_pending_messages(
                    'price_events',
                    self._handle_price_event,
                    idle_time_ms=30000  # Reclamar mensajes idle > 30s
                )

    def on_price_update(self, callback: Callable):
        """Registrar callback para updates de precio"""
        self.price_callbacks.append(callback)

    async def _handle_price_event(self, event: dict):
        """Procesar evento de precio con NHOD tracking"""
        try:
            ticker = event.get('ticker')
            price = event.get('price')
            volume = event.get('volume', 0)

            if not ticker or not price:
                return

            # Update NHOD tracker
            nhod_update = await self.nhod_tracker.update_with_realtime_price(
                ticker, price, volume
            )

            # Enhanced event
            enhanced_event = {
                **event,
                'nhod_data': nhod_update.get('nhod_data', {}) if nhod_update else {}
            }

            # Call registered callbacks
            for callback in self.price_callbacks:
                try:
                    await callback(enhanced_event)
                except Exception as e:
                    logger.error(f"Error in price callback: {e}")

        except Exception as e:
            logger.error(f"Error handling price event: {e}")

    async def close(self):
        """Close handler"""
        if self.consumer:
            await self.consumer.close()
