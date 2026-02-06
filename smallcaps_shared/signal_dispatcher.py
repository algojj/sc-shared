"""
Strategy Signal Dispatcher - v1.0.0

Dispatches trading strategy signals (WATCH, ENTRY, AT_LEVEL) to configured webhooks.
Tracks signal_id and notifies Discord on delivery failures.

Usage:
    from smallcaps_shared import StrategySignalDispatcher, StrategySignal

    dispatcher = StrategySignalDispatcher(db_service)
    await dispatcher.init()

    signal = StrategySignal(
        strategy="BHT",
        signal_type="WATCH",
        ticker="LAZO",
        bias="SHORT",
        price=5.50,
        ...
    )
    result = await dispatcher.dispatch_signal(signal)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import httpx

logger = logging.getLogger(__name__)

# Discord webhook for failure notifications
FAILURE_DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1469415750321181017/G_xRIoWA_f7xGkLGJtw-u3diTydhxL7wGDOXoW_U29Bs1Wqk0Us2FrQb2_zAdSEfoXzo"


@dataclass
class StrategySignal:
    """Standard signal format for all trading strategies"""

    # Required fields
    strategy: str           # BHT, MFL, CLIMAX, GUS, PMB, MDB
    signal_type: str        # WATCH, AT_LEVEL, ENTRY, BREAKOUT
    ticker: str
    bias: str               # LONG or SHORT
    price: float

    # Optional price levels
    level_entry: Optional[float] = None
    level_tp: Optional[float] = None
    level_sl: Optional[float] = None

    # Market data
    gap_percentage: Optional[float] = None
    float_shares: Optional[int] = None
    marketcap: Optional[int] = None

    # Volume data
    vol_acum: Optional[int] = None
    vol_premarket: Optional[int] = None
    vol_rth: Optional[int] = None
    vol_afterhours: Optional[int] = None

    # Additional context
    country: Optional[str] = None
    note: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Auto-generated
    signal_id: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        """Generate signal_id and timestamp if not provided"""
        if not self.signal_id:
            ts = int(time.time())
            self.signal_id = f"{self.strategy}-{self.signal_type}-{self.ticker}-{ts}"
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc)

    def to_payload(self) -> Dict[str, Any]:
        """Convert to webhook payload format"""
        payload = {
            "signal_id": self.signal_id,
            "strategy": self.strategy,
            "signal_type": self.signal_type,
            "ticker": self.ticker,
            "bias": self.bias,
            "price": round(self.price, 4) if self.price else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }

        # Add optional fields if present
        if self.country:
            payload["country"] = self.country
        if self.level_entry is not None:
            payload["level_entry"] = round(self.level_entry, 4)
        if self.level_tp is not None:
            payload["level_tp"] = round(self.level_tp, 4)
        if self.level_sl is not None:
            payload["level_sl"] = round(self.level_sl, 4)
        if self.gap_percentage is not None:
            payload["gap_percentage"] = round(self.gap_percentage, 2)
        if self.float_shares is not None:
            payload["float"] = self.float_shares
        if self.marketcap is not None:
            payload["marketcap"] = self.marketcap
        if self.vol_acum is not None:
            payload["vol_acum"] = self.vol_acum
        if self.vol_premarket is not None:
            payload["vol_premarket"] = self.vol_premarket
        if self.vol_rth is not None:
            payload["vol_rth"] = self.vol_rth
        if self.vol_afterhours is not None:
            payload["vol_afterhours"] = self.vol_afterhours
        if self.note:
            payload["note"] = self.note
        if self.metadata:
            payload["metadata"] = self.metadata

        return payload


@dataclass
class DispatchResult:
    """Result of signal dispatch operation"""
    signal_id: str
    webhooks_total: int
    webhooks_success: int
    webhooks_failed: int
    stored: bool
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True if all webhooks succeeded"""
        return self.webhooks_failed == 0 and self.webhooks_total > 0

    @property
    def partial(self) -> bool:
        """True if some webhooks succeeded but not all"""
        return self.webhooks_success > 0 and self.webhooks_failed > 0


class StrategySignalDispatcher:
    """
    Centralized dispatcher for strategy signals.

    Responsibilities:
    1. Generate unique signal_id
    2. Lookup webhooks filtered by strategy
    3. Send to all matching webhooks with retry
    4. Store signal in database (if webhooks exist)
    5. Notify Discord on failures
    """

    def __init__(
        self,
        db_service,
        failure_webhook: str = FAILURE_DISCORD_WEBHOOK,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 10.0
    ):
        self.db = db_service
        self.failure_webhook = failure_webhook
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.http_client: Optional[httpx.AsyncClient] = None

        # Stats
        self.signals_dispatched = 0
        self.signals_failed = 0
        self.webhooks_sent = 0
        self.webhooks_failed = 0

    async def init(self):
        """Initialize HTTP client"""
        if not self.http_client:
            self.http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=5.0),
                limits=httpx.Limits(max_connections=20)
            )
        logger.info("StrategySignalDispatcher initialized")

    async def close(self):
        """Close HTTP client"""
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None

    async def dispatch_signal(self, signal: StrategySignal) -> DispatchResult:
        """
        Dispatch a signal to all matching webhooks.

        Args:
            signal: StrategySignal to dispatch

        Returns:
            DispatchResult with status and stats
        """
        await self._ensure_client()

        # Get matching webhooks
        webhooks = await self._get_matching_webhooks(signal.strategy, signal.signal_type)

        if not webhooks:
            logger.debug(f"No webhooks configured for strategy={signal.strategy}")
            return DispatchResult(
                signal_id=signal.signal_id,
                webhooks_total=0,
                webhooks_success=0,
                webhooks_failed=0,
                stored=False
            )

        # Store signal in database
        stored = await self._store_signal(signal, len(webhooks))

        # Dispatch to all webhooks
        payload = signal.to_payload()
        results = await asyncio.gather(
            *[self._send_to_webhook(w, payload, signal) for w in webhooks],
            return_exceptions=True
        )

        # Count successes and failures
        successes = sum(1 for r in results if r is True)
        failures = len(results) - successes
        errors = [str(r) for r in results if isinstance(r, Exception)]

        # Update stats
        self.signals_dispatched += 1
        self.webhooks_sent += successes
        self.webhooks_failed += failures

        if failures > 0:
            self.signals_failed += 1

        # Update database with dispatch results
        await self._update_dispatch_status(signal.signal_id, successes, failures)

        result = DispatchResult(
            signal_id=signal.signal_id,
            webhooks_total=len(webhooks),
            webhooks_success=successes,
            webhooks_failed=failures,
            stored=stored,
            errors=errors
        )

        if result.success:
            logger.info(f"Signal dispatched: {signal.signal_id} → {successes}/{len(webhooks)} webhooks")
        elif result.partial:
            logger.warning(f"Signal partial: {signal.signal_id} → {successes}/{len(webhooks)} webhooks OK")
        else:
            logger.error(f"Signal failed: {signal.signal_id} → 0/{len(webhooks)} webhooks")

        return result

    async def _ensure_client(self):
        """Ensure HTTP client is initialized"""
        if not self.http_client:
            await self.init()

    async def _get_matching_webhooks(
        self,
        strategy: str,
        signal_type: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get webhooks that match the given strategy.

        A webhook matches if:
        - is_active = true
        - filters is NULL/empty (receives all) OR
        - filters->strategies contains this strategy
        """
        try:
            query = """
                SELECT id, user_id, name, url, secret_token, filters
                FROM user_webhooks
                WHERE is_active = true
                  AND (
                    filters IS NULL
                    OR filters = '{}'::jsonb
                    OR filters->'strategies' IS NULL
                    OR filters->'strategies' @> $1::jsonb
                  )
            """
            rows = await self.db.fetch(query, f'["{strategy}"]')

            webhooks = []
            for row in rows:
                # Additional filter: check signal_type if specified in filters
                filters = row.get('filters') or {}
                signal_types = filters.get('signal_types', [])

                # If signal_types filter is set, check if this signal_type is allowed
                if signal_types and signal_type and signal_type not in signal_types:
                    continue

                webhooks.append({
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'name': row['name'],
                    'url': row['url'],
                    'secret_token': row.get('secret_token'),
                    'filters': filters
                })

            logger.debug(f"Found {len(webhooks)} webhooks for strategy={strategy}")
            return webhooks

        except Exception as e:
            logger.error(f"Error fetching webhooks: {e}")
            return []

    async def _send_to_webhook(
        self,
        webhook: Dict[str, Any],
        payload: Dict[str, Any],
        signal: StrategySignal
    ) -> bool:
        """
        Send payload to a single webhook with retry logic.

        Returns:
            True if successful, False otherwise
        """
        url = webhook['url']
        webhook_id = webhook['id']
        last_error = None
        start_time = time.time()

        for attempt in range(1, self.max_retries + 1):
            try:
                headers = {"Content-Type": "application/json"}

                # Add HMAC signature if secret_token is configured
                if webhook.get('secret_token'):
                    import hashlib
                    import hmac
                    import json as json_lib
                    body = json_lib.dumps(payload)
                    signature = hmac.new(
                        webhook['secret_token'].encode(),
                        body.encode(),
                        hashlib.sha256
                    ).hexdigest()
                    headers["X-Smallcaps-Signature"] = f"sha256={signature}"

                response = await self.http_client.post(
                    url,
                    json=payload,
                    headers=headers
                )

                response_time = int((time.time() - start_time) * 1000)

                if response.status_code in (200, 201, 202, 204):
                    # Log successful delivery
                    await self._log_delivery(
                        signal.signal_id,
                        webhook_id,
                        payload,
                        response.status_code,
                        response.text[:500] if response.text else None,
                        response_time,
                        'success',
                        None,
                        attempt
                    )
                    logger.debug(f"Webhook {webhook['name']} OK (attempt {attempt})")
                    return True
                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                    logger.warning(f"Webhook {webhook['name']} failed: {last_error}")

            except httpx.TimeoutException as e:
                last_error = f"Timeout: {e}"
                logger.warning(f"Webhook {webhook['name']} timeout (attempt {attempt})")

            except httpx.ConnectError as e:
                last_error = f"Connection error: {e}"
                logger.warning(f"Webhook {webhook['name']} connection error")

            except Exception as e:
                last_error = f"Error: {e}"
                logger.error(f"Webhook {webhook['name']} error: {e}")

            # Wait before retry
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * attempt)

        # All retries failed
        response_time = int((time.time() - start_time) * 1000)
        await self._log_delivery(
            signal.signal_id,
            webhook_id,
            payload,
            None,
            None,
            response_time,
            'failed',
            last_error,
            self.max_retries
        )

        # Notify Discord about failure
        await self._notify_failure(signal, webhook, last_error)
        return False

    async def _store_signal(self, signal: StrategySignal, webhook_count: int) -> bool:
        """Store signal in strategy_signals table"""
        try:
            query = """
                INSERT INTO strategy_signals (
                    signal_id, ticker, strategy_name, signal_status,
                    entry_price, stop_price, target_price,
                    bias, country, gap_percentage, float_shares, market_cap,
                    vol_premarket, vol_rth, vol_afterhours, vol_acum,
                    notes, metadata,
                    dispatch_status, webhooks_total, dispatched_at
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16,
                    $17, $18,
                    'dispatching', $19, NOW()
                )
                ON CONFLICT (signal_id) DO NOTHING
            """
            await self.db.execute(
                query,
                signal.signal_id,
                signal.ticker,
                signal.strategy,
                signal.signal_type,
                signal.level_entry,
                signal.level_sl,
                signal.level_tp,
                signal.bias,
                signal.country,
                signal.gap_percentage,
                signal.float_shares,
                signal.marketcap,
                signal.vol_premarket,
                signal.vol_rth,
                signal.vol_afterhours,
                signal.vol_acum,
                signal.note,
                signal.metadata if signal.metadata else None,
                webhook_count
            )
            logger.debug(f"Signal stored: {signal.signal_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to store signal: {e}")
            return False

    async def _update_dispatch_status(
        self,
        signal_id: str,
        successes: int,
        failures: int
    ):
        """Update dispatch status in database"""
        try:
            status = 'success' if failures == 0 else ('partial' if successes > 0 else 'failed')
            query = """
                UPDATE strategy_signals
                SET dispatch_status = $1,
                    webhooks_success = $2,
                    webhooks_failed = $3
                WHERE signal_id = $4
            """
            await self.db.execute(query, status, successes, failures, signal_id)

        except Exception as e:
            logger.error(f"Failed to update dispatch status: {e}")

    async def _log_delivery(
        self,
        signal_id: str,
        webhook_id: int,
        payload: Dict,
        http_status: Optional[int],
        response_body: Optional[str],
        response_time_ms: int,
        status: str,
        error_message: Optional[str],
        attempt: int
    ):
        """Log delivery attempt to signal_dispatch_log table"""
        try:
            import json as json_lib
            query = """
                INSERT INTO signal_dispatch_log (
                    signal_id, webhook_id, payload,
                    http_status, response_body, response_time_ms,
                    status, error_message, attempt_number,
                    delivered_at
                ) VALUES (
                    $1, $2, $3,
                    $4, $5, $6,
                    $7, $8, $9,
                    CASE WHEN $7 = 'success' THEN NOW() ELSE NULL END
                )
            """
            await self.db.execute(
                query,
                signal_id,
                webhook_id,
                json_lib.dumps(payload),
                http_status,
                response_body,
                response_time_ms,
                status,
                error_message,
                attempt
            )

        except Exception as e:
            logger.error(f"Failed to log delivery: {e}")

    async def _notify_failure(
        self,
        signal: StrategySignal,
        webhook: Dict[str, Any],
        error: str
    ):
        """Send Discord notification about webhook delivery failure"""
        if not self.failure_webhook:
            logger.error(f"WEBHOOK FAILED: {signal.signal_id} to {webhook['name']} - {error}")
            return

        try:
            embed = {
                "title": f"Webhook Delivery Failed",
                "color": 0xFF0000,  # Red
                "description": f"Failed to deliver signal after {self.max_retries} retries",
                "fields": [
                    {"name": "Signal ID", "value": signal.signal_id, "inline": True},
                    {"name": "Strategy", "value": signal.strategy, "inline": True},
                    {"name": "Ticker", "value": signal.ticker, "inline": True},
                    {"name": "Signal Type", "value": signal.signal_type, "inline": True},
                    {"name": "Webhook", "value": webhook['name'], "inline": True},
                    {"name": "Webhook ID", "value": str(webhook['id']), "inline": True},
                    {"name": "Error", "value": f"```{error[:400]}```", "inline": False},
                ],
                "footer": {"text": "SmallCaps Signal Dispatcher"},
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            await self.http_client.post(
                self.failure_webhook,
                json={"embeds": [embed]},
                headers={"Content-Type": "application/json"}
            )
            logger.info(f"Discord failure notification sent for {signal.signal_id}")

        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get dispatcher statistics"""
        return {
            "signals_dispatched": self.signals_dispatched,
            "signals_failed": self.signals_failed,
            "webhooks_sent": self.webhooks_sent,
            "webhooks_failed": self.webhooks_failed,
            "success_rate": (
                f"{self.webhooks_sent / (self.webhooks_sent + self.webhooks_failed) * 100:.1f}%"
                if (self.webhooks_sent + self.webhooks_failed) > 0
                else "N/A"
            )
        }
