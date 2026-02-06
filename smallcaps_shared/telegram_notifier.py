"""
Telegram notification service for bug reports and system alerts
Sends notifications directly to Telegram bot with multi-subscriber support
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import List, Optional, TYPE_CHECKING
import os
import json
import pytz

if TYPE_CHECKING:
    from .database_service import DatabaseService

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """
    Send critical system alerts and bug reports via Telegram Bot

    Supports multiple subscribers via database integration.
    Users subscribe by sending /start to the bot.

    Usage:
        notifier = TelegramNotifier(db_service=db_service)
        await notifier.send_message(
            message="🐛 Bug Report from user@example.com",
            parse_mode="HTML"
        )
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        db_service: Optional['DatabaseService'] = None
    ):
        self.bot_token = bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.db_service = db_service

        self.api_url = f'https://api.telegram.org/bot{self.bot_token}' if self.bot_token else None
        self.enabled = bool(self.bot_token)

        if self.enabled:
            logger.info(f"TelegramNotifier initialized - Multi-subscriber mode: {self.db_service is not None}")
        else:
            logger.warning("TelegramNotifier disabled - TELEGRAM_BOT_TOKEN not configured")

    async def _get_active_subscribers(self, subscription_type: str = 'all') -> List[int]:
        """
        Get list of active subscriber chat_ids from database

        Args:
            subscription_type: Filter by subscription type ('all', 'bugs_only', 'alerts_only')

        Returns:
            List of chat_ids (integers)
        """
        if not self.db_service:
            logger.warning("No database service - cannot get subscribers")
            return []

        try:
            query = """
                SELECT chat_id
                FROM telegram_subscribers
                WHERE is_active = true
                AND (subscription_type = $1 OR subscription_type = 'all')
            """

            subscribers = await self.db_service.fetch_all(query, subscription_type)

            chat_ids = [int(sub['chat_id']) for sub in subscribers]

            logger.info(f"Found {len(chat_ids)} active subscribers for type '{subscription_type}'")
            return chat_ids

        except Exception as e:
            logger.error(f"Error fetching subscribers: {e}")
            return []

    async def send_message(
        self,
        message: str,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        subscription_type: str = 'all'
    ) -> bool:
        """
        Send message to all active Telegram subscribers

        Args:
            message: Message text (supports HTML or Markdown based on parse_mode)
            parse_mode: "HTML", "Markdown", or "MarkdownV2"
            disable_web_page_preview: Don't show link previews
            subscription_type: Filter subscribers by type ('all', 'bugs_only', 'alerts_only')

        Returns:
            True if message sent to at least one subscriber successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("TelegramNotifier disabled - skipping message")
            return False

        try:
            # Get active subscribers from database
            chat_ids = await self._get_active_subscribers(subscription_type)

            if not chat_ids:
                logger.warning(f"No active subscribers found for type '{subscription_type}'")
                return False

            # Split message if too long (Telegram limit: 4096 chars)
            message_parts = self._split_message(message, max_length=4000)

            total_success = 0
            total_failed = 0

            for chat_id in chat_ids:
                success_count = 0
                for part in message_parts:
                    if await self._send_telegram_request(part, parse_mode, disable_web_page_preview, chat_id):
                        success_count += 1
                        # Small delay between messages to avoid rate limits
                        if len(message_parts) > 1:
                            await asyncio.sleep(0.5)

                if success_count == len(message_parts):
                    total_success += 1
                else:
                    total_failed += 1
                    logger.warning(f"Failed to send {len(message_parts) - success_count}/{len(message_parts)} message parts to chat_id {chat_id}")

            logger.info(f"Telegram message sent to {total_success}/{len(chat_ids)} subscribers ({total_failed} failed)")

            return total_success > 0

        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

    async def _send_telegram_request(
        self,
        message: str,
        parse_mode: str,
        disable_web_page_preview: bool,
        chat_id: int
    ) -> bool:
        """Send single message to specific chat_id via Telegram API"""
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': disable_web_page_preview
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f'{self.api_url}/sendMessage',
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        logger.debug("Telegram message sent successfully")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Telegram API error: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error sending Telegram request: {e}")
            return False

    def _split_message(self, message: str, max_length: int = 4000) -> List[str]:
        """Split long messages into chunks"""
        if len(message) <= max_length:
            return [message]

        parts = []
        current_part = ""

        for line in message.split('\n'):
            if len(current_part) + len(line) + 1 <= max_length:
                current_part += line + '\n'
            else:
                if current_part:
                    parts.append(current_part)
                current_part = line + '\n'

        if current_part:
            parts.append(current_part)

        return parts

    async def send_bug_report(
        self,
        bug_id: int,
        user_email: str,
        user_name: str,
        user_role: str,
        description: str,
        url: str,
        user_agent: str,
        screenshot_url: Optional[str] = None,
        logs: Optional[List[dict]] = None,
        source: Optional[str] = None,
        ticker: Optional[str] = None,
        row_data: Optional[dict] = None,
        row_html: Optional[str] = None,
        grid_context: Optional[dict] = None
    ) -> bool:
        """
        Send bug report to Telegram with full scanner context

        Args:
            bug_id: Bug report ID from database
            user_email: User who reported the bug
            user_name: User's display name
            user_role: User's role (admin, user, etc)
            description: Bug description
            url: URL where bug occurred
            user_agent: Browser user agent
            screenshot_url: Optional screenshot URL
            logs: Optional list of log entries
            source: Scanner/component source (Gap Scanner, Momentum Scanner, etc)
            ticker: Stock ticker symbol
            row_data: Full row data from scanner
            row_html: HTML of the problematic row
            grid_context: Grid metadata (headers, filters, surrounding rows)

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        try:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            # Build Telegram message with HTML formatting
            message = f"""🐛 <b>BUG REPORT #{bug_id}</b>

<b>Usuario que reportó:</b>
• Email: <code>{user_email}</code>
• Nombre: {user_name}
• Rol: {user_role}

<b>Descripción del problema:</b>
{self._escape_html(description)}

<b>Origen del reporte:</b>"""

            if source:
                message += f"\n• Fuente: <b>{source}</b>"
            if ticker:
                message += f"\n• Ticker: <b>${ticker}</b>"

            message += f"\n• URL: <code>{url}</code>"
            message += f"\n• User Agent: <code>{user_agent[:100]}...</code>"
            message += f"\n• Timestamp: {timestamp}"

            if screenshot_url:
                message += f"\n• Screenshot: {screenshot_url}"

            # Add row data (key fields only to avoid message length limits)
            if row_data:
                message += f"\n\n<b>📊 Datos de la fila ({ticker or 'N/A'}):</b>"

                # Priority fields to show (most relevant for debugging)
                priority_fields = ['price', 'current_price', 'gap_percent', 'change_percent',
                                 'volume', 'volume_premarket', 'prev_close', 'float_shares',
                                 'vwap', 'momentum_score', 'session']

                for field in priority_fields:
                    if field in row_data:
                        value = row_data[field]
                        # Format numbers nicely
                        if isinstance(value, (int, float)):
                            if field.endswith('_percent'):
                                value = f"{value:.2f}%"
                            elif field in ['price', 'current_price', 'prev_close', 'vwap']:
                                value = f"${value:.2f}"
                            elif field == 'volume' or field == 'volume_premarket':
                                value = f"{value:,}" if isinstance(value, int) else value

                        message += f"\n  • {field}: <code>{value}</code>"

                # Count remaining fields
                remaining = len(row_data) - len([f for f in priority_fields if f in row_data])
                if remaining > 0:
                    message += f"\n  <i>...y {remaining} campos más</i>"

            # Add grid context (filters, sort, surrounding rows)
            if grid_context:
                filters = grid_context.get('filters', {})
                if filters:
                    message += f"\n\n<b>🔍 Filtros activos:</b>"
                    for key, value in list(filters.items())[:5]:  # Limit to 5 filters
                        message += f"\n  • {key}: <code>{value}</code>"

                surrounding = grid_context.get('surroundingRows', [])
                if surrounding:
                    message += f"\n\n<b>📍 Filas cercanas (contexto):</b>"
                    for row in surrounding[:3]:  # Show max 3 rows
                        is_current = row.get('isCurrentRow', False)
                        ticker_sym = row.get('ticker', 'N/A')
                        price = row.get('price', 'N/A')
                        gap = row.get('gap_percent', row.get('change_percent', 'N/A'))

                        indicator = "👉" if is_current else "  "
                        message += f"\n{indicator} <code>{ticker_sym:6} ${price:6} {gap:+6.1f}%</code>"

            # Add HTML snippet (first 500 chars only - Telegram has message limits)
            if row_html:
                html_preview = row_html[:500].replace('<', '&lt;').replace('>', '&gt;')
                message += f"\n\n<b>🔍 HTML del row (preview):</b>\n<pre>{html_preview}...</pre>"

            if logs and len(logs) > 0:
                message += f"\n\n<b>Logs ({len(logs)} eventos):</b>"
                # Add first 5 logs (reduced from 10 to save space)
                for log in logs[:5]:
                    log_time = log.get('timestamp', 'N/A')
                    log_level = log.get('level', 'INFO')
                    log_msg = log.get('message', 'N/A')
                    message += f"\n[{log_time}] {log_level}: {self._escape_html(log_msg[:80])}"

                if len(logs) > 5:
                    message += f"\n<i>...y {len(logs) - 5} logs más</i>"

            message += f"\n\n<i>Frontend: http://15.235.115.4</i>"

            # Telegram has a 4096 character limit per message
            if len(message) > 4096:
                # Split into multiple messages if needed
                message = message[:4090] + "...\n<i>[Mensaje truncado]</i>"
                logger.warning(f"Telegram bug report message truncated (was {len(message)} chars)")

            return await self.send_message(message, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Failed to send bug report to Telegram: {e}")
            return False

    async def send_critical_alert(
        self,
        subject: str,
        message: str,
        service_name: str,
        error_details: Optional[dict] = None,
        log_url: Optional[str] = None
    ) -> bool:
        """
        Send critical system alert to Telegram

        Args:
            subject: Alert subject
            message: Alert message
            service_name: Name of service reporting
            error_details: Optional error metadata
            log_url: Optional URL to logs

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        try:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            telegram_msg = f"""🚨 <b>CRITICAL ALERT</b>

<b>{subject}</b>

<b>Service:</b> {service_name}
<b>Time:</b> {timestamp}

<b>Message:</b>
{self._escape_html(message)}"""

            if error_details:
                telegram_msg += "\n\n<b>Error Details:</b>"
                for key, value in error_details.items():
                    telegram_msg += f"\n• {key}: {self._escape_html(str(value))}"

            if log_url:
                telegram_msg += f"\n\n<a href='{log_url}'>View Full Logs</a>"

            telegram_msg += "\n\n<i>SmallCaps Scanner Monitoring System</i>"

            return await self.send_message(telegram_msg, parse_mode="HTML", subscription_type='alerts_only')

        except Exception as e:
            logger.error(f"Failed to send critical alert to Telegram: {e}")
            return False

    async def send_user_notification(
        self,
        title: str,
        body: str,
        notification_type: str = "info"
    ) -> bool:
        """
        Send user notification to Telegram

        Args:
            title: Notification title
            body: Notification body
            notification_type: "info", "warning", "error", "success"

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        # Choose emoji based on type
        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "success": "✅"
        }
        emoji = emoji_map.get(notification_type, "ℹ️")

        try:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            telegram_msg = f"""{emoji} <b>{title}</b>

{self._escape_html(body)}

<i>{timestamp}</i>"""

            return await self.send_message(telegram_msg, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Failed to send user notification to Telegram: {e}")
            return False

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters for Telegram"""
        if not text:
            return ""

        text = str(text)
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        text = text.replace('"', '&quot;')
        return text

    async def send_alert_report(
        self,
        ticker: str,
        alert_type: str,
        alert_data: dict,
        reporter_notes: Optional[str] = None,
        user_email: Optional[str] = None,
        user_name: Optional[str] = None,
        user_role: Optional[str] = None
    ) -> bool:
        """
        Send alert report to Telegram

        Args:
            ticker: Ticker symbol
            alert_type: Type of alert (vwap_cross, nhod, etc)
            alert_data: Full alert data
            reporter_notes: Optional notes from reporter
            user_email: Optional email of reporter
            user_name: Optional name of reporter
            user_role: Optional role of reporter

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        try:
            # Use alert's original timestamp converted to Eastern Time
            alert_timestamp_str = alert_data.get('timestamp')
            if alert_timestamp_str:
                try:
                    # Parse ISO timestamp (e.g., "2025-11-12T09:17:23.919825")
                    if isinstance(alert_timestamp_str, str):
                        # Remove timezone suffix if present and parse
                        alert_timestamp_str = alert_timestamp_str.replace('Z', '+00:00')
                        if '+' in alert_timestamp_str or alert_timestamp_str.endswith('Z'):
                            alert_dt = datetime.fromisoformat(alert_timestamp_str)
                        else:
                            # Assume UTC if no timezone info
                            alert_dt = datetime.fromisoformat(alert_timestamp_str).replace(tzinfo=timezone.utc)
                    else:
                        alert_dt = alert_timestamp_str

                    # Convert to Eastern Time
                    et_tz = pytz.timezone('America/New_York')
                    alert_dt_et = alert_dt.astimezone(et_tz)
                    timestamp = alert_dt_et.strftime('%Y-%m-%d %H:%M:%S ET')
                except Exception as e:
                    logger.warning(f"Could not parse alert timestamp '{alert_timestamp_str}': {e}, using current time")
                    timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S ET')
            else:
                # Fallback to current time in ET if no timestamp in alert_data
                timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S ET')

            price = alert_data.get('price', 'N/A')

            telegram_msg = f"""📊 <b>ALERT REPORT - {ticker}</b>

<b>Alert Type:</b> {alert_type.upper()}
<b>Price:</b> ${price}
<b>Timestamp:</b> {timestamp}"""

            if user_email or user_name:
                telegram_msg += f"\n\n<b>Reportado por:</b>"
                if user_name:
                    telegram_msg += f"\n• Nombre: {user_name}"
                if user_email:
                    telegram_msg += f"\n• Email: <code>{user_email}</code>"
                if user_role:
                    telegram_msg += f"\n• Rol: {user_role}"

            if reporter_notes:
                telegram_msg += f"\n\n<b>Reporter Notes:</b>\n{self._escape_html(reporter_notes)}"

            telegram_msg += f"\n\n<b>Alert Details:</b>\n<pre>{json.dumps(alert_data, indent=2, default=str)[:1000]}</pre>"

            telegram_msg += f"\n\n<i>Frontend: http://15.235.115.4</i>"

            return await self.send_message(telegram_msg, parse_mode="HTML", subscription_type='bugs_only')

        except Exception as e:
            logger.error(f"Failed to send alert report to Telegram: {e}")
            return False
