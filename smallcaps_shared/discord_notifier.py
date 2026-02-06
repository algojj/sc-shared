"""
Discord notification service for trading alerts
Sends notifications via Discord webhook with Telegram fallback on rate limits
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
import os

logger = logging.getLogger(__name__)

# Telegram fallback configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
# Use TELEGRAM_ALERT_CHAT_ID which is already configured in K8s secrets
TELEGRAM_FALLBACK_CHAT_ID = os.getenv('TELEGRAM_FALLBACK_CHAT_ID') or os.getenv('TELEGRAM_ALERT_CHAT_ID')


class DiscordNotifier:
    """
    Send trading alerts via Discord webhook with Telegram fallback

    Usage:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/...")
        await notifier.send_alert(message="🔥 NHOD: MLTX $18.19 (+26.9%)")

    Features:
        - Automatic Telegram fallback on Discord rate limit (429)
        - Rate limit tracking to avoid spam
    """

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        self.enabled = bool(self.webhook_url)
        self._rate_limit_notified = False  # Track if we already notified about rate limit
        self._rate_limit_count = 0  # Count consecutive rate limits

        if self.enabled:
            logger.info("DiscordNotifier initialized")
        else:
            logger.warning("DiscordNotifier disabled - DISCORD_WEBHOOK_URL not configured")

    async def _send_telegram_fallback(self, message: str, error_info: str = "") -> bool:
        """Send message to Telegram when Discord is rate limited"""
        if not TELEGRAM_BOT_TOKEN:
            logger.warning("Telegram fallback not configured - TELEGRAM_BOT_TOKEN missing")
            return False

        try:
            telegram_msg = f"⚠️ <b>Discord Rate Limited</b>\n\n"
            telegram_msg += f"<b>Error:</b> {error_info}\n\n"
            telegram_msg += f"<b>Original message:</b>\n{message[:500]}"
            if len(message) > 500:
                telegram_msg += "..."

            api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
            payload = {
                'chat_id': TELEGRAM_FALLBACK_CHAT_ID,
                'text': telegram_msg,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        logger.info("✅ Telegram fallback sent successfully")
                        return True
                    else:
                        error = await resp.text()
                        logger.error(f"Telegram fallback failed: {resp.status} - {error}")
                        return False

        except Exception as e:
            logger.error(f"Error sending Telegram fallback: {e}")
            return False

    async def send_alert(
        self,
        message: str,
        username: str = "SmallCaps Scanner",
        avatar_url: str = None,
        telegram_fallback: bool = True
    ) -> bool:
        """
        Send a message to Discord webhook with automatic Telegram fallback on rate limit

        Args:
            message: Message content (supports Discord markdown)
            username: Bot username displayed in Discord
            avatar_url: Optional avatar URL for the bot
            telegram_fallback: If True, send to Telegram when Discord rate limits (429)

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("DiscordNotifier disabled - skipping message")
            return False

        payload = {
            "content": message,
            "username": username,
        }

        if avatar_url:
            payload["avatar_url"] = avatar_url

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status in [200, 204]:
                        logger.debug("Discord message sent successfully")
                        self._rate_limit_count = 0  # Reset counter on success
                        self._rate_limit_notified = False
                        return True

                    elif response.status == 429:
                        # Rate limited by Discord
                        error_text = await response.text()
                        self._rate_limit_count += 1
                        logger.warning(f"Discord rate limited (429): {error_text} [count: {self._rate_limit_count}]")

                        # Send to Telegram as fallback (only first few to avoid spam)
                        if telegram_fallback and self._rate_limit_count <= 3:
                            await self._send_telegram_fallback(
                                message,
                                f"Discord 429 - Rate limited (#{self._rate_limit_count})"
                            )

                        # Try to respect retry_after if provided
                        try:
                            import json
                            error_data = json.loads(error_text)
                            retry_after = error_data.get('retry_after', 0.5)
                            if retry_after and retry_after < 5:  # Max wait 5 seconds
                                await asyncio.sleep(retry_after)
                        except:
                            pass

                        return False

                    else:
                        error_text = await response.text()
                        logger.error(f"Discord webhook error: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error sending Discord message: {e}")
            return False

    async def send_embed(
        self,
        title: str,
        description: str,
        color: int = 0x00ff00,
        fields: list = None,
        username: str = "SmallCaps Scanner",
        timestamp: bool = True,
        telegram_fallback: bool = True
    ) -> bool:
        """
        Send an embed message to Discord webhook with Telegram fallback on rate limit

        Args:
            title: Embed title
            description: Embed description
            color: Embed color (hex value)
            fields: List of {"name": str, "value": str, "inline": bool}
            username: Bot username
            timestamp: Add timestamp to embed
            telegram_fallback: If True, send to Telegram when Discord rate limits (429)

        Returns:
            True if message sent successfully
        """
        if not self.enabled:
            return False

        embed = {
            "title": title,
            "description": description,
            "color": color,
        }

        if timestamp:
            embed["timestamp"] = datetime.now(timezone.utc).isoformat()

        if fields:
            embed["fields"] = fields

        payload = {
            "username": username,
            "embeds": [embed]
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status in [200, 204]:
                        logger.debug("Discord embed sent successfully")
                        self._rate_limit_count = 0
                        return True

                    elif response.status == 429:
                        error_text = await response.text()
                        self._rate_limit_count += 1
                        logger.warning(f"Discord rate limited (429) on embed: {error_text}")

                        # Send simplified version to Telegram
                        if telegram_fallback and self._rate_limit_count <= 3:
                            fallback_msg = f"**{title}**\n{description}"
                            if fields:
                                for f in fields[:5]:
                                    fallback_msg += f"\n• {f.get('name')}: {f.get('value')}"
                            await self._send_telegram_fallback(
                                fallback_msg,
                                f"Discord 429 on embed"
                            )

                        return False

                    else:
                        error_text = await response.text()
                        logger.error(f"Discord webhook error: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error sending Discord embed: {e}")
            return False

    async def send_trading_alert(
        self,
        alert_type: str,
        ticker: str,
        price: float,
        change_percent: float,
        volume: int,
        pm_high: float = None,
        pm_low: float = None,
        rth_high: float = None,
        rth_low: float = None
    ) -> bool:
        """
        Send a formatted trading alert to Discord

        Args:
            alert_type: Type of alert (NHOD, NLOD, HALT, etc.)
            ticker: Stock ticker symbol
            price: Current price
            change_percent: Percentage change
            volume: Volume
            pm_high/pm_low: Premarket high/low
            rth_high/rth_low: Regular trading hours high/low

        Returns:
            True if sent successfully
        """
        # Format volume
        if volume >= 1_000_000:
            vol_str = f"{volume/1_000_000:.1f}M"
        elif volume >= 1_000:
            vol_str = f"{volume/1_000:.0f}K"
        else:
            vol_str = str(volume)

        # Choose emoji based on alert type
        emoji_map = {
            'nhod': '🔥',
            'nlod': '❄️',
            'halt': '⛔',
            'vwap_cross': '📊',
            'breakout': '💥',
            'momentum': '⚡',
            'new_gapper': '🚨'
        }
        emoji = emoji_map.get(alert_type.lower(), '📈')

        # Build compact message
        msg = f"{emoji} **{alert_type.upper()}**: **{ticker}** ${price:.2f} ({change_percent:+.1f}%)"

        if pm_high and pm_high > 0:
            pm_l = pm_low if pm_low and pm_low > 0 else 0
            msg += f" PM:{pm_high:.2f}/{pm_l:.2f}"

        if rth_high and rth_high > 0:
            rth_l = rth_low if rth_low and rth_low > 0 else 0
            msg += f" RTH:{rth_high:.2f}/{rth_l:.2f}"

        msg += f" Vol:{vol_str}"

        return await self.send_alert(msg)
