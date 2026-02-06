"""
Email notification service using PostmarkApp
Sends critical alerts to configured email addresses
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import List, Optional
import os
import pytz

logger = logging.getLogger(__name__)


class EmailNotifier:
    """
    Send critical system alerts via PostmarkApp

    Usage:
        notifier = EmailNotifier()
        await notifier.send_critical_alert(
            subject="WebSocket Disconnected",
            message="Polygon WebSocket failed after 5 reconnection attempts",
            service_name="polygon-stream-service",
            error_details={"attempts": 5, "last_error": "Connection timeout"}
        )
    """

    def __init__(
        self,
        server_token: Optional[str] = None,
        from_email: Optional[str] = None,
        to_emails: Optional[List[str]] = None
    ):
        self.server_token = server_token or os.getenv('POSTMARK_SERVER_TOKEN', '356669a7-c665-4e74-9acb-e722a8664338')
        self.from_email = from_email or os.getenv('POSTMARK_FROM_EMAIL', 'hola@jose.ar')

        # Parse to_emails from env or use default
        default_to = 'jsfrncœgmail.com;hola@jose.ar'
        to_emails_str = os.getenv('POSTMARK_TO_EMAILS', default_to)
        self.to_emails = to_emails or [email.strip() for email in to_emails_str.split(';')]

        self.api_url = 'https://api.postmarkapp.com/email'
        self.enabled = bool(self.server_token)

        logger.info(f"EmailNotifier initialized - From: {self.from_email}, To: {self.to_emails}")

    async def send_critical_alert(
        self,
        subject: str,
        message: str,
        service_name: str,
        error_details: Optional[dict] = None,
        log_url: Optional[str] = None
    ) -> bool:
        """
        Send critical alert email via PostmarkApp

        Args:
            subject: Email subject line
            message: Main alert message
            service_name: Name of the service reporting the error
            error_details: Optional dict with error metadata
            log_url: Optional URL to view full logs

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("EmailNotifier disabled - POSTMARK_SERVER_TOKEN not configured")
            return False

        try:
            # Build HTML email body
            html_body = self._build_html_body(
                subject=subject,
                message=message,
                service_name=service_name,
                error_details=error_details,
                log_url=log_url
            )

            # Build text body (fallback)
            text_body = self._build_text_body(
                subject=subject,
                message=message,
                service_name=service_name,
                error_details=error_details,
                log_url=log_url
            )

            # Send to each recipient
            tasks = []
            for to_email in self.to_emails:
                tasks.append(self._send_email(
                    to_email=to_email,
                    subject=f"ALERT: {subject}",
                    html_body=html_body,
                    text_body=text_body
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check if all emails sent successfully
            success_count = sum(1 for r in results if r is True)
            logger.info(f"Sent {success_count}/{len(self.to_emails)} alert emails: {subject}")

            return success_count > 0

        except Exception as e:
            logger.error(f"Failed to send critical alert email: {e}")
            return False

    async def _send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        text_body: str
    ) -> bool:
        """Send email via PostmarkApp API"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Postmark-Server-Token': self.server_token
        }

        payload = {
            'From': self.from_email,
            'To': to_email,
            'Subject': subject,
            'HtmlBody': html_body,
            'TextBody': text_body,
            'MessageStream': 'outbound'
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        logger.debug(f"Email sent successfully to {to_email}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to send email to {to_email}: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {e}")
            return False

    def _build_html_body(
        self,
        subject: str,
        message: str,
        service_name: str,
        error_details: Optional[dict],
        log_url: Optional[str]
    ) -> str:
        """Build HTML email body"""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .alert-box {{ background-color: #ff4444; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .info-box {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .details {{ background-color: #fff; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }}
                .btn {{ display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="alert-box">
                    <h2>CRITICAL ALERT</h2>
                    <h3>{subject}</h3>
                </div>

                <div class="info-box">
                    <p><strong>Service:</strong> {service_name}</p>
                    <p><strong>Time:</strong> {timestamp}</p>
                    <p><strong>Message:</strong> {message}</p>
                </div>
        """

        # Add error details if provided
        if error_details:
            html += '<div class="details"><h4>Error Details:</h4><ul>'
            for key, value in error_details.items():
                html += f'<li><strong>{key}:</strong> {value}</li>'
            html += '</ul></div>'

        # Add log URL if provided
        if log_url:
            html += f'<p><a href="{log_url}" class="btn">View Full Logs</a></p>'

        html += """
                <div class="footer">
                    <p>This is an automated alert from SmallCaps Scanner monitoring system.</p>
                    <p>Please investigate and resolve the issue as soon as possible.</p>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    def _build_text_body(
        self,
        subject: str,
        message: str,
        service_name: str,
        error_details: Optional[dict],
        log_url: Optional[str]
    ) -> str:
        """Build plain text email body"""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text = f"""
CRITICAL ALERT: {subject}

Service: {service_name}
Time: {timestamp}

Message:
{message}
"""

        if error_details:
            text += "\nError Details:\n"
            for key, value in error_details.items():
                text += f"- {key}: {value}\n"

        if log_url:
            text += f"\nView full logs: {log_url}\n"

        text += """
---
This is an automated alert from SmallCaps Scanner monitoring system.
Please investigate and resolve the issue as soon as possible.
"""

        return text

    async def send_alert_report(
        self,
        ticker: str,
        alert_type: str,
        alert_data: dict,
        reporter_notes: Optional[str] = None
    ) -> bool:
        """
        Send alert report to Trello + personal email

        Args:
            ticker: Ticker symbol
            alert_type: Type of alert (vwap_cross, nhod, etc)
            alert_data: Full alert data including debug_data
            reporter_notes: Optional notes from reporter

        Returns:
            True if at least one email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("EmailNotifier disabled - POSTMARK_SERVER_TOKEN not configured")
            return False

        try:
            # Send to both Trello and personal email
            recipients = [
                'jose_franco+qyx9ngcmlmthfexeq6ct@app.trello.com',  # Trello board
                'hola@jose.ar'  # Personal email
            ]

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
                    timestamp = alert_dt_et.strftime("%Y-%m-%d %H:%M:%S ET")
                except Exception as e:
                    logger.warning(f"Could not parse alert timestamp '{alert_timestamp_str}': {e}, using current time")
                    timestamp = datetime.now(pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S ET")
            else:
                # Fallback to current time in ET if no timestamp in alert_data
                timestamp = datetime.now(pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S ET")

            subject = f"[ALERT REPORT] {ticker} - {alert_type.upper()} - {timestamp}"

            # Build detailed HTML body
            html_body = f"""
            <html>
            <body style="font-family: monospace; font-size: 12px;">
                <h2 style="color: #ff4444;">Alert Report - {ticker}</h2>
                <p><strong>Alert Type:</strong> {alert_type}</p>
                <p><strong>Timestamp:</strong> {timestamp}</p>
                <p><strong>Price:</strong> ${alert_data.get('price', 'N/A')}</p>

                {f'<p><strong>Reporter Notes:</strong> {reporter_notes}</p>' if reporter_notes else ''}

                <h3>Alert Details:</h3>
                <pre style="background: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto;">
{self._format_dict_for_email(alert_data)}
                </pre>

                <p style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 11px;">
                    Reported via SmallCaps Scanner - Alert Report System<br>
                    Frontend: http://15.235.115.4<br>
                    Time: {timestamp}
                </p>
            </body>
            </html>
            """

            # Build text body
            text_body = f"""
Alert Report - {ticker}

Alert Type: {alert_type}
Timestamp: {timestamp}
Price: ${alert_data.get('price', 'N/A')}

{f'Reporter Notes: {reporter_notes}' if reporter_notes else ''}

Alert Details:
{self._format_dict_for_text(alert_data)}

---
Reported via SmallCaps Scanner - Alert Report System
Frontend: http://15.235.115.4
Time: {timestamp}
"""

            # Send to all recipients
            tasks = []
            for recipient in recipients:
                tasks.append(self._send_email(
                    to_email=recipient,
                    subject=subject,
                    html_body=html_body,
                    text_body=text_body
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check results
            success_count = sum(1 for r in results if r is True)

            if success_count > 0:
                logger.info(f"Alert report sent to {success_count}/{len(recipients)} recipients: {ticker} - {alert_type}")
                return True
            else:
                logger.error(f"Failed to send alert report to any recipient: {ticker}")
                return False

        except Exception as e:
            logger.error(f"Failed to send alert report: {e}")
            return False

    def _format_dict_for_email(self, data: dict, indent: int = 0) -> str:
        """Format dictionary for HTML email display"""
        import json
        try:
            return json.dumps(data, indent=2, default=str)
        except:
            return str(data)

    def _format_dict_for_text(self, data: dict, indent: int = 0) -> str:
        """Format dictionary for text email display"""
        import json
        try:
            return json.dumps(data, indent=2, default=str)
        except:
            return str(data)

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
        logs: Optional[list] = None,
        source: Optional[str] = None,
        ticker: Optional[str] = None,
        row_data: Optional[dict] = None,
        row_html: Optional[str] = None
    ) -> bool:
        """
        Send bug report email to Trello + personal email

        Args:
            bug_id: Bug report ID
            user_email: Email of user who reported the bug
            user_name: Name of user who reported the bug
            user_role: Role of user who reported the bug
            description: Bug description
            url: URL where bug occurred
            user_agent: User agent string
            screenshot_url: Optional screenshot URL
            logs: Optional list of log entries
            source: Source scanner/component (Market Scanner, Momentum Scanner, etc)
            ticker: Ticker symbol (if from a scanner grid)
            row_data: Row data from scanner grid
            row_html: HTML of the row

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("EmailNotifier disabled - POSTMARK_SERVER_TOKEN not configured")
            return False

        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S ET")
            subject = f"[BUG REPORT #{bug_id}] {ticker or 'General'} - {source or 'Unknown'}"

            # Format row data for display
            row_data_html = ""
            if row_data:
                row_data_html = "<ul>" + "".join([f"<li><strong>{k}:</strong> {v}</li>" for k, v in row_data.items()]) + "</ul>"

            # Format logs for display
            logs_html = ""
            if logs:
                log_lines = []
                for log in (logs or [])[:50]:
                    ts = log.get('timestamp', 'N/A')
                    level = log.get('level', 'INFO')
                    msg = log.get('message', 'N/A')
                    log_lines.append(f"[{ts}] {level}: {msg}")
                logs_html = "\n".join(log_lines)

            # Build HTML email
            html_body = f"""
            <html>
            <body style="font-family: monospace; font-size: 12px;">
                <h2 style="color: #ff4444;">🐛 BUG REPORT #{bug_id}</h2>

                <h3>Usuario que reportó:</h3>
                <ul>
                    <li><strong>Email:</strong> {user_email}</li>
                    <li><strong>Nombre:</strong> {user_name}</li>
                    <li><strong>Rol:</strong> {user_role}</li>
                </ul>

                <h3>Descripción del problema:</h3>
                <p style="background: #f5f5f5; padding: 10px; border-radius: 5px;">{description}</p>

                <h3>Origen del reporte:</h3>
                <ul>
                    <li><strong>Fuente:</strong> {source or 'Unknown'}</li>
                    <li><strong>Ticker:</strong> {ticker or 'N/A'}</li>
                    <li><strong>URL:</strong> {url}</li>
                    <li><strong>User Agent:</strong> {user_agent}</li>
                    <li><strong>Timestamp:</strong> {timestamp}</li>
                    {f'<li><strong>Screenshot:</strong> <a href="http://15.235.115.4{screenshot_url}">{screenshot_url}</a></li>' if screenshot_url else ''}
                </ul>

                {f'<h3>📊 Datos de la alerta (rowData):</h3>{row_data_html}' if row_data_html else ''}

                {f'<h3>🔍 HTML de la fila:</h3><pre style="background: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap;">{row_html[:2000]}</pre>' if row_html else ''}

                {f'<h3>Logs (últimos {len(logs or [])} eventos):</h3><pre style="background: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto;">{logs_html}</pre>' if logs else ''}

                <p style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 11px;">
                    Reported via SmallCaps Scanner - Bug Report System<br>
                    Frontend: <a href="http://15.235.115.4">http://15.235.115.4</a><br>
                    Time: {timestamp}
                </p>
            </body>
            </html>
            """

            # Build text email
            row_data_text = ""
            if row_data:
                row_data_text = "\n".join([f"  • {k}: {v}" for k, v in row_data.items()])

            # Extract text sections to avoid f-string backslash issues
            screenshot_text = f'• Screenshot: http://15.235.115.4{screenshot_url}' if screenshot_url else ''
            row_data_section = f'📊 Datos de la alerta (rowData):\n{row_data_text}\n' if row_data_text else ''
            row_html_section = f'🔍 HTML de la fila:\n---\n{row_html[:2000]}\n---\n' if row_html else ''
            logs_section = f'Logs (últimos {len(logs or [])} eventos):\n---\n{logs_html}\n---\n' if logs else ''

            text_body = f"""
🐛 BUG REPORT #{bug_id}

Usuario que reportó:
• Email: {user_email}
• Nombre: {user_name}
• Rol: {user_role}

Descripción del problema:
{description}

Origen del reporte:
• Fuente: {source or 'Unknown'}
• Ticker: {ticker or 'N/A'}
• URL: {url}
• User Agent: {user_agent}
• Timestamp: {timestamp}
{screenshot_text}

{row_data_section}

{row_html_section}

{logs_section}

---
Reported via SmallCaps Scanner - Bug Report System
Frontend: http://15.235.115.4
Time: {timestamp}
"""

            # Send to Trello + personal email
            recipients = [
                'jose_franco+qyx9ngcmlmthfexeq6ct@app.trello.com',  # Trello board
                'hola@jose.ar'  # Personal email
            ]

            tasks = []
            for recipient in recipients:
                tasks.append(self._send_email(
                    to_email=recipient,
                    subject=subject,
                    html_body=html_body,
                    text_body=text_body
                ))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)

            if success_count > 0:
                logger.info(f"Bug report #{bug_id} sent to {success_count}/{len(recipients)} recipients")
                return True
            else:
                logger.error(f"Failed to send bug report #{bug_id} to any recipient")
                return False

        except Exception as e:
            logger.error(f"Failed to send bug report email: {e}")
            return False


