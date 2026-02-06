"""
GitHub Issue Creator for bug reports
Creates issues automatically when users report bugs (excludes connection issues)
Uses GitHub REST API (no CLI dependency)
"""

import asyncio
import aiohttp
import logging
import os
import re
from typing import Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Patterns to EXCLUDE from issue creation (connection/transient issues)
EXCLUDE_PATTERNS = [
    r'no route to host',
    r'connection timed out',
    r'operation timed out',
    r'connection refused',
    r'network is unreachable',
    r'ssh.*timeout',
    r'ssh.*connection',
    r'temporary failure',
    r'name resolution',
    r'dns.*failed',
    r'socket.*error',
    r'econnrefused',
    r'econnreset',
    r'etimedout',
    r'ehostunreach',
    r'enetunreach',
    r'connection reset',
    r'broken pipe',
    r'502 bad gateway',
    r'503 service unavailable',
    r'504 gateway timeout',
]


class GitHubIssueCreator:
    """
    Creates GitHub issues for bug reports
    Filters out connection/network issues automatically
    Uses GitHub REST API
    """

    def __init__(self, repo: str = "jefrnc/codesmall", token: str = None):
        self.repo = repo
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.api_url = f"https://api.github.com/repos/{repo}/issues"
        self.exclude_regex = re.compile(
            '|'.join(EXCLUDE_PATTERNS),
            re.IGNORECASE
        )

        if not self.token:
            logger.warning("GitHubIssueCreator: No GITHUB_TOKEN configured - issue creation disabled")

    def _should_exclude(self, text: str) -> bool:
        """Check if the text contains connection-related issues"""
        if not text:
            return False
        return bool(self.exclude_regex.search(text))

    def _escape_markdown(self, text: str) -> str:
        """Escape special markdown characters"""
        if not text:
            return ""
        # Don't escape everything, just backticks in code
        return text.replace('`', '\\`')

    async def create_bug_issue(
        self,
        bug_id: int,
        description: str,
        user_email: str,
        user_name: str = None,
        source: str = None,
        ticker: str = None,
        url: str = None,
        row_data: dict = None,
        labels: List[str] = None
    ) -> Optional[str]:
        """
        Create a GitHub issue for a bug report

        Returns:
            Issue URL if created, None if excluded or failed
        """
        # Check if this is a connection issue - skip if so
        if self._should_exclude(description):
            logger.info(f"Skipping GitHub issue for bug #{bug_id} - connection-related issue")
            return None

        if row_data and self._should_exclude(str(row_data)):
            logger.info(f"Skipping GitHub issue for bug #{bug_id} - connection data in row_data")
            return None

        try:
            # Build issue title
            title_parts = [f"[BUG #{bug_id}]"]
            if ticker:
                title_parts.append(f"${ticker}")
            if source:
                title_parts.append(f"- {source}")

            # Truncate description for title
            desc_preview = description[:50].replace('\n', ' ')
            if len(description) > 50:
                desc_preview += "..."
            title_parts.append(f": {desc_preview}")

            title = " ".join(title_parts)

            # Build issue body
            body = f"""## Bug Report #{bug_id}

**Reportado por:** {user_name or 'Unknown'} ({user_email})
**Fecha:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}
"""

            if source:
                body += f"**Fuente:** {source}\n"
            if ticker:
                body += f"**Ticker:** ${ticker}\n"
            if url:
                body += f"**URL:** `{url}`\n"

            body += f"""
## Descripcion
{description}
"""

            if row_data:
                body += """
## Datos del Scanner
```json
"""
                # Add key fields only
                priority_fields = ['price', 'current_price', 'gap_percent', 'change_percent',
                                 'volume', 'prev_close', 'float_shares', 'session']
                filtered_data = {k: v for k, v in row_data.items() if k in priority_fields}

                import json
                body += json.dumps(filtered_data, indent=2, default=str)
                body += "\n```\n"

            body += """
---
*Issue creado automaticamente desde el sistema de bug reports*
"""

            # Default labels
            if labels is None:
                labels = ["bug", "user-reported"]

            if ticker:
                labels.append("scanner-data")

            # Create issue using GitHub API
            if not self.token:
                logger.warning("Cannot create GitHub issue - no token configured")
                return None

            headers = {
                "Authorization": f"token {self.token}",
                "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json"
            }

            payload = {
                "title": title,
                "body": body,
                "labels": labels
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status == 201:
                        data = await response.json()
                        issue_url = data.get("html_url")
                        logger.info(f"Created GitHub issue for bug #{bug_id}: {issue_url}")
                        return issue_url
                    else:
                        error = await response.text()
                        logger.error(f"Failed to create GitHub issue: {response.status} - {error}")
                        return None

        except Exception as e:
            logger.error(f"Error creating GitHub issue for bug #{bug_id}: {e}")
            return None

    async def create_system_issue(
        self,
        title: str,
        description: str,
        service_name: str,
        error_details: dict = None,
        labels: List[str] = None
    ) -> Optional[str]:
        """
        Create a GitHub issue for system errors (NOT connection issues)

        Returns:
            Issue URL if created, None if excluded or failed
        """
        # Check if this is a connection issue
        if self._should_exclude(title) or self._should_exclude(description):
            logger.info(f"Skipping GitHub issue - connection-related: {title[:50]}")
            return None

        if error_details and self._should_exclude(str(error_details)):
            logger.info(f"Skipping GitHub issue - connection data in error_details")
            return None

        try:
            full_title = f"[SYSTEM] {service_name}: {title[:80]}"

            body = f"""## System Error

**Service:** {service_name}
**Time:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}

## Description
{description}
"""

            if error_details:
                body += """
## Error Details
```
"""
                for key, value in error_details.items():
                    body += f"{key}: {value}\n"
                body += "```\n"

            body += """
---
*Issue creado automaticamente por el sistema de monitoreo*
"""

            if labels is None:
                labels = ["bug", "system-error"]

            # Create issue using GitHub API
            if not self.token:
                logger.warning("Cannot create GitHub issue - no token configured")
                return None

            headers = {
                "Authorization": f"token {self.token}",
                "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json"
            }

            payload = {
                "title": full_title,
                "body": body,
                "labels": labels
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status == 201:
                        data = await response.json()
                        issue_url = data.get("html_url")
                        logger.info(f"Created GitHub system issue: {issue_url}")
                        return issue_url
                    else:
                        error = await response.text()
                        logger.error(f"Failed to create GitHub issue: {response.status} - {error}")
                        return None

        except Exception as e:
            logger.error(f"Error creating GitHub system issue: {e}")
            return None
