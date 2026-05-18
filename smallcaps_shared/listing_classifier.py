"""
Listing classification for tickers.

Distinguishes traditional IPO vs SPAC vs derivative listings (units, warrants,
rights) vs ADR vs already-seasoned common stock, using data already stored in
the `companies` table.

Signal priority (highest to lowest):
    1. company_name contains "Units"/"Unit"         -> SPAC_UNIT
    2. company_name contains "Warrant(s)"           -> WARRANT / SPAC_WARRANT
    3. company_name contains "Rights"/"Right"       -> RIGHT / SPAC_RIGHT
    4. company_name matches SPAC patterns
       ("Acquisition Corp", "Blank Check",
        "Merger Corp")                              -> SPAC_COMMON
    5. sic_code == '6770' (Blank Checks)            -> SPAC_COMMON
    6. ticker_type == 'ADRC'                        -> ADR
    7. ticker_type == 'SP'                          -> STRUCTURED
    8. ticker_type == 'CS' and recently listed      -> IPO
       otherwise                                    -> COMMON

Caveat — de-SPAC: when a SPAC completes its business combination the
listed_date stays as the original shell IPO date and the company_name changes
to the operating company. We do not auto-detect that here. Callers that need
true "new public company" status post-de-SPAC need ticker/name change history,
which is not currently tracked.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from datetime import date, datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Optional


class ListingType(str, Enum):
    IPO = "IPO"
    COMMON = "COMMON"
    SPAC_COMMON = "SPAC_COMMON"
    SPAC_UNIT = "SPAC_UNIT"
    SPAC_WARRANT = "SPAC_WARRANT"
    SPAC_RIGHT = "SPAC_RIGHT"
    WARRANT = "WARRANT"
    RIGHT = "RIGHT"
    ADR = "ADR"
    STRUCTURED = "STRUCTURED"
    UNKNOWN = "UNKNOWN"


_SPAC_NAME_RE = re.compile(
    r"\b(acquisition\s+(corp|corporation)|blank\s+check|merger\s+corp)\b",
    re.IGNORECASE,
)
_UNIT_RE = re.compile(r"\bunits?\b", re.IGNORECASE)
_WARRANT_RE = re.compile(r"\bwarrants?\b", re.IGNORECASE)
_RIGHT_RE = re.compile(r"\brights?\b", re.IGNORECASE)
_SPAC_SIC_CODES = {"6770"}


@dataclass(frozen=True)
class TickerListingInfo:
    ticker: str
    listing_date: Optional[date]
    days_since_listing: Optional[int]
    listing_type: ListingType
    is_spac: bool
    is_derivative: bool
    is_recent_listing: bool
    is_traditional_ipo: bool
    classification_source: str

    def as_dict(self) -> dict:
        d = asdict(self)
        d["listing_type"] = self.listing_type.value
        if self.listing_date is not None:
            d["listing_date"] = self.listing_date.isoformat()
        return d


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def classify_from_row(
    row: dict,
    *,
    recent_threshold_days: int = 90,
    today: Optional[date] = None,
) -> TickerListingInfo:
    """Classify a ticker from an in-memory `companies` row.

    Accepts asyncpg.Record-style mappings or plain dicts. Missing fields
    default to None.
    """
    ticker = (row.get("ticker") or "").upper()
    name = row.get("company_name") or ""
    ticker_type = (row.get("ticker_type") or "").upper().strip()
    sic_code = (row.get("sic_code") or "").strip()

    listing_date = _coerce_date(row.get("listed_date")) or _coerce_date(
        row.get("listing_date")
    )

    days_since: Optional[int] = None
    if listing_date is not None:
        today = today or date.today()
        days_since = (today - listing_date).days

    is_recent = days_since is not None and 0 <= days_since <= recent_threshold_days

    name_is_spac = bool(_SPAC_NAME_RE.search(name))
    is_unit = bool(_UNIT_RE.search(name))
    is_warrant = bool(_WARRANT_RE.search(name))
    is_right = bool(_RIGHT_RE.search(name))
    sic_is_spac = sic_code in _SPAC_SIC_CODES

    listing_type = ListingType.UNKNOWN
    source = "default"

    if is_unit:
        # SPAC IPOs almost always begin life as units, so we treat any "Unit"
        # naming as SPAC unit even if "Acquisition" is absent.
        listing_type = ListingType.SPAC_UNIT
        source = "name:unit"
    elif is_warrant:
        listing_type = (
            ListingType.SPAC_WARRANT if name_is_spac else ListingType.WARRANT
        )
        source = "name:warrant"
    elif is_right:
        listing_type = ListingType.SPAC_RIGHT if name_is_spac else ListingType.RIGHT
        source = "name:right"
    elif name_is_spac:
        listing_type = ListingType.SPAC_COMMON
        source = "name:acquisition"
    elif sic_is_spac:
        listing_type = ListingType.SPAC_COMMON
        source = "sic:6770"
    elif ticker_type == "ADRC":
        listing_type = ListingType.ADR
        source = "ticker_type:ADRC"
    elif ticker_type == "SP":
        listing_type = ListingType.STRUCTURED
        source = "ticker_type:SP"
    elif ticker_type == "CS":
        listing_type = ListingType.IPO if is_recent else ListingType.COMMON
        source = "ticker_type:CS"
    else:
        # No ticker_type and no name signal — fall back to recency only.
        listing_type = ListingType.IPO if is_recent else ListingType.UNKNOWN
        source = "fallback:recency" if is_recent else "fallback:unknown"

    is_spac = listing_type in (
        ListingType.SPAC_COMMON,
        ListingType.SPAC_UNIT,
        ListingType.SPAC_WARRANT,
        ListingType.SPAC_RIGHT,
    )
    is_derivative = listing_type in (
        ListingType.SPAC_UNIT,
        ListingType.SPAC_WARRANT,
        ListingType.SPAC_RIGHT,
        ListingType.WARRANT,
        ListingType.RIGHT,
    )
    is_traditional_ipo = listing_type == ListingType.IPO

    return TickerListingInfo(
        ticker=ticker,
        listing_date=listing_date,
        days_since_listing=days_since,
        listing_type=listing_type,
        is_spac=is_spac,
        is_derivative=is_derivative,
        is_recent_listing=is_recent,
        is_traditional_ipo=is_traditional_ipo,
        classification_source=source,
    )


_COMPANIES_SELECT = """
    SELECT ticker, company_name, ticker_type, sic_code,
           listed_date, listing_date
    FROM companies
    WHERE ticker = $1
"""


def classify_from_polygon_details(
    details: dict,
    *,
    recent_threshold_days: int = 90,
    today: Optional[date] = None,
) -> TickerListingInfo:
    """Classify from a Polygon `/v3/reference/tickers/{ticker}` results dict.

    Maps Polygon's `type` -> our `ticker_type`, `name` -> `company_name`, and
    `list_date` -> `listed_date`.
    """
    row = {
        "ticker": details.get("ticker"),
        "company_name": details.get("name"),
        "ticker_type": details.get("type"),
        "sic_code": details.get("sic_code"),
        "listed_date": details.get("list_date"),
        "listing_date": None,
    }
    return classify_from_row(
        row, recent_threshold_days=recent_threshold_days, today=today
    )


def _row_is_empty(row: dict) -> bool:
    """A companies row exists but has none of the signals we care about."""
    return not (
        row.get("listed_date")
        or row.get("listing_date")
        or row.get("ticker_type")
        or row.get("sic_code")
    )


async def classify_listing(
    pool,
    ticker: str,
    *,
    recent_threshold_days: int = 90,
    today: Optional[date] = None,
    polygon_fetcher: Optional[Callable[[str], Awaitable[Optional[dict]]]] = None,
) -> TickerListingInfo:
    """Async DB-backed classifier with optional Polygon fallback.

    Queries the `companies` table first. If the row is missing or has no
    classification signals (common for tickers that listed today and the
    metadata-updater hasn't backfilled yet), and a `polygon_fetcher` is
    supplied, falls back to Polygon `/v3/reference/tickers/{ticker}`.

    `polygon_fetcher` must be an async callable returning the Polygon
    `results` dict (or None on failure). The module deliberately stays
    HTTP-client agnostic so callers can plug in `httpx`, `aiohttp`, or a
    cached service.
    """
    ticker = ticker.upper()
    row = await pool.fetchrow(_COMPANIES_SELECT, ticker)

    if row is not None:
        row_dict = dict(row)
        if not _row_is_empty(row_dict):
            return classify_from_row(
                row_dict,
                recent_threshold_days=recent_threshold_days,
                today=today,
            )

    if polygon_fetcher is not None:
        try:
            details = await polygon_fetcher(ticker)
        except Exception:
            details = None
        if details:
            info = classify_from_polygon_details(
                details,
                recent_threshold_days=recent_threshold_days,
                today=today,
            )
            # Preserve the original ticker casing
            return info

    return TickerListingInfo(
        ticker=ticker,
        listing_date=None,
        days_since_listing=None,
        listing_type=ListingType.UNKNOWN,
        is_spac=False,
        is_derivative=False,
        is_recent_listing=False,
        is_traditional_ipo=False,
        classification_source="not_in_db" if row is None else "row_empty",
    )
