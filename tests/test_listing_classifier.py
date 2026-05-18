"""Pure-function tests for listing_classifier."""

from datetime import date

import pytest

from smallcaps_shared.listing_classifier import (
    ListingType,
    classify_from_polygon_details,
    classify_from_row,
)


TODAY = date(2026, 5, 18)


def test_traditional_ipo_from_polygon():
    # LABT — listed 2026-04-23, type CS. The exact case reported by the user.
    info = classify_from_polygon_details(
        {
            "ticker": "LABT",
            "name": "Lakewood-Amedex Biotherapeutics Inc. Common Stock",
            "type": "CS",
            "list_date": "2026-04-23",
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.IPO
    assert info.is_traditional_ipo is True
    assert info.is_spac is False
    assert info.is_derivative is False
    assert info.is_recent_listing is True
    assert info.days_since_listing == 25


def test_spac_unit():
    info = classify_from_row(
        {
            "ticker": "IDACU",
            "company_name": "Iron Dome Acquisition I Corp. Units",
            "listed_date": date(2026, 5, 16),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.SPAC_UNIT
    assert info.is_spac is True
    assert info.is_derivative is True
    assert info.is_traditional_ipo is False


def test_spac_warrant():
    info = classify_from_row(
        {
            "ticker": "MYXXW",
            "company_name": "Maywood Acquisition Corp. 2 Warrants",
            "listed_date": date(2026, 5, 16),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.SPAC_WARRANT
    assert info.is_spac is True
    assert info.is_derivative is True


def test_spac_right():
    info = classify_from_row(
        {
            "ticker": "MYXXR",
            "company_name": "Maywood Acquisition Corp. 2 Rights",
            "listed_date": date(2026, 5, 16),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.SPAC_RIGHT
    assert info.is_spac is True
    assert info.is_derivative is True


def test_spac_common_via_name():
    info = classify_from_row(
        {
            "ticker": "MYX",
            "company_name": "Maywood Acquisition Corp. 2 Class A Ordinary Share",
            "ticker_type": "CS",
            "listed_date": date(2026, 5, 16),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.SPAC_COMMON
    assert info.is_spac is True
    assert info.is_traditional_ipo is False  # SPAC IPO is NOT a traditional IPO
    assert info.classification_source == "name:acquisition"


def test_spac_common_via_sic_6770():
    info = classify_from_row(
        {
            "ticker": "TDWD",
            "company_name": "Tailwind 2.0",  # name doesn't say "Acquisition"
            "ticker_type": "CS",
            "sic_code": "6770",
            "listed_date": date(2025, 12, 9),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.SPAC_COMMON
    assert info.classification_source == "sic:6770"


def test_adr():
    info = classify_from_row(
        {
            "ticker": "BABA",
            "company_name": "Alibaba Group",
            "ticker_type": "ADRC",
            "listed_date": date(2014, 9, 19),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.ADR
    assert info.is_traditional_ipo is False


def test_seasoned_common_not_recent():
    info = classify_from_row(
        {
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "ticker_type": "CS",
            "listed_date": date(1980, 12, 12),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.COMMON
    assert info.is_recent_listing is False
    assert info.is_traditional_ipo is False


def test_warrant_not_spac():
    info = classify_from_row(
        {
            "ticker": "EXYNW",
            "company_name": "Exyn Technologies, Inc. Warrant",
            "listed_date": date(2026, 5, 16),
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.WARRANT
    assert info.is_spac is False
    assert info.is_derivative is True


def test_empty_row_falls_back_to_unknown():
    info = classify_from_row(
        {
            "ticker": "LABT",
            "company_name": "Lakewood-Amedex Biotherapeutics Inc. Common Stock",
        },
        today=TODAY,
    )
    assert info.listing_type == ListingType.UNKNOWN
    assert info.classification_source == "fallback:unknown"


def test_listed_date_preferred_over_listing_date():
    info = classify_from_row(
        {
            "ticker": "FOO",
            "company_name": "Foo Inc.",
            "ticker_type": "CS",
            "listed_date": date(2026, 5, 1),
            "listing_date": date(2020, 1, 1),  # should be ignored
        },
        today=TODAY,
    )
    assert info.listing_date == date(2026, 5, 1)
    assert info.days_since_listing == 17


def test_listing_date_fallback_when_listed_date_missing():
    info = classify_from_row(
        {
            "ticker": "FOO",
            "company_name": "Foo Inc.",
            "ticker_type": "CS",
            "listing_date": date(2026, 5, 1),
        },
        today=TODAY,
    )
    assert info.listing_date == date(2026, 5, 1)


def test_string_date_parsed():
    info = classify_from_row(
        {
            "ticker": "FOO",
            "company_name": "Foo Inc.",
            "ticker_type": "CS",
            "listed_date": "2026-05-01",
        },
        today=TODAY,
    )
    assert info.listing_date == date(2026, 5, 1)


def test_threshold_boundary():
    # Exactly threshold_days old → still recent
    info = classify_from_row(
        {
            "ticker": "FOO",
            "company_name": "Foo Inc.",
            "ticker_type": "CS",
            "listed_date": date(2026, 2, 17),  # 90 days before 2026-05-18
        },
        today=TODAY,
        recent_threshold_days=90,
    )
    assert info.days_since_listing == 90
    assert info.is_recent_listing is True

    # One day past threshold → not recent
    info2 = classify_from_row(
        {
            "ticker": "FOO",
            "company_name": "Foo Inc.",
            "ticker_type": "CS",
            "listed_date": date(2026, 2, 16),
        },
        today=TODAY,
        recent_threshold_days=90,
    )
    assert info2.is_recent_listing is False


def test_as_dict_serialization():
    info = classify_from_row(
        {
            "ticker": "LABT",
            "company_name": "Lakewood-Amedex Biotherapeutics Inc. Common Stock",
            "ticker_type": "CS",
            "listed_date": date(2026, 4, 23),
        },
        today=TODAY,
    )
    d = info.as_dict()
    assert d["ticker"] == "LABT"
    assert d["listing_type"] == "IPO"
    assert d["listing_date"] == "2026-04-23"
    assert d["is_traditional_ipo"] is True


def test_classify_listing_polygon_fallback_for_empty_db_row():
    """Async path: empty DB row should fall back to Polygon fetcher."""
    import asyncio
    from smallcaps_shared.listing_classifier import classify_listing

    class FakePool:
        async def fetchrow(self, query, ticker):
            return {
                "ticker": ticker,
                "company_name": "Lakewood-Amedex Biotherapeutics Inc. Common Stock",
                "ticker_type": None,
                "sic_code": None,
                "listed_date": None,
                "listing_date": None,
            }

    async def fake_polygon(ticker):
        return {
            "ticker": ticker,
            "name": "Lakewood-Amedex Biotherapeutics Inc. Common Stock",
            "type": "CS",
            "list_date": "2026-04-23",
        }

    info = asyncio.run(
        classify_listing(
            FakePool(), "LABT", polygon_fetcher=fake_polygon, today=TODAY
        )
    )
    assert info.listing_type == ListingType.IPO
    assert info.is_traditional_ipo is True
    assert info.days_since_listing == 25
    assert info.classification_source == "ticker_type:CS"


def test_classify_listing_no_polygon_fetcher_returns_unknown_for_empty_row():
    import asyncio
    from smallcaps_shared.listing_classifier import classify_listing

    class FakePool:
        async def fetchrow(self, query, ticker):
            return {
                "ticker": ticker,
                "company_name": "Foo",
                "ticker_type": None,
                "sic_code": None,
                "listed_date": None,
                "listing_date": None,
            }

    info = asyncio.run(classify_listing(FakePool(), "LABT", today=TODAY))
    assert info.listing_type == ListingType.UNKNOWN
    assert info.classification_source == "row_empty"


def test_classify_listing_missing_ticker_returns_not_in_db():
    import asyncio
    from smallcaps_shared.listing_classifier import classify_listing

    class FakePool:
        async def fetchrow(self, query, ticker):
            return None

    info = asyncio.run(classify_listing(FakePool(), "ZZZZ", today=TODAY))
    assert info.listing_type == ListingType.UNKNOWN
    assert info.classification_source == "not_in_db"
