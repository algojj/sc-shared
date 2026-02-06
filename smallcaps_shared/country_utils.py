"""
Country Detection Utilities
Shared logic for detecting company country of origin from Polygon API data
"""
import logging
from typing import List
import asyncio

logger = logging.getLogger(__name__)


async def enrich_with_country_codes(polygon_client, results: List):
    """
    Enrich scan results with country codes from Polygon ticker details

    Args:
        polygon_client: PolygonBaseClient instance
        results: List of ScanResult objects to enrich

    This fetches ticker details in parallel (much faster than sequential)
    and updates each result's country_code field based on business address
    """
    if not results:
        return

    logger.info(f"[COUNTRY] Fetching country data for {len(results)} tickers...")

    async def fetch_country(result):
        try:
            details = await polygon_client.get_ticker_details(result.ticker)
            if not details:
                return

            # Strategy 1: Business address
            address = details.get('address', {})
            if address:
                state_or_country = (
                    address.get('state', '') or
                    address.get('state_or_country', '') or
                    ''
                )
                city = address.get('city', '').lower()

                country_code = detect_country_from_address(city, state_or_country)
                if country_code != 'US':
                    result.country_code = country_code
                    logger.debug(f"[COUNTRY] {result.ticker}: country={country_code} (from address: {city}, {state_or_country})")
                    return

            # Strategy 2: Homepage URL domain TLD
            homepage_url = details.get('homepage_url', '')
            if homepage_url:
                url_country = detect_country_from_url(homepage_url)
                if url_country:
                    result.country_code = url_country
                    logger.debug(f"[COUNTRY] {result.ticker}: country={url_country} (from URL: {homepage_url})")
                    return

            # Strategy 3: Description text
            description = details.get('description', '')
            if description:
                desc_country = detect_country_from_description(description)
                if desc_country:
                    result.country_code = desc_country
                    logger.debug(f"[COUNTRY] {result.ticker}: country={desc_country} (from description)")
                    return

            # Default: US (address-based detection returned US, or no data at all)
            if address:
                state_or_country = (
                    address.get('state', '') or
                    address.get('state_or_country', '') or
                    ''
                )
                city = address.get('city', '').lower()
                result.country_code = detect_country_from_address(city, state_or_country)
            else:
                result.country_code = 'US'

        except Exception as e:
            logger.warning(f"[COUNTRY] Error fetching country for {result.ticker}: {e}")

    # Fetch all in parallel (much faster than sequential)
    await asyncio.gather(*[fetch_country(r) for r in results], return_exceptions=True)

    # Log results
    country_counts = {}
    for r in results:
        code = r.country_code or 'US'
        country_counts[code] = country_counts.get(code, 0) + 1

    logger.info(f"[COUNTRY] Country distribution: {country_counts}")


def detect_country_from_address(city: str, state_or_country: str) -> str:
    """
    Detect country code from city and state_or_country strings

    Args:
        city: City name (lowercase)
        state_or_country: State or country string from Polygon

    Returns:
        2-letter ISO country code (default: 'US')
    """
    state_or_country_lower = state_or_country.lower()

    # Check for international locations
    # China
    if 'china' in state_or_country_lower or city in ['beijing', 'shanghai', 'shenzhen', 'hangzhou', 'guangzhou', 'wuhan', 'chongqing']:
        return 'CN'

    # Singapore
    if 'singapore' in state_or_country_lower or city == 'singapore':
        return 'SG'

    # Canada
    if 'canada' in state_or_country_lower or city in ['toronto', 'vancouver', 'montreal', 'calgary', 'ottawa']:
        return 'CA'

    # Israel
    if 'israel' in state_or_country_lower or city in ['tel aviv', 'jerusalem', 'haifa', 'isfiya']:
        return 'IL'

    # Brazil
    if 'brazil' in state_or_country_lower or 'brasil' in state_or_country_lower or city in ['sao paulo', 'rio de janeiro', 'brasilia']:
        return 'BR'

    # India
    if 'india' in state_or_country_lower or city in ['mumbai', 'bangalore', 'delhi', 'hyderabad', 'chennai']:
        return 'IN'

    # South Korea
    if 'korea' in state_or_country_lower or city in ['seoul', 'busan', 'incheon']:
        return 'KR'

    # Japan
    if 'japan' in state_or_country_lower or city in ['tokyo', 'osaka', 'kyoto', 'yokohama']:
        return 'JP'

    # Mexico
    if 'mexico' in state_or_country_lower or city in ['mexico city', 'guadalajara', 'monterrey']:
        return 'MX'

    # Argentina
    if 'argentina' in state_or_country_lower or city == 'buenos aires':
        return 'AR'

    # Australia
    if 'australia' in state_or_country_lower or city in ['sydney', 'melbourne', 'brisbane']:
        return 'AU'

    # United Kingdom
    if 'united kingdom' in state_or_country_lower or 'u.k.' in state_or_country_lower or city in ['london', 'manchester', 'birmingham']:
        return 'GB'

    # Germany
    if 'germany' in state_or_country_lower or city in ['berlin', 'munich', 'frankfurt']:
        return 'DE'

    # France
    if 'france' in state_or_country_lower or city == 'paris':
        return 'FR'

    # Netherlands
    if 'netherlands' in state_or_country_lower or city == 'amsterdam':
        return 'NL'

    # Switzerland
    if 'switzerland' in state_or_country_lower or city in ['zurich', 'geneva']:
        return 'CH'

    # Default to US
    return 'US'


def detect_country_from_url(homepage_url: str) -> str:
    """
    Detect country from homepage URL domain TLD.

    Args:
        homepage_url: Company homepage URL

    Returns:
        2-letter ISO country code, or '' if not detected
    """
    if not homepage_url:
        return ''

    homepage_url = homepage_url.lower().rstrip('/')

    # Extract domain from URL
    try:
        # Remove protocol
        domain = homepage_url.split('://')[-1].split('/')[0]
        # Get TLD (last part after dot)
        tld = domain.split('.')[-1]
    except Exception:
        return ''

    TLD_MAP = {
        'sg': 'SG',  # Singapore
        'cn': 'CN',  # China
        'hk': 'CN',  # Hong Kong → treat as China for trading
        'il': 'IL',  # Israel
        'ca': 'CA',  # Canada
        'br': 'BR',  # Brazil
        'in': 'IN',  # India
        'kr': 'KR',  # South Korea
        'jp': 'JP',  # Japan
        'mx': 'MX',  # Mexico
        'ar': 'AR',  # Argentina
        'au': 'AU',  # Australia
        'uk': 'GB',  # United Kingdom
        'de': 'DE',  # Germany
        'fr': 'FR',  # France
        'nl': 'NL',  # Netherlands
        'ch': 'CH',  # Switzerland
        'se': 'SE',  # Sweden
        'no': 'NO',  # Norway
        'dk': 'DK',  # Denmark
        'fi': 'FI',  # Finland
        'ie': 'IE',  # Ireland
        'nz': 'NZ',  # New Zealand
        'tw': 'TW',  # Taiwan
        'my': 'MY',  # Malaysia
        'th': 'TH',  # Thailand
    }

    return TLD_MAP.get(tld, '')


def detect_country_from_description(description: str) -> str:
    """
    Detect country from company description text.

    Args:
        description: Company description from Polygon

    Returns:
        2-letter ISO country code, or '' if not detected
    """
    if not description:
        return ''

    desc_lower = description.lower()

    # Check for explicit country mentions — order matters (most specific first)
    COUNTRY_PATTERNS = [
        ('headquartered in singapore', 'SG'),
        ('based in singapore', 'SG'),
        ('operates in singapore', 'SG'),
        ('incorporated in singapore', 'SG'),
        ('singapore-based', 'SG'),
        ('headquartered in israel', 'IL'),
        ('based in israel', 'IL'),
        ('israel-based', 'IL'),
        ('headquartered in china', 'CN'),
        ('based in china', 'CN'),
        ('china-based', 'CN'),
        ('headquartered in canada', 'CA'),
        ('based in canada', 'CA'),
        ('canadian company', 'CA'),
        ('headquartered in the united kingdom', 'GB'),
        ('based in the united kingdom', 'GB'),
        ('based in london', 'GB'),
        ('uk-based', 'GB'),
        ('headquartered in australia', 'AU'),
        ('based in australia', 'AU'),
        ('headquartered in brazil', 'BR'),
        ('based in brazil', 'BR'),
        ('headquartered in india', 'IN'),
        ('based in india', 'IN'),
        ('india-based', 'IN'),
        ('headquartered in japan', 'JP'),
        ('based in japan', 'JP'),
        ('headquartered in south korea', 'KR'),
        ('based in south korea', 'KR'),
        ('headquartered in germany', 'DE'),
        ('based in germany', 'DE'),
        ('headquartered in france', 'FR'),
        ('based in france', 'FR'),
        ('headquartered in switzerland', 'CH'),
        ('based in switzerland', 'CH'),
        ('headquartered in hong kong', 'CN'),
        ('based in hong kong', 'CN'),
        ('headquartered in taiwan', 'TW'),
        ('based in taiwan', 'TW'),
        ('headquartered in malaysia', 'MY'),
        ('based in malaysia', 'MY'),
    ]

    for pattern, code in COUNTRY_PATTERNS:
        if pattern in desc_lower:
            return code

    return ''


def get_flag_emoji(country_code: str) -> str:
    """
    Convert country code to flag emoji

    Args:
        country_code: 2-letter ISO country code

    Returns:
        Flag emoji string
    """
    FLAG_MAP = {
        'US': '🇺🇸',
        'CN': '🇨🇳',
        'CA': '🇨🇦',
        'GB': '🇬🇧',
        'IL': '🇮🇱',
        'BR': '🇧🇷',
        'IN': '🇮🇳',
        'SG': '🇸🇬',
        'KR': '🇰🇷',
        'JP': '🇯🇵',
        'MX': '🇲🇽',
        'AR': '🇦🇷',
        'AU': '🇦🇺',
        'DE': '🇩🇪',
        'FR': '🇫🇷',
        'NL': '🇳🇱',
        'CH': '🇨🇭',
    }

    return FLAG_MAP.get(country_code, '🇺🇸')  # Default to US flag
