"""
Country detection utility for stocks
Automatically detects company origin country from various sources
"""
import asyncio
import logging

logger = logging.getLogger(__name__)

async def get_country_code_async(details: dict) -> str:
    """
    Get country code from company details (async version with SEC API support)
    Returns 2-letter ISO country code
    """

    # Try SEC API first if CIK is available
    cik = details.get('cik')
    if cik:
        try:
            # Try to import SEC client if available
            try:
                from ..due_diligence_service.legacy_modules.sec_api import sec_client
                sec_info = await sec_client.get_company_info(str(cik))
                if sec_info:
                    country_from_sec = sec_client.get_country_code_from_sec(sec_info)
                    if country_from_sec != 'US':  # Only override if we have specific non-US info
                        return country_from_sec
            except ImportError:
                # SEC API not available, continue with fallback
                logger.debug("SEC API not available, using fallback country detection")
                pass
        except Exception as e:
            logger.warning(f"Error fetching SEC data: {e}")

    # Fall back to original detection logic
    return get_country_code_sync(details)

def get_country_code_sync(details: dict) -> str:
    """
    Synchronous country code detection (fallback method)
    Returns 2-letter ISO country code
    """
    
    # Default to US
    country_code = 'US'
    
    if not details:
        return country_code
    
    # Check for specific ticker mappings first (highest priority)
    ticker = details.get('ticker', '')
    if ticker:
        ticker_country_map = {
            # Japan
            'MRM': 'JP',    # MEDIROM Healthcare Technologies
            'SONY': 'JP',
            'TM': 'JP',     # Toyota
            'HMC': 'JP',    # Honda

            # Israel
            'CHEK': 'IL',
            'WLDS': 'IL',
            'RDHL': 'IL',
            'MGIH': 'IL',
            'CHKP': 'IL',
            'CYBR': 'IL',
            'FVRR': 'IL',
            'MNDY': 'IL',
            'WIX': 'IL',
            'NICE': 'IL',
            'TEVA': 'IL',
            'RSKD': 'IL',
            # UK/Great Britain
            'ADAP': 'GB',
            'BSQR': 'GB',
            # Canada
            'HIVE': 'CA',
            'BITF': 'CA',
            'GRO': 'CA',    # Agnico Eagle Mines - Canadian
            'PFAI': 'CA',   # Pineapple Financial - Canadian
            # Australia
            'COOT': 'AU',   # Australian Oilseeds Holdings
            # China
            'LI': 'CN',
            'BABA': 'CN',
            'JD': 'CN',
            'PDD': 'CN',
            'BILI': 'CN',
            'NIO': 'CN',
            'XPEV': 'CN',
            'NTES': 'CN',
            'BIDU': 'CN',
            'SINA': 'CN',
            'WB': 'CN',
            'MOMO': 'CN',
            'YY': 'CN',
            'BZUN': 'CN',
            'VIPS': 'CN',
            'LU': 'CN',
            'EDU': 'CN',
            'TAL': 'CN',
            'BTBT': 'CN',   # Bit Digital — Chinese-origin bitcoin miner
            'BIYA': 'CN',   # Baiya International — Chinese company
            'FLYE': 'CN',   # Fly-E Group — Chinese-origin e-bike company
            # Korea
            'CPNG': 'KR',   # Coupang — South Korean e-commerce (US office in Seattle)
            'KEP': 'KR',    # Korea Electric Power
            # India
            'INFY': 'IN',   # Infosys
            # Finland
            'NOK': 'FI',    # Nokia
            # Sweden
            'ERIC': 'SE',   # Ericsson
            # Ireland
            'ACNB': 'IE',   # Accenture
            # UAE
            'SWVL': 'AE',   # Swvl Holdings
        }

        if ticker.upper() in ticker_country_map:
            return ticker_country_map[ticker.upper()]
    
    # Primary source: company address
    # Try branding/business address first (where company operates), then fall back to registered address
    address = None

    # Priority 1: branding.logo_url often indicates business location
    if details.get('branding'):
        branding = details.get('branding', {})
        # If branding exists, try to get address

    # Priority 2: address field (could be business or HQ address)
    if details.get('address'):
        address = details.get('address', {})

    # Some Polygon responses have list_date with address embedded
    # Check if there's a primary_listing or similar field

    if address:
        country = address.get('country', '').strip()

        # Common country name mappings (defined before if country: so city/state checks can use it)
        country_mapping = {
                # North America
                'UNITED STATES': 'US', 'USA': 'US', 'UNITED STATES OF AMERICA': 'US',
                'CANADA': 'CA',
                'MEXICO': 'MX',
                
                # Asia
                'CHINA': 'CN', 'PEOPLES REPUBLIC OF CHINA': 'CN', 'PRC': 'CN',
                'HONG KONG': 'HK', 'HONGKONG': 'HK',
                'TAIWAN': 'TW', 'CHINESE TAIPEI': 'TW',
                'JAPAN': 'JP',
                'SOUTH KOREA': 'KR', 'KOREA': 'KR', 'REPUBLIC OF KOREA': 'KR',
                'INDIA': 'IN',
                'SINGAPORE': 'SG',
                'MALAYSIA': 'MY',
                'THAILAND': 'TH',
                'INDONESIA': 'ID',
                'PHILIPPINES': 'PH',
                'VIETNAM': 'VN',
                
                # Europe
                'UNITED KINGDOM': 'GB', 'UK': 'GB', 'GREAT BRITAIN': 'GB', 'ENGLAND': 'GB',
                'GERMANY': 'DE', 'DEUTSCHLAND': 'DE',
                'FRANCE': 'FR',
                'ITALY': 'IT', 'ITALIA': 'IT',
                'SPAIN': 'ES', 'ESPAÑA': 'ES',
                'NETHERLANDS': 'NL', 'HOLLAND': 'NL',
                'SWITZERLAND': 'CH', 'SWISS': 'CH',
                'SWEDEN': 'SE',
                'NORWAY': 'NO',
                'DENMARK': 'DK',
                'FINLAND': 'FI',
                'BELGIUM': 'BE',
                'AUSTRIA': 'AT',
                'IRELAND': 'IE',
                'LUXEMBOURG': 'LU',
                'PORTUGAL': 'PT',
                'GREECE': 'GR',
                'POLAND': 'PL',
                'CZECH REPUBLIC': 'CZ', 'CZECHIA': 'CZ',
                'HUNGARY': 'HU',
                'ROMANIA': 'RO',
                
                # Middle East
                'ISRAEL': 'IL',
                'SAUDI ARABIA': 'SA',
                'UNITED ARAB EMIRATES': 'AE', 'UAE': 'AE',
                'QATAR': 'QA',
                'TURKEY': 'TR',
                
                # UK Cities/Counties (common ones)
                'LONDON': 'GB', 'MANCHESTER': 'GB', 'BIRMINGHAM': 'GB',
                'LEEDS': 'GB', 'LIVERPOOL': 'GB', 'SHEFFIELD': 'GB',
                'BRISTOL': 'GB', 'CARDIFF': 'GB', 'EDINBURGH': 'GB',
                'GLASGOW': 'GB', 'BELFAST': 'GB',
                'OXFORDSHIRE': 'GB', 'YORKSHIRE': 'GB', 'LANCASHIRE': 'GB',
                'SURREY': 'GB', 'KENT': 'GB', 'ESSEX': 'GB',
                'HERTFORDSHIRE': 'GB', 'BUCKINGHAMSHIRE': 'GB',
                'CAMBRIDGESHIRE': 'GB', 'DERBYSHIRE': 'GB',
                'LEICESTERSHIRE': 'GB', 'NOTTINGHAMSHIRE': 'GB',
                'STAFFORDSHIRE': 'GB', 'WARWICKSHIRE': 'GB',
                'WORCESTERSHIRE': 'GB', 'GLOUCESTERSHIRE': 'GB',
                'HAMPSHIRE': 'GB', 'DORSET': 'GB', 'DEVON': 'GB',
                'CORNWALL': 'GB', 'SOMERSET': 'GB', 'WILTSHIRE': 'GB',
                'ABINGDON': 'GB', 'MILTON PARK': 'GB',
                
                # Israeli Cities
                'TEL AVIV': 'IL', 'TELAVIV': 'IL', 'TEL-AVIV': 'IL',
                'JERUSALEM': 'IL', 'HAIFA': 'IL', 'HERZLIYA': 'IL',
                'PETAH TIKVA': 'IL', 'RAMAT GAN': 'IL', 'RISHON LEZION': 'IL',
                'NETANYA': 'IL', 'BEERSHEBA': 'IL', 'HOLON': 'IL',
                'KFAR SABA': 'IL', 'RAANANA': 'IL', 'MODIIN': 'IL',

                # Hong Kong Cities
                'HONG KONG': 'HK', 'KOWLOON': 'HK', 'WAN CHAI': 'HK',
                'CAUSEWAY BAY': 'HK', 'TSIM SHA TSUI': 'HK', 'ABERDEEN': 'HK',
                'CENTRAL': 'HK', 'ADMIRALTY': 'HK', 'SHATIN': 'HK',
                'KWUN TONG': 'HK', 'TSUEN WAN': 'HK',

                # Chinese Cities
                'BEIJING': 'CN', 'SHANGHAI': 'CN', 'SHENZHEN': 'CN',
                'GUANGZHOU': 'CN', 'HANGZHOU': 'CN', 'CHENGDU': 'CN',
                'NANJING': 'CN', 'WUHAN': 'CN', 'SUZHOU': 'CN',
                'XIAMEN': 'CN', 'TIANJIN': 'CN', 'DALIAN': 'CN',

                # Japanese Cities
                'TOKYO': 'JP', 'OSAKA': 'JP', 'YOKOHAMA': 'JP',
                'KYOTO': 'JP', 'NAGOYA': 'JP', 'FUKUOKA': 'JP', 'SAPPORO': 'JP',

                # Korean Cities
                'SEOUL': 'KR', 'BUSAN': 'KR', 'INCHEON': 'KR', 'SEONGNAM': 'KR',

                # Taiwanese Cities
                'TAIPEI': 'TW', 'KAOHSIUNG': 'TW', 'TAICHUNG': 'TW', 'HSINCHU': 'TW',

                # Singapore
                'SINGAPORE': 'SG',

                # Indian Cities
                'MUMBAI': 'IN', 'BANGALORE': 'IN', 'BENGALURU': 'IN',
                'HYDERABAD': 'IN', 'CHENNAI': 'IN', 'PUNE': 'IN', 'NEW DELHI': 'IN',

                # Canadian Cities
                'TORONTO': 'CA', 'VANCOUVER': 'CA', 'MONTREAL': 'CA',
                'CALGARY': 'CA', 'OTTAWA': 'CA',

                # Australian Cities
                'SYDNEY': 'AU', 'MELBOURNE': 'AU', 'BRISBANE': 'AU', 'PERTH': 'AU',

                # Brazilian Cities
                'SAO PAULO': 'BR', 'RIO DE JANEIRO': 'BR',

                # German Cities
                'BERLIN': 'DE', 'MUNICH': 'DE', 'FRANKFURT': 'DE', 'HAMBURG': 'DE',

                # French Cities
                'PARIS': 'FR', 'LYON': 'FR', 'MARSEILLE': 'FR',

                # Greek Cities
                'ATHENS': 'GR', 'THESSALONIKI': 'GR', 'PIRAEUS': 'GR',
                'HERAKLION': 'GR', 'PATRAS': 'GR', 'MAROUSI': 'GR',
                'GLYFADA': 'GR', 'KIFISSIA': 'GR',

                # Africa
                'SOUTH AFRICA': 'ZA',
                'NIGERIA': 'NG',
                'EGYPT': 'EG',
                'KENYA': 'KE',
                'MOROCCO': 'MA',
                'ETHIOPIA': 'ET',
                'GHANA': 'GH',
                
                # South America
                'BRAZIL': 'BR', 'BRASIL': 'BR',
                'ARGENTINA': 'AR',
                'CHILE': 'CL',
                'COLOMBIA': 'CO',
                'PERU': 'PE',
                'VENEZUELA': 'VE',
                'URUGUAY': 'UY',
                'PARAGUAY': 'PY',
                'ECUADOR': 'EC',
                
                # Oceania
                'AUSTRALIA': 'AU',
                'NEW ZEALAND': 'NZ',
                
                # Caribbean & Central America
                'BERMUDA': 'BM',
                'CAYMAN ISLANDS': 'KY',
                'BRITISH VIRGIN ISLANDS': 'VG', 'BVI': 'VG',
                'PANAMA': 'PA',
                'COSTA RICA': 'CR',
                'JAMAICA': 'JM',
                'BAHAMAS': 'BS',
                'BARBADOS': 'BB',
                
                # Special Administrative Regions
                'MACAU': 'MO', 'MACAO': 'MO',
                
                # Other
                'RUSSIA': 'RU', 'RUSSIAN FEDERATION': 'RU',
                'UKRAINE': 'UA',
                'KAZAKHSTAN': 'KZ',
                'PAKISTAN': 'PK',
                'BANGLADESH': 'BD',
                'SRI LANKA': 'LK',
                'MONACO': 'MC',
                'MALTA': 'MT',
                'CYPRUS': 'CY',
                'ICELAND': 'IS',
                'ESTONIA': 'EE',
                'LATVIA': 'LV',
                'LITHUANIA': 'LT',
                'SLOVENIA': 'SI',
                'SLOVAKIA': 'SK',
                'CROATIA': 'HR',
                'SERBIA': 'RS',
                'BULGARIA': 'BG',
                'ALBANIA': 'AL',
                'MACEDONIA': 'MK',
                'BOSNIA': 'BA',
                'MONTENEGRO': 'ME',
                'JERSEY': 'JE',
                'GUERNSEY': 'GG',
                'ISLE OF MAN': 'IM',
                'GIBRALTAR': 'GI',
                'LIECHTENSTEIN': 'LI',
                'ANDORRA': 'AD',
                'SAN MARINO': 'SM'
        }

        if country:
            # If it's already a 2-letter code, use it
            if len(country) == 2:
                return country.upper()

            # Try to match country name
            country_upper = country.upper().strip()
            country_code = country_mapping.get(country_upper, None)
            
            if country_code:
                return country_code
            
        # Check if city is mentioned in the address fields
        if address.get('city'):
            city_upper = address.get('city', '').upper().strip()
            # Remove common suffixes and clean up
            city_clean = city_upper.replace(',', '').replace('.', '').strip()
            # Try to find city in our mapping
            for location, code in country_mapping.items():
                if location in city_clean or city_clean.startswith(location):
                    return code
        
        # Check state field for international regions  
        if address.get('state'):
            state_upper = address.get('state', '').upper().strip()
            state_clean = state_upper.replace(',', '').replace('.', '').strip()
            for location, code in country_mapping.items():
                if location in state_clean or state_clean.startswith(location):
                    return code
            
            # If not found in mapping, try to extract first 2 letters if it looks like a country name
            if len(country) > 3:
                # Log unknown country for future mapping
                print(f"Unknown country: {country} for ticker {details.get('ticker', 'unknown')}")
                # Still default to US if unknown
                return 'US'
            
            # If it's a 3-letter code, try common conversions
            if len(country) == 3:
                three_to_two = {
                    'USA': 'US', 'GBR': 'GB', 'DEU': 'DE', 'FRA': 'FR',
                    'CHN': 'CN', 'JPN': 'JP', 'CAN': 'CA', 'AUS': 'AU',
                    'IND': 'IN', 'BRA': 'BR', 'MEX': 'MX', 'KOR': 'KR',
                    'ESP': 'ES', 'ITA': 'IT', 'NLD': 'NL', 'CHE': 'CH',
                    'SWE': 'SE', 'SGP': 'SG', 'HKG': 'HK', 'TWN': 'TW',
                    'ISR': 'IL', 'ZAF': 'ZA', 'ARE': 'AE', 'SAU': 'SA'
                }
                return three_to_two.get(country.upper(), 'US')
    
    # Check website domain for country hints (very reliable indicator)
    ticker = details.get('ticker', '').upper()
    name = details.get('name', '').upper() if details.get('name') else ''
    website = details.get('homepage_url', '').lower() if details.get('homepage_url') else ''
    description = details.get('description', '').lower() if details.get('description') else ''

    # Check website domain first - most reliable indicator
    if website:
        # Japanese domains
        if website.endswith('.jp') or website.endswith('.co.jp'):
            return 'JP'
        elif website.endswith('.cn') or website.endswith('.com.cn'):
            return 'CN'
        elif website.endswith('.hk') or website.endswith('.com.hk'):
            return 'HK'
        elif website.endswith('.sg') or website.endswith('.com.sg'):
            return 'SG'
        elif website.endswith('.kr') or website.endswith('.co.kr'):
            return 'KR'
        elif website.endswith('.tw') or website.endswith('.com.tw'):
            return 'TW'
        elif website.endswith('.in') or website.endswith('.co.in'):
            return 'IN'
        elif website.endswith('.il') or website.endswith('.co.il'):
            return 'IL'
        elif website.endswith('.uk') or website.endswith('.co.uk'):
            return 'GB'
        elif website.endswith('.ca'):
            return 'CA'
        elif website.endswith('.au') or website.endswith('.com.au'):
            return 'AU'
        elif website.endswith('.de'):
            return 'DE'
        elif website.endswith('.fr'):
            return 'FR'
        elif website.endswith('.nl'):
            return 'NL'
        elif website.endswith('.br') or website.endswith('.com.br'):
            return 'BR'

    # Check description for geographic clues (expanded patterns)
    if description:
        DESCRIPTION_PATTERNS = {
            'HK': ['hong kong-based', 'headquartered in hong kong', 'based in hong kong',
                    'incorporated in hong kong', 'hong kong sar'],
            'CN': ['china-based', 'headquartered in china', 'based in china',
                    'revenue from china', 'operations in china', 'incorporated in china',
                    'prc-based', 'beijing-based', 'shanghai-based', 'shenzhen-based'],
            'JP': ['japan-based', 'headquartered in japan', 'based in japan',
                    'revenue from japan', 'operations in japan', 'tokyo-based'],
            'IL': ['israel-based', 'headquartered in israel', 'based in israel',
                    'revenue from israel', 'operations in israel', 'tel aviv-based'],
            'CA': ['canada-based', 'headquartered in canada', 'based in canada',
                    'revenue from canada', 'operations in canada', 'toronto-based', 'vancouver-based'],
            'KR': ['korea-based', 'headquartered in korea', 'based in korea',
                    'south korea-based', 'seoul-based'],
            'TW': ['taiwan-based', 'headquartered in taiwan', 'based in taiwan'],
            'SG': ['singapore-based', 'headquartered in singapore', 'based in singapore'],
            'GB': ['uk-based', 'headquartered in the united kingdom', 'based in the uk',
                    'london-based', 'headquartered in london'],
            'DE': ['germany-based', 'headquartered in germany', 'based in germany'],
            'BR': ['brazil-based', 'headquartered in brazil', 'revenue from brazil',
                    'operations in brazil'],
            'IN': ['india-based', 'headquartered in india', 'based in india',
                    'revenue from india', 'operations in india', 'mumbai-based', 'bangalore-based'],
            'AU': ['australia-based', 'headquartered in australia', 'based in australia'],
            'BM': ['incorporated in bermuda', 'bermuda-based'],
            'KY': ['incorporated in the cayman islands', 'cayman islands-based'],
            'GR': ['greece-based', 'headquartered in greece', 'based in greece',
                    'athens-based', 'incorporated in greece'],
            'FI': ['finland-based', 'headquartered in finland', 'based in finland',
                    'helsinki-based', 'espoo-based'],
            'AE': ['dubai-based', 'headquartered in dubai', 'based in dubai',
                    'abu dhabi-based', 'headquartered in the uae', 'based in the uae',
                    'united arab emirates-based'],
            'SE': ['sweden-based', 'headquartered in sweden', 'based in sweden',
                    'stockholm-based'],
            'NO': ['norway-based', 'headquartered in norway', 'based in norway',
                    'oslo-based'],
            'NZ': ['new zealand-based', 'headquartered in new zealand', 'auckland-based'],
        }

        for code, patterns in DESCRIPTION_PATTERNS.items():
            if any(p in description for p in patterns):
                return code

    # Special handling for ADRs — try harder before defaulting to US
    if details.get('type') in ('ADR', 'ADRC'):
        # ADRs are foreign companies listed in US — use all available clues
        # We already checked address, website, and description above
        # If we still have no evidence, log and default to US
        logger.debug(f"ADR ticker {ticker} with no country evidence — defaulting to US")
    
    return country_code

def get_country_code(details: dict) -> str:
    """
    Synchronous wrapper for backwards compatibility
    For new code, prefer get_country_code_async() when possible
    """
    return get_country_code_sync(details)