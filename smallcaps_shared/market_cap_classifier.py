"""
Market Cap Classification Utility
Classifies companies based on their market capitalization
"""

from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class MarketCapClassifier:
    """
    Classifies companies by market capitalization using standard definitions
    """

    # Market cap thresholds in USD
    THRESHOLDS = {
        'NANO': (0, 50_000_000),           # < $50M
        'MICRO': (50_000_000, 300_000_000), # $50M - $300M
        'SMALL': (300_000_000, 2_000_000_000), # $300M - $2B
        'MID': (2_000_000_000, 10_000_000_000), # $2B - $10B
        'LARGE': (10_000_000_000, 200_000_000_000), # $10B - $200B
        'MEGA': (200_000_000_000, float('inf')) # > $200B
    }

    @classmethod
    def classify_market_cap(cls, market_cap: Optional[float]) -> Tuple[str, Optional[int]]:
        """
        Classify market cap into standard categories

        Args:
            market_cap: Market capitalization in USD (can be None)

        Returns:
            Tuple of (category, market_cap_value)
            category: One of NANO, MICRO, SMALL, MID, LARGE, MEGA, or UNKNOWN
            market_cap_value: Market cap as integer (None if invalid)
        """
        if market_cap is None or market_cap <= 0:
            return 'UNKNOWN', None

        try:
            market_cap_int = int(market_cap)

            for category, (min_val, max_val) in cls.THRESHOLDS.items():
                if min_val <= market_cap < max_val:
                    return category, market_cap_int

            # Shouldn't reach here, but fallback
            return 'UNKNOWN', market_cap_int

        except (ValueError, TypeError, OverflowError):
            logger.warning(f"Invalid market cap value: {market_cap}")
            return 'UNKNOWN', None

    @classmethod
    def get_category_range(cls, category: str) -> Tuple[str, str]:
        """
        Get human-readable range for a category

        Args:
            category: Market cap category

        Returns:
            Tuple of (min_readable, max_readable)
        """
        if category not in cls.THRESHOLDS:
            return 'Unknown', 'Unknown'

        min_val, max_val = cls.THRESHOLDS[category]

        min_readable = cls._format_market_cap(min_val)
        if max_val == float('inf'):
            max_readable = '∞'
        else:
            max_readable = cls._format_market_cap(max_val)

        return min_readable, max_readable

    @classmethod
    def _format_market_cap(cls, value: float) -> str:
        """Format market cap value to human readable string"""
        if value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.1f}B"
        elif value >= 1_000_000:
            return f"${value / 1_000_000:.0f}M"
        elif value >= 1_000:
            return f"${value / 1_000:.0f}K"
        else:
            return f"${value:.0f}"

    @staticmethod
    def format_shares(value: float) -> str:
        """
        Format shares/float value to human readable string WITHOUT dollar sign
        Examples:
            214340000 -> "214.34M"
            494700000 -> "494.70M"
            1200000000 -> "1.20B"
            500000 -> "500.00K"
        """
        if value is None or value == 0:
            return "0"
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        elif value >= 1_000:
            return f"{value / 1_000:.2f}K"
        else:
            return f"{int(value)}"

    @classmethod
    def get_category_description(cls, category: str) -> str:
        """Get description of market cap category"""
        descriptions = {
            'NANO': 'Nano Cap - Smallest public companies, highest risk/reward',
            'MICRO': 'Micro Cap - Small companies, high volatility, limited liquidity',
            'SMALL': 'Small Cap - Growing companies, moderate volatility',
            'MID': 'Mid Cap - Established companies, balanced growth/stability',
            'LARGE': 'Large Cap - Major corporations, lower volatility',
            'MEGA': 'Mega Cap - Largest companies, most stable',
            'UNKNOWN': 'Unknown - Market cap not available or invalid'
        }
        return descriptions.get(category, 'Unknown category')

    @classmethod
    def is_small_cap_or_below(cls, category: str) -> bool:
        """Check if category is small cap or smaller (for scanner targeting)"""
        return category in ['NANO', 'MICRO', 'SMALL']

    @classmethod
    def get_trading_characteristics(cls, category: str) -> dict:
        """Get typical trading characteristics for each category"""
        characteristics = {
            'NANO': {
                'volatility': 'Very High',
                'liquidity': 'Very Low',
                'gap_frequency': 'Very High',
                'institutional_ownership': 'Very Low',
                'retail_friendly': True,
                'premarket_activity': 'High'
            },
            'MICRO': {
                'volatility': 'High',
                'liquidity': 'Low',
                'gap_frequency': 'High',
                'institutional_ownership': 'Low',
                'retail_friendly': True,
                'premarket_activity': 'High'
            },
            'SMALL': {
                'volatility': 'Moderate-High',
                'liquidity': 'Moderate',
                'gap_frequency': 'Moderate',
                'institutional_ownership': 'Moderate',
                'retail_friendly': True,
                'premarket_activity': 'Moderate'
            },
            'MID': {
                'volatility': 'Moderate',
                'liquidity': 'High',
                'gap_frequency': 'Low-Moderate',
                'institutional_ownership': 'High',
                'retail_friendly': False,
                'premarket_activity': 'Low'
            },
            'LARGE': {
                'volatility': 'Low-Moderate',
                'liquidity': 'Very High',
                'gap_frequency': 'Low',
                'institutional_ownership': 'Very High',
                'retail_friendly': False,
                'premarket_activity': 'Very Low'
            },
            'MEGA': {
                'volatility': 'Low',
                'liquidity': 'Extremely High',
                'gap_frequency': 'Very Low',
                'institutional_ownership': 'Extremely High',
                'retail_friendly': False,
                'premarket_activity': 'Very Low'
            }
        }
        return characteristics.get(category, {})


if __name__ == "__main__":
    # Test the classifier
    test_cases = [
        (25_000_000, 'NANO'),      # $25M
        (100_000_000, 'MICRO'),    # $100M
        (1_000_000_000, 'SMALL'),  # $1B
        (5_000_000_000, 'MID'),    # $5B
        (50_000_000_000, 'LARGE'), # $50B
        (500_000_000_000, 'MEGA'), # $500B
        (None, 'UNKNOWN'),
        (-1000, 'UNKNOWN')
    ]

    print("🧪 Testing Market Cap Classifier")
    print("=" * 50)

    for market_cap, expected in test_cases:
        category, value = MarketCapClassifier.classify_market_cap(market_cap)
        status = "✅" if category == expected else "❌"

        print(f"{status} ${market_cap} -> {category} (expected {expected})")

        if category != 'UNKNOWN':
            min_range, max_range = MarketCapClassifier.get_category_range(category)
            print(f"   Range: {min_range} - {max_range}")
            print(f"   Description: {MarketCapClassifier.get_category_description(category)}")
            characteristics = MarketCapClassifier.get_trading_characteristics(category)
            print(f"   Volatility: {characteristics.get('volatility', 'N/A')}")
            print()