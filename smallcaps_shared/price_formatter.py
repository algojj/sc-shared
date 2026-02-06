"""
Price formatting utilities with adaptive precision
Ensures correct decimal places for stocks at different price ranges
"""


def format_price(price: float) -> str:
    """
    Format price with adaptive precision based on price range

    Rules:
    - Price < $1: Use 4 decimals (e.g., $0.3125)
    - Price >= $1 and < $10: Use 3 decimals (e.g., $5.125)
    - Price >= $10: Use 2 decimals (e.g., $150.25)

    This ensures:
    - Penny stocks show enough precision (TOVX $0.3125 vs $0.32)
    - Low-price stocks don't lose info ($5.125 vs $5.13)
    - Higher-price stocks stay clean ($150.25 vs $150.250)

    Args:
        price: The price to format

    Returns:
        Formatted price string (e.g., "0.3125", "5.125", "150.25")
    """
    if price is None:
        return "N/A"

    try:
        price_float = float(price)

        if price_float < 1.0:
            # Penny stocks: 4 decimals
            return f"{price_float:.4f}"
        elif price_float < 10.0:
            # Low-price stocks: 3 decimals
            return f"{price_float:.3f}"
        else:
            # Regular stocks: 2 decimals
            return f"{price_float:.2f}"

    except (ValueError, TypeError):
        return str(price)


def get_price_precision(price: float) -> int:
    """
    Get the number of decimal places to use for a given price

    Args:
        price: The price to check

    Returns:
        Number of decimal places (2, 3, or 4)
    """
    if price is None:
        return 2

    try:
        price_float = float(price)

        if price_float < 1.0:
            return 4  # Penny stocks
        elif price_float < 10.0:
            return 3  # Low-price stocks
        else:
            return 2  # Regular stocks

    except (ValueError, TypeError):
        return 2  # Default to 2 decimals on error


# Convenience function for dollar formatting
def format_dollar(price: float) -> str:
    """Format price with $ sign"""
    return f"${format_price(price)}"
