from decimal import Decimal, ROUND_HALF_UP


def money(value):
    """
    Normalize a numeric value to a 2‑decimal float suitable for display
    and JSON serialization.

    - Accepts None, int, float, Decimal
    - Returns float rounded to 2 decimal places
    """

    if value is None:
        return 0.0

    # Convert to Decimal for safe rounding
    amount = Decimal(str(value)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )

    return float(amount)
