import dateparser
import numpy as np
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import logging
@lru_cache(maxsize=1000)
def convert_date(date_string):
    """Converts a date string or Excel serial number to the specified format (DD/MM/YYYY).

    Args:
        date_string: A string or number representing a date.

    Returns:
        A string representing the date in the specified format, or None for empty or invalid rows.
    """
    # Check if the input is None or a missing numeric value
    if date_string is None or (isinstance(date_string, float) and np.isnan(date_string)):
        return None

    # Define common missing value representations
    missing_values = ["", "None", "NaN", "null", "N/A", "n/a", "na", "NA", "#N/A", "?", "missing"]

    # Check if the input is a recognized missing value
    if isinstance(date_string, str) and date_string.strip().lower() in [val.lower() for val in missing_values]:
        return None

    # Check if the input is a number (Excel serial date)
    try:
        serial_number = float(date_string)
        # Excel serial date base is 1899-12-30
        base_date = datetime(1899, 12, 30)
        calculated_date = base_date + timedelta(days=int(serial_number))
        return calculated_date.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        pass  # Not a valid number, proceed to parsing

    # Safeguard: Return None for empty or invalid strings before calling dateparser
    if not isinstance(date_string, str) or not date_string.strip():
        return None

    # Use dateparser to parse valid date strings
    date = dateparser.parse(date_string)
    if date is not None:
        return date.strftime('%d/%m/%Y')

    # Return None for unrecognized date formats
    return None
