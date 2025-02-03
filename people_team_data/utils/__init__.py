"""An assortment of utility functions used by the People Team Data project."""
import re
from numbers import Number

import pandas as pd

from .constants import *


def to_camel_case(s):
    """Converts a string to camel case."""
    s = re.sub(r'[^a-zA-Z0-9]+', ' ', str(s))
    words = s.split()
    if not words:
        return ''
    return words[0].lower() + ''.join(word.capitalize() for word in words[1:])

def is_camel_case(s):
    """Checks to see if a string is in camel case."""
    return isinstance(s, str) and bool(re.match(r'^[a-z]+[A-Za-z0-9]*$', s))

def currency_to_decimal(s: str):
    """Converts a currency string to a unitless decimal."""
    if isinstance(s, Number):
        return s
    elif not isinstance(s, str):
        return None
    else:
        return re.sub(r"[^\d.]", "", str(s)).strip() or None
    
def apply_config_to_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Mutatively pplies the configuration to the DataFrame."""
    # Convert columns to the specified types
    for column, dtype in config.get("columns", {}).items():
        if dtype == "currency":
            df[column] = pd.to_numeric(df[column].apply(currency_to_decimal), errors='coerce', downcast='float')
        elif dtype == "datetime64[ns]":
            df[column] = pd.to_datetime(df[column], errors='coerce')
        elif dtype == "int":
            df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
        elif dtype == "float":
            df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
        else:
            df[column] = df[column].astype(dtype, errors='ignore')

    # Drop rows with null values in non-null columns
    non_null_columns = config.get("non_null", [])
    df.dropna(subset=non_null_columns, inplace=True)

    return df