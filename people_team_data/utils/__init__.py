"""An assortment of utility functions used by the People Team Data project."""
import re
from numbers import Number

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