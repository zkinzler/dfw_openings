"""
Normalization utilities for matching venues across data sources.
"""

import re

STOP_WORDS = {"llc", "inc", "inc.", "co", "company", "restaurant", "bar", "ltd", "corp", "corporation"}


def normalize_name(name: str | None) -> str:
    """
    Normalize a business name for matching.
    - Lowercase
    - Remove punctuation
    - Remove common stop words
    - Remove extra whitespace
    """
    if not name:
        return ""

    s = name.lower()
    # Remove punctuation, keep letters, numbers, and spaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    # Split into tokens and remove stop words
    tokens = [t for t in s.split() if t and t not in STOP_WORDS]
    return " ".join(tokens)


def normalize_address(addr: str | None) -> str:
    """
    Normalize an address for matching.
    - Lowercase
    - Expand common abbreviations
    - Remove extra whitespace
    """
    if not addr:
        return ""

    s = addr.lower()

    # Expand common abbreviations
    replacements = {
        " ste ": " suite ",
        " st ": " street ",
        " st.": " street",
        " rd ": " road ",
        " rd.": " road",
        " ave ": " avenue ",
        " ave.": " avenue",
        " blvd ": " boulevard ",
        " blvd.": " boulevard",
        " dr ": " drive ",
        " dr.": " drive",
        " ln ": " lane ",
        " ln.": " lane",
        " ct ": " court ",
        " ct.": " court",
        " pkwy ": " parkway ",
        " pkwy.": " parkway",
    }

    for old, new in replacements.items():
        s = s.replace(old, new)

    # Clean up extra whitespace
    s = re.sub(r"\s+", " ", s)
    return s.strip()
