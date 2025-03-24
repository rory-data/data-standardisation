"""
This module provides functions for standardising and normalising data across a dataset, ensuring consistent formatting and representation of various data types.

Functions:
---------
    standardise_strings: Standardises string columns by trimming whitespace, converting to uppercase, and normalising Unicode characters.
    normalise_unicode_string: Normalises Unicode strings by handling diacritics and non-Latin characters while preserving special characters like macrons.
    normalise_unicode: Wrapper that registers the Unicode normalisation function as an Ibis UDF.
    normalise_timestamps: Converts timestamp columns to a consistent ISO-8601 format.
"""

import logging
import re
import unicodedata as uni
from datetime import datetime
from typing import Any

import ibis  # type: ignore
import ibis.selectors as sel  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
)


def standardise_strings(table_expr: Any) -> Any:
    """
     Standardise string columns by trimming whitespace, converting to uppercase, and normalising Unicode.

    This function applies the following transformations to all string columns:
    1. Removes leading and trailing whitespace
    2. Converts text to uppercase for consistent comparison
    3. Normalises Unicode characters (removes diacritics, handles special characters)

    Args:
        table_expr (ibis.expr.types.Table): The input Ibis table expression.

    Returns:
        ibis.expr.types.Table: The table with standardised string columns.

    Example:
        >>> table = ibis.table([('name', 'string'), ('address', 'string')])
        >>> standardised = standardise_strings(table)

    """
    try:
        string_columns = table_expr.select(sel.of_type("string")).columns
        for col in string_columns:
            try:
                table_expr = table_expr.mutate(**{col: table_expr[col].strip().upper()})
                table_expr = table_expr.mutate(**{col: normalise_unicode(table_expr[col])})
            except Exception as e:
                logging.error(f"Error processing column {col}: {e}")
        return table_expr
    except Exception as e:
        logging.error(f"Error in standardise_strings: {e}")
        return table_expr


def normalise_unicode_string(text: str) -> str:
    """
    Normalise a Unicode string by handling diacritics and non-Latin characters.

    This function:
    1. Removes non-Latin characters
    2. Normalises diacritics (e.g. converts é to e, ā to a)
    3. Removes control characters

    Args:
        text (Optional[str]): The input string to normalise. Can be None.

    Returns:
        Optional[str]: The normalised string, or None if input was None.

    Example:
        >>> normalise_unicode_string("Café")
        "Cafe"
        >>> normalise_unicode_string("Māori")
        "Maori"
    """
    if text is None:
        return None
    try:
        # Remove non-Latin characters except macrons
        def replace_non_latin(match: Any) -> str:
            char = str(match.group(0))
            # Keep macrons (ā, ē, ī, ō, ū, Ā, Ē, Ī, Ō, Ū)
            if char in "āēīōūĀĒĪŌŪ":
                return char
            else:
                return ""

        # Match any character outside basic Latin range, except keep macrons
        text = re.sub(r"[^\x00-\xFF]", replace_non_latin, text)
        # Normalise to decompose diacritic characters
        normalised_text = uni.normalize("NFKD", text)
        # Filter out non-spacing marks (the decomposed diacritics)
        without_diacritics = "".join(c for c in normalised_text if uni.category(c) != "Mn")
        # Remove control characters
        cleaned_text = "".join(c for c in without_diacritics if uni.category(c)[0] != "C")
        return cleaned_text
    except Exception as e:
        logging.error(f"Error in normalise_unicode_string for text '{text}': {e}")
        return text


@ibis.udf.scalar.python
def normalise_unicode(text: str) -> str:
    """
    Ibis UDF wrapper for Unicode normalisation function.

    This registers the string normalisation function as a user-defined function
    that can be used within Ibis expressions.

    Args:
        text (Optional[str]): The input string to normalise.

    Returns:
        Optional[str]: The normalised string.
    """
    try:
        return normalise_unicode_string(text)
    except Exception as e:
        logging.error(f"Error in normalise_unicode UDF: {e}")
        return text


def normalise_timestamps(table_expr: Any, timestamp_columns: list[str]) -> Any:
    """
    Normalise timestamp columns to ISO-8601 format.

    This function attempts to convert various date/time formats to a consistent timestamp representation using Ibis' try_cast functionality. This ensures all datetime values follow a consistent standard.

    Args:
        table_expr (ibis.expr.types.Table): The input Ibis table expression.
        timestamp_columns (List[str]): List of column names to normalise as timestamps.

    Returns:
        ibis.expr.types.Table: The table with normalised timestamp columns.

    Notes:
        - Columns that cannot be converted will maintain their original format
        - Errors during conversion are logged but won't stop processing

    Example:
        >>> table = ibis.table([('date_col', 'string'), ('created_at', 'string')])
        >>> with_timestamps = normalise_timestamps(table, ['date_col', 'created_at'])
    """
    try:
        for col in timestamp_columns:
            if col in table_expr.columns:
                try:
                    table_expr = table_expr.mutate(**{col: table_expr[col].try_cast("timestamp")})
                except Exception as e:
                    logging.error(
                        f"Warning: Could not normalise timestamp format for column '{col}'. "
                        f"Ensure the column contains valid timestamp data. Error: {e}"
                    )
        return table_expr
    except Exception as e:
        logging.error(f"Error in normalise_timestamps: {e}")
        return table_expr
