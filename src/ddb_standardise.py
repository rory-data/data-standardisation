"""
This script standardises data from a Parquet file using a combination of Ibis and Polars for efficient data processing.

It performs the following operations:

1. Column name standardisation (snake_case)
2. Null value handling and normalisation
3. Timestamp format standardisation
4. String cleansing and normalisation
5. Row deduplication
6. Metadata tagging

Input/Output:
------------
- Input: Parquet file with any column structure
- Output: Standardised Parquet file with consistent formatting
"""

import logging
from datetime import datetime

import ibis  # type: ignore
import polars as pl

from include import standardise as std  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
)

# Values that should be converted to NULL for consistency
# Any field matching these values will be standardised to NULL
NULL_VALUES = ["NULL", "N/A", ""]

# Columns containing date/time information that need format standardisation
# These columns will be converted to a consistent ISO timestamp format
TIMESTAMP_COLUMNS = ["date_column"]

# File paths for input and output data
# Relative paths from the project root directory
INPUT_FILEPATH = "files/input.parquet"
OUTPUT_FILEPATH = "files/output.parquet"


if __name__ == "__main__":
    try:
        con = ibis.duckdb.connect()
        table = con.read_parquet(INPUT_FILEPATH)
    except Exception as e:
        logging.error(f"Error reading in Parquet file: {e}")
        exit(1)

    # 1. Standardise column names to snake_case
    try:
        table = table.rename("snake_case")
    except Exception as e:
        logging.error(f"Error renaming columns to snake_case: {e}")

    # 2. Basic null/empty value handling
    try:
        case_exprs = [
            ibis.case().when(table[col].cast("string").isin(NULL_VALUES), ibis.null()).else_(table[col]).end().name(col)
            for col in table.columns
        ]
        table_expr = table.select(*case_exprs)
    except Exception as e:
        logging.error(f"Error handling null/empty values: {e}")
        table_expr = table  # Continue with the original table

    # 3. Timestamp format normalisation. Converts various date/time formats to ISO-8601 standard format.
    try:
        table_expr = std.normalise_timestamps(table_expr, timestamp_columns=TIMESTAMP_COLUMNS)
    except Exception as e:
        logging.error(f"Error normalising timestamps: {e}")

    # 4. String cleansing and normalisation
    try:
        table_expr = std.standardise_strings(table_expr)
    except Exception as e:
        logging.error(f"Error standardising strings: {e}")

    # 5. Simple deduplication
    try:
        before_dedup = len(table_expr.execute())
        table_expr = table_expr.distinct()
        after_dedup = len(table_expr.execute())
        logging.info(f"Deduplicated rows: {before_dedup - after_dedup}")
    except Exception as e:
        logging.error(f"Error during deduplication: {e}")

    # 6. Dataset metadata tagging
    try:
        table_expr = table_expr.mutate(
            standardise_timestamp=ibis.literal(datetime.now().isoformat(sep=" ", timespec="seconds")).try_cast(
                "timestamp"
            ),
            data_source=ibis.literal("UNSPECIFIED"),
        )
    except Exception as e:
        logging.error(f"Error tagging metadata: {e}")

    try:
        table_expr.to_parquet(OUTPUT_FILEPATH)
        logging.info("Successfully cleansed data and exported.")
    except Exception as e:
        logging.error(f"Error writing Parquet file with Ibis/Polars: {e}")

    try:
        df_output_ibis_polars = pl.read_parquet(OUTPUT_FILEPATH)
        logging.info("\nCleaned Data (Ibis/Polars - Snake Case):")
        logging.info(df_output_ibis_polars)
    except Exception as e:
        logging.error(f"Error reading output Parquet file with Polars: {e}")
