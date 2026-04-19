"""
CSV row split/join and numeric column operations.

Uses the C++ ``chunkflow_core`` extension exclusively for all CSV operations.
"""

from __future__ import annotations

import chunkflow_core as _core

__all__ = [
    "csv_math_backend",
    # CSV parsing
    "split_csv_row",
    "join_csv_row",
    "split_delimited_row",
    "join_delimited_row",
    # Binary arithmetic
    "apply_csv_row_math_binary",
    "apply_csv_row_math_scalar",
    "apply_csv_rows_math_binary",
    "apply_csv_rows_math_scalar",
    # Extended arithmetic (unary + power)
    "apply_csv_row_math_unary",
    "apply_csv_rows_math_unary",
    "apply_csv_row_math_power",
    # Filtering
    "filter_rows",
    "filter_rows_by_field",
    "filter_rows_by_range",
]

csv_math_backend = "cpp"

# CSV parsing
split_csv_row = _core.split_csv_row
join_csv_row = _core.join_csv_row
split_delimited_row = _core.split_delimited_row
join_delimited_row = _core.join_delimited_row

# Binary arithmetic
apply_csv_row_math_binary = _core.apply_csv_row_math_binary
apply_csv_row_math_scalar = _core.apply_csv_row_math_scalar
apply_csv_rows_math_binary = _core.apply_csv_rows_math_binary
apply_csv_rows_math_scalar = _core.apply_csv_rows_math_scalar

# Extended arithmetic
apply_csv_row_math_unary = _core.apply_csv_row_math_unary
apply_csv_rows_math_unary = _core.apply_csv_rows_math_unary
apply_csv_row_math_power = _core.apply_csv_row_math_power

# Filtering
filter_rows = _core.filter_rows
filter_rows_by_field = _core.filter_rows_by_field
filter_rows_by_range = _core.filter_rows_by_range
