"""
CSV row split/join and numeric column operations.

Uses the C++ ``chunkflow_core`` extension exclusively for all CSV operations.
"""

from __future__ import annotations

import chunkflow_core as _core

__all__ = [
    "csv_math_backend",
    "split_csv_row",
    "join_csv_row",
    "apply_csv_row_math_binary",
    "apply_csv_row_math_scalar",
    "apply_csv_rows_math_binary",
    "apply_csv_rows_math_scalar",
]

csv_math_backend = "cpp"
split_csv_row = _core.split_csv_row
join_csv_row = _core.join_csv_row
apply_csv_row_math_binary = _core.apply_csv_row_math_binary
apply_csv_row_math_scalar = _core.apply_csv_row_math_scalar
apply_csv_rows_math_binary = _core.apply_csv_rows_math_binary
apply_csv_rows_math_scalar = _core.apply_csv_rows_math_scalar
