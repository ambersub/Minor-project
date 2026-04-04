"""
CSV row split/join and numeric column operations.

Uses the ``chunkflow_core`` extension when it imports successfully; otherwise falls
back to :mod:`chunkflow.csv_math_impl` so tools and tests work without MinGW/SQLite DLLs.
"""

from __future__ import annotations

__all__ = [
    "csv_math_backend",
    "split_csv_row",
    "join_csv_row",
    "apply_csv_row_math_binary",
    "apply_csv_row_math_scalar",
    "apply_csv_rows_math_binary",
    "apply_csv_rows_math_scalar",
]

try:
    import chunkflow_core as _core

    csv_math_backend = "cpp"
    split_csv_row = _core.split_csv_row
    join_csv_row = _core.join_csv_row
    apply_csv_row_math_binary = _core.apply_csv_row_math_binary
    apply_csv_row_math_scalar = _core.apply_csv_row_math_scalar
    apply_csv_rows_math_binary = _core.apply_csv_rows_math_binary
    apply_csv_rows_math_scalar = _core.apply_csv_rows_math_scalar
except ImportError:
    from chunkflow.csv_math_impl import (
        apply_csv_row_math_binary,
        apply_csv_row_math_scalar,
        apply_csv_rows_math_binary,
        apply_csv_rows_math_scalar,
        join_csv_row,
        split_csv_row,
    )

    csv_math_backend = "python"
