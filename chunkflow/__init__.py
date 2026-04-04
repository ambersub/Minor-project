"""
chunkflow — CSV row utilities (current public scope).

Split/join CSV lines and apply numeric column operations row-wise. The implementation
prefers the ``chunkflow_core`` extension when it loads; otherwise uses pure Python.

Example
-------
    from chunkflow import split_csv_row, apply_csv_rows_math_scalar

    lines = path.read_text(encoding="utf-8").splitlines()
    out = apply_csv_rows_math_scalar(lines, "add", col_sales, 50.0, new_col, skip_header=True)

Chunked SQLite processing (RERA / large jobs) lives in ``chunkflow.chunking``:

    from chunkflow.chunking import ChunkProcessor
"""

from __future__ import annotations

from chunkflow.csv_math import (
    apply_csv_row_math_binary,
    apply_csv_row_math_scalar,
    apply_csv_rows_math_binary,
    apply_csv_rows_math_scalar,
    csv_math_backend,
    join_csv_row,
    split_csv_row,
)

__all__ = [
    "apply_csv_row_math_binary",
    "apply_csv_row_math_scalar",
    "apply_csv_rows_math_binary",
    "apply_csv_rows_math_scalar",
    "csv_math_backend",
    "join_csv_row",
    "split_csv_row",
]
