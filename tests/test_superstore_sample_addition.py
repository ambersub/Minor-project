"""
Superstore sample CSV: add a constant to the *Sales* column and verify row results.

Uses ``tests/fixtures/superstore_sample.csv`` and :mod:`chunkflow.csv_math`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chunkflow.csv_math import (
    apply_csv_row_math_scalar,
    apply_csv_rows_math_scalar,
    split_csv_row,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "superstore_sample.csv"

COL_SALES = 17


def _load_fixture_lines() -> list[str]:
    text = FIXTURE.read_text(encoding="utf-8")
    return [ln for ln in text.splitlines() if ln.strip()]


def test_add_constant_to_sales_column_all_data_rows() -> None:
    """
    Add 50.0 to *Sales* for every data row; append the sum as a new column (index 21).
    Header row is unchanged. Check expected totals for each sample row.
    """
    rows = _load_fixture_lines()
    assert len(rows) >= 2

    delta = 50.0
    out_rows = apply_csv_rows_math_scalar(
        rows,
        "add",
        COL_SALES,
        delta,
        21,
        skip_header=True,
    )

    assert out_rows[0] == rows[0]
    header_cells = split_csv_row(out_rows[0])
    assert header_cells[COL_SALES] == "Sales"
    assert len(split_csv_row(out_rows[1])) == 22

    # Expected: original Sales + 50 for each data line in the fixture
    expected_sums = []
    for i in range(1, len(rows)):
        sales = float(split_csv_row(rows[i])[COL_SALES])
        expected_sums.append(sales + delta)

    for i in range(1, len(out_rows)):
        cells = split_csv_row(out_rows[i])
        result_col = float(cells[-1])
        assert result_col == pytest.approx(expected_sums[i - 1])

    # First data row in fixture: Sales 261.96 -> 311.96
    assert float(split_csv_row(out_rows[1])[-1]) == pytest.approx(311.96)


def test_addition_result_matches_manual_first_row() -> None:
    """Single-row check: addition to Sales yields the documented numeric result."""
    rows = _load_fixture_lines()
    first_data = rows[1]
    out = apply_csv_row_math_scalar(first_data, "add", COL_SALES, 100.0, 21)
    cells = split_csv_row(out)
    assert float(cells[COL_SALES]) == pytest.approx(261.96)
    assert float(cells[-1]) == pytest.approx(361.96)
