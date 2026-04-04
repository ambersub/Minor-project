"""
Tests for CSV row-wise math (Superstore-shaped rows).

Uses :mod:`chunkflow.csv_math`, which prefers ``chunkflow_core`` when the extension
loads and otherwise uses the pure-Python implementation (so tests still run when
Windows reports DLL load failures for the .pyd).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chunkflow.csv_math import (
    apply_csv_row_math_binary,
    apply_csv_row_math_scalar,
    apply_csv_rows_math_binary,
    csv_math_backend,
    join_csv_row,
    split_csv_row,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "superstore_sample.csv"

# Superstore-style column indices (0-based): Sales, Quantity, Discount, Profit
COL_SALES = 17
COL_QTY = 18
COL_DISCOUNT = 19
COL_PROFIT = 20


def _load_fixture_lines() -> list[str]:
    text = FIXTURE.read_text(encoding="utf-8")
    return [ln for ln in text.splitlines() if ln.strip()]


def test_backend_is_cpp_or_python() -> None:
    assert csv_math_backend in ("cpp", "python")


def test_split_csv_superstore_header_column_count() -> None:
    rows = _load_fixture_lines()
    fields = split_csv_row(rows[0])
    assert len(fields) == 21
    assert fields[0] == "Row ID"
    assert fields[COL_SALES] == "Sales"
    assert fields[COL_PROFIT] == "Profit"


def test_split_csv_quoted_product_name_preserved() -> None:
    rows = _load_fixture_lines()
    fields = split_csv_row(rows[2])
    assert "Hon Deluxe Fabric Upholstered Stacking Chairs" in fields[16]
    assert "," in fields[16]


def test_join_split_roundtrip_quoted_row() -> None:
    rows = _load_fixture_lines()
    line = rows[2]
    fields = split_csv_row(line)
    assert join_csv_row(fields) == line


def test_apply_row_add_sales_plus_quantity_appended_column() -> None:
    rows = _load_fixture_lines()
    data = rows[1]
    out = apply_csv_row_math_binary(
        data, "add", COL_SALES, COL_QTY, 21
    )
    cells = split_csv_row(out)
    assert len(cells) == 22
    assert float(cells[-1]) == pytest.approx(261.96 + 2.0)


def test_apply_row_subtract_profit_minus_sales_replace_profit() -> None:
    rows = _load_fixture_lines()
    data = rows[1]
    out = apply_csv_row_math_binary(
        data, "sub", COL_PROFIT, COL_SALES, COL_PROFIT
    )
    cells = split_csv_row(out)
    assert len(cells) == 21
    assert float(cells[COL_PROFIT]) == pytest.approx(41.9136 - 261.96)


def test_apply_rows_all_add_with_skip_header() -> None:
    rows = _load_fixture_lines()
    out_rows = apply_csv_rows_math_binary(
        rows, "add", COL_SALES, COL_QTY, 21, True
    )
    assert len(out_rows) == len(rows)
    assert out_rows[0] == rows[0]
    for i in range(1, len(rows)):
        cells = split_csv_row(out_rows[i])
        sales = float(split_csv_row(rows[i])[COL_SALES])
        qty = float(split_csv_row(rows[i])[COL_QTY])
        assert float(cells[-1]) == pytest.approx(sales + qty)


def test_apply_row_scalar_subtract_discount_from_sales() -> None:
    rows = _load_fixture_lines()
    # Row 4 in full dataset has discount; use row with discount 0: no change in spirit
    data = rows[1]
    out = apply_csv_row_math_scalar(
        data, "sub", COL_SALES, 100.0, 21
    )
    cells = split_csv_row(out)
    assert float(cells[-1]) == pytest.approx(261.96 - 100.0)


def test_apply_row_multiply_quantity_by_scalar() -> None:
    rows = _load_fixture_lines()
    data = rows[1]
    out = apply_csv_row_math_scalar(data, "mul", COL_QTY, 10.0, COL_QTY)
    cells = split_csv_row(out)
    assert float(cells[COL_QTY]) == pytest.approx(20.0)


def test_divide_sales_by_quantity() -> None:
    rows = _load_fixture_lines()
    data = rows[1]
    out = apply_csv_row_math_binary(
        data, "div", COL_SALES, COL_QTY, 21
    )
    cells = split_csv_row(out)
    assert float(cells[-1]) == pytest.approx(261.96 / 2.0)


def test_division_by_zero_raises() -> None:
    rows = _load_fixture_lines()
    data = rows[1]
    with pytest.raises(ValueError, match="division by zero"):
        apply_csv_row_math_binary(data, "div", COL_SALES, COL_DISCOUNT, 21)
