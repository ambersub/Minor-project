"""
Superstore **CSV** sample: read a ``.csv`` file, add to *Sales* via ``chunkflow_core``,
write **CSV** ``output.csv`` (comma-separated lines, UTF-8).

- Tests: ``python -m pytest tests/test_superstore_sample_addition.py -v``
- CLI: ``python tests/test_superstore_sample_addition.py [input.csv] [output.csv]``
(defaults: fixture → repo ``output.csv``). Paths must end with ``.csv``.

On Windows DLL errors for ``chunkflow_core``, see PATH / ``CHUNKFLOW_MINGW_BIN``.
"""

from __future__ import annotations

import csv
import sys
from io import StringIO
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_CSV = REPO_ROOT / "output.csv"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "superstore_sample.csv"

COL_SALES = 17


def _bootstrap_windows_dlls() -> None:
    if sys.platform != "win32":
        return
    import os
    import shutil

    candidates: list[Path] = []
    gcc = shutil.which("gcc")
    if gcc:
        candidates.append(Path(gcc).resolve().parent)
    env_bin = os.environ.get("CHUNKFLOW_MINGW_BIN", "").strip()
    if env_bin:
        candidates.append(Path(env_bin))
    mp = os.environ.get("MINGW_PREFIX", "").strip()
    if mp:
        candidates.append(Path(mp) / "bin")

    seen: set[Path] = set()
    for p in candidates:
        try:
            p = p.resolve()
        except OSError:
            continue
        if not p.is_dir() or p in seen:
            continue
        seen.add(p)
        has_sqlite = any((p / name).exists() for name in (
            "sqlite3.dll",
            "libsqlite3-0.dll",
            "libsqlite3.dll",
        ))
        if not has_sqlite:
            continue
        try:
            os.add_dll_directory(str(p))
        except (AttributeError, OSError):
            pass


_bootstrap_windows_dlls()

try:
    import chunkflow_core
except ImportError:
    chunkflow_core = None  # type: ignore[misc, assignment]


def _require_chunkflow_core():
    if chunkflow_core is None:
        pytest.skip(
            "chunkflow_core could not be imported (build with pip install -e .). "
            "On Windows, DLL load often means sqlite3.dll / MinGW bin is missing.",
        )
    return chunkflow_core


def _assert_csv_file(path: Path) -> None:
    if path.suffix.lower() != ".csv":
        raise ValueError(f"CSV only: path must use .csv extension, got {path}")


def _csv_field_count(line: str) -> int:
    return len(next(csv.reader(StringIO(line))))


def read_csv_lines(path: Path) -> list[str]:
    """Load a text CSV file: UTF-8 (with BOM stripped), non-empty lines, tabular rows only."""
    _assert_csv_file(path)
    text = path.read_text(encoding="utf-8-sig")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines, start=1):
        n = _csv_field_count(ln)
        if n < 2:
            raise ValueError(
                f"CSV only: line {i} in {path} must have at least two columns, got {n}"
            )
    return lines


def write_csv_lines(path: Path, lines: list[str]) -> None:
    """Write CSV text: one record per line, comma-separated, UTF-8 without BOM."""
    _assert_csv_file(path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_fixture_lines() -> list[str]:
    return read_csv_lines(FIXTURE)


def _run_add_sales_write_csv(
    input_csv: Path,
    output_csv: Path,
    delta: float = 50.0,
) -> list[str]:
    assert chunkflow_core is not None
    cf = chunkflow_core
    rows = read_csv_lines(input_csv)
    if len(rows) < 2:
        raise ValueError("CSV must contain a header row and at least one data row")

    out_rows = cf.apply_csv_rows_math_scalar(
        rows,
        "add",
        COL_SALES,
        delta,
        21,
        skip_header=True,
    )
    write_csv_lines(output_csv, out_rows)
    return out_rows


def test_input_and_output_are_csv_files() -> None:
    _assert_csv_file(FIXTURE)
    _assert_csv_file(OUTPUT_CSV)


def test_add_constant_to_sales_column_writes_valid_csv_output() -> None:
    """
    CSV in → ``chunkflow_core`` (CSV rows) → CSV out at ``output.csv``.
    Re-parse output with the stdlib csv reader to ensure valid CSV records.
    """
    rows = _load_fixture_lines()
    assert len(rows) >= 2

    delta = 50.0
    cf = _require_chunkflow_core()
    out_rows = cf.apply_csv_rows_math_scalar(
        rows,
        "add",
        COL_SALES,
        delta,
        21,
        skip_header=True,
    )

    write_csv_lines(OUTPUT_CSV, out_rows)
    assert OUTPUT_CSV.is_file()
    assert OUTPUT_CSV.suffix.lower() == ".csv"

    reread = read_csv_lines(OUTPUT_CSV)
    assert reread == out_rows

    parsed = list(csv.reader(StringIO("\n".join(out_rows))))
    assert len(parsed) == len(out_rows)
    assert all(len(rec) >= 2 for rec in parsed)
    assert len(parsed[1]) == 22

    assert out_rows[0] == rows[0]
    header_cells = cf.split_csv_row(out_rows[0])
    assert header_cells[COL_SALES] == "Sales"

    expected_sums = []
    for i in range(1, len(rows)):
        sales = float(cf.split_csv_row(rows[i])[COL_SALES])
        expected_sums.append(sales + delta)

    for i in range(1, len(out_rows)):
        cells = cf.split_csv_row(out_rows[i])
        assert float(cells[-1]) == pytest.approx(expected_sums[i - 1])

    assert float(cf.split_csv_row(out_rows[1])[-1]) == pytest.approx(311.96)


def test_addition_result_matches_manual_first_row() -> None:
    cf = _require_chunkflow_core()
    rows = _load_fixture_lines()
    first_data = rows[1]
    out = cf.apply_csv_row_math_scalar(
        first_data, "add", COL_SALES, 100.0, 21
    )
    cells = cf.split_csv_row(out)
    assert float(cells[COL_SALES]) == pytest.approx(261.96)
    assert float(cells[-1]) == pytest.approx(361.96)


def test_rejects_non_csv_extension(tmp_path: Path) -> None:
    p = tmp_path / "data.txt"
    p.write_text("a,b\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CSV only"):
        read_csv_lines(p)


if __name__ == "__main__":
    if chunkflow_core is None:
        print(
            "chunkflow_core failed to import.\n"
            "  Build:  pip install -e .\n"
            "  Windows: add MinGW bin (sqlite3.dll) to PATH or set CHUNKFLOW_MINGW_BIN.",
            file=sys.stderr,
        )
        sys.exit(1)

    in_csv = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else FIXTURE
    out_csv = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else OUTPUT_CSV

    _assert_csv_file(in_csv)
    _assert_csv_file(out_csv)

    out = _run_add_sales_write_csv(in_csv, out_csv, 50.0)
    print(f"CSV in:  {in_csv}")
    print(f"CSV out: {out_csv} ({len(out)} data lines including header)")