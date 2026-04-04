"""
Pure-Python CSV split/join and row-wise math (mirrors chunkflow_core C++ helpers).
Used when the extension is missing or fails to load (e.g. DLL errors on Windows).
"""

from __future__ import annotations


def _trim(s: str) -> str:
    return s.strip()


def split_csv_row(line: str) -> list[str]:
    out: list[str] = []
    cur: list[str] = []
    in_quotes = False
    i = 0
    n = len(line)
    while i < n:
        c = line[i]
        if in_quotes:
            if c == '"':
                if i + 1 < n and line[i + 1] == '"':
                    cur.append('"')
                    i += 2
                    continue
                in_quotes = False
                i += 1
                continue
            cur.append(c)
            i += 1
        else:
            if c == '"':
                in_quotes = True
                i += 1
            elif c == ",":
                out.append(_trim("".join(cur)))
                cur = []
                i += 1
            else:
                cur.append(c)
                i += 1
    out.append(_trim("".join(cur)))
    return out


def _field_needs_quotes(f: str) -> bool:
    return any(ch in f for ch in ',"\r\n')


def join_csv_row(fields: list[str]) -> str:
    parts: list[str] = []
    for f in fields:
        if _field_needs_quotes(f):
            inner = f.replace('"', '""')
            parts.append(f'"{inner}"')
        else:
            parts.append(f)
    return ",".join(parts)


def _parse_double_field(s: str) -> float | None:
    if not s:
        return None
    t = s.strip()
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_op_binary(op: str) -> str:
    k = op.strip().lower()
    if k in ("add", "+"):
        return "add"
    if k in ("sub", "subtract", "-"):
        return "sub"
    if k in ("mul", "multiply", "*"):
        return "mul"
    if k in ("div", "divide", "/"):
        return "div"
    raise ValueError(
        f"unknown operation: {op!r} (use add, sub, mul, div or + - * /)"
    )


def _parse_op_scalar(op: str) -> str:
    return _parse_op_binary(op)


def _apply_binary(kind: str, a: float, b: float) -> float:
    if kind == "add":
        return a + b
    if kind == "sub":
        return a - b
    if kind == "mul":
        return a * b
    if kind == "div":
        if b == 0.0:
            raise ValueError("division by zero")
        return a / b
    raise ValueError(kind)


def _apply_scalar(kind: str, value: float, scalar: float) -> float:
    if kind == "add":
        return value + scalar
    if kind == "sub":
        return value - scalar
    if kind == "mul":
        return value * scalar
    if kind == "div":
        if scalar == 0.0:
            raise ValueError("division by zero")
        return value / scalar
    raise ValueError(kind)


def _format_double_cell(v: float) -> str:
    return format(v, ".17g")


def apply_csv_row_math_binary(
    row: str,
    operation: str,
    col_left: int,
    col_right: int,
    col_out: int,
) -> str:
    if col_left < 0 or col_right < 0 or col_out < 0:
        raise ValueError("column indices must be non-negative")

    fields = split_csv_row(row)
    if col_left >= len(fields) or col_right >= len(fields):
        raise ValueError("column index out of range for CSV row")

    a = _parse_double_field(fields[col_left])
    b = _parse_double_field(fields[col_right])
    if a is None or b is None:
        raise ValueError("non-numeric value in selected column(s)")

    result = _apply_binary(_parse_op_binary(operation), a, b)
    if col_out > len(fields):
        raise ValueError("col_out beyond one-past-last column index")

    cell = _format_double_cell(result)
    if col_out == len(fields):
        fields.append(cell)
    else:
        fields[col_out] = cell
    return join_csv_row(fields)


def apply_csv_row_math_scalar(
    row: str,
    operation: str,
    col: int,
    scalar: float,
    col_out: int,
) -> str:
    if col < 0 or col_out < 0:
        raise ValueError("column indices must be non-negative")

    fields = split_csv_row(row)
    if col >= len(fields):
        raise ValueError("column index out of range for CSV row")

    v = _parse_double_field(fields[col])
    if v is None:
        raise ValueError("non-numeric value in selected column")

    result = _apply_scalar(_parse_op_scalar(operation), v, scalar)
    if col_out > len(fields):
        raise ValueError("col_out beyond one-past-last column index")

    cell = _format_double_cell(result)
    if col_out == len(fields):
        fields.append(cell)
    else:
        fields[col_out] = cell
    return join_csv_row(fields)


def apply_csv_rows_math_binary(
    rows: list[str],
    operation: str,
    col_left: int,
    col_right: int,
    col_out: int,
    skip_header: bool = False,
) -> list[str]:
    out: list[str] = []
    for i, row in enumerate(rows):
        if skip_header and i == 0:
            out.append(row)
        else:
            out.append(
                apply_csv_row_math_binary(
                    row, operation, col_left, col_right, col_out
                )
            )
    return out


def apply_csv_rows_math_scalar(
    rows: list[str],
    operation: str,
    col: int,
    scalar: float,
    col_out: int,
    skip_header: bool = False,
) -> list[str]:
    out: list[str] = []
    for i, row in enumerate(rows):
        if skip_header and i == 0:
            out.append(row)
        else:
            out.append(
                apply_csv_row_math_scalar(row, operation, col, scalar, col_out)
            )
    return out
