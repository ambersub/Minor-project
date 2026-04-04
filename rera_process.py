"""
rera_process.py — RERA CSV via ChunkFlow (ChunkProcessor), export three CSVs.

Flow:
  1. Load RERA_Data_Output - Sheet1.csv.
  2. ChunkProcessor processes each row through ``process_rera_row`` (chunked run;
     SQLite is ChunkFlow's working store — see CHUNKFLOW_BACKEND in chunkflow).
  3. Read results from the DB, split into missing / clean / all.
  4. Write:
       - rera_sheet_missing.csv (column ``Missing columns``: fields that failed clean checks)
       - rera_sheet_clean.csv
       - rera_sheet_dates_split.csv (clean rows leave ``Missing columns`` empty)

Environment (chunkflow):
    CHUNKFLOW_BACKEND — auto | python | cpp (default auto: python on Windows)
    CHUNKFLOW_THREADS — passed as ChunkProcessor(num_threads=…); 0 = auto worker count
    CHUNKFLOW_MAX_WORKERS — hard cap / override for parallel chunk threads (Python backend)
    RERA_FORCE_REPROCESS — if 1, delete the ChunkFlow DB before running (fresh run)

Run:
    python rera_process.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

from chunkflow.chunking import ChunkProcessor


DATE_COL = "Project Start and Expiration date"
START_COL = "Start Date"
END_COL = "Expiry Date"
INDEX_COL = "_record_index"
MISSING_COLUMNS_COL = "Missing columns"

INPUT_CSV = "RERA_Data_Output - Sheet1.csv"
OUT_MISSING = "rera_sheet_missing.csv"
OUT_CLEAN = "rera_sheet_clean.csv"
OUT_SPLIT = "rera_sheet_dates_split.csv"

# Separate from older experiments: change name if you need a full re-run after editing the transform.
DB_PATH = "rera_chunkflow.db"
LOG_PATH = "rera_chunkflow.log"


def normalize_header(fieldnames: list[str] | None) -> list[str]:
    if not fieldnames:
        return []
    out = []
    for h in fieldnames:
        if h and h.startswith("daName of Developers"):
            out.append("Name of Developers")
        else:
            out.append(h or "")
    return out


def load_csv(filepath: Path) -> list[dict]:
    rows: list[dict] = []
    with filepath.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")
        reader.fieldnames = normalize_header(list(reader.fieldnames or []))
        for row in reader:
            rows.append(
                {k: (v or "").strip() if isinstance(v, str) else v for k, v in row.items()}
            )
    return rows


def split_date_range_j(date_str: str) -> tuple[str, str]:
    s = re.sub(r"\s+", " ", (date_str or "").strip())
    if " to " not in s:
        return "", ""
    parts = s.split(" 2 ", 1)
    start = parts[0].strip()
    end = parts[1].strip() if len(parts) > 1 else ""
    return start, end


def row_missing_contact(row: dict) -> bool:
    phone = (row.get("Contact Number") or "").strip()
    email = (row.get("Email id") or "").strip()
    return not (phone or email)


def row_is_clean(row: dict, start: str, end: str) -> bool:
    if row_missing_contact(row):
        return False
    if not start or not end:
        return False
    try:
        datetime.strptime(start, "%d-%m-%Y")
        datetime.strptime(end, "%d-%m-%Y")
    except ValueError:
        return False
    project = (row.get("Project Name") or "").strip()
    rera = (row.get("RERA Registration number") or "").strip()
    if not project or not rera:
        return False
    return True


def missing_data_columns_report(row: dict, start: str, end: str) -> str:
    """
    List source columns that fail the clean-data rules (semicolon-separated).
    Uses the same logic as row_is_clean.
    """
    issues: list[str] = []

    phone = (row.get("Contact Number") or "").strip()
    email = (row.get("Email id") or "").strip()
    if not phone and not email:
        issues.append("Contact Number")
        issues.append("Email id")

    date_raw = re.sub(r"\s+", " ", (row.get(DATE_COL) or "").strip())
    if " to " not in date_raw:
        issues.append(DATE_COL)
    elif not start or not end:
        issues.append(DATE_COL)
    else:
        try:
            datetime.strptime(start, "%d-%m-%Y")
        except ValueError:
            issues.append(START_COL)
        try:
            datetime.strptime(end, "%d-%m-%Y")
        except ValueError:
            issues.append(END_COL)

    if not (row.get("Project Name") or "").strip():
        issues.append("Project Name")
    if not (row.get("RERA Registration number") or "").strip():
        issues.append("RERA Registration number")

    seen: set[str] = set()
    ordered: list[str] = []
    for col in issues:
        if col not in seen:
            seen.add(col)
            ordered.append(col)
    return "; ".join(ordered)


def build_split_row(row: dict, start: str, end: str) -> dict:
    keys = [k for k in row.keys() if k and k != INDEX_COL]
    out: dict = {}
    for k in keys:
        if k == DATE_COL:
            continue
        out[k] = row.get(k, "")
    insert_after = "RERA Registration number"
    ordered: list[tuple[str, object]] = []
    inserted = False
    for k in keys:
        if k == DATE_COL:
            continue
        ordered.append((k, out[k]))
        if k == insert_after and not inserted:
            ordered.append((START_COL, start))
            ordered.append((END_COL, end))
            inserted = True
    if not inserted:
        ordered.append((START_COL, start))
        ordered.append((END_COL, end))
    return dict(ordered)


def process_rera_row(raw: str) -> str:
    """ChunkFlow transform: serialised JSON row in, JSON string out."""
    row = json.loads(raw)

    cleaned: dict = {}
    for key, val in row.items():
        if key == INDEX_COL:
            cleaned[key] = val
            continue
        if isinstance(val, str):
            val = re.sub(r"\s+", " ", val).strip()
        cleaned[key] = val

    rec_index = cleaned.pop(INDEX_COL, None)

    start, end = split_date_range_j(cleaned.get(DATE_COL, ""))
    split_row = build_split_row(cleaned, start, end)
    if rec_index is not None:
        split_row[INDEX_COL] = rec_index

    clean = row_is_clean(cleaned, start, end)
    split_row["Data Quality"] = "clean" if clean else "missing"
    if not clean:
        split_row[MISSING_COLUMNS_COL] = missing_data_columns_report(
            cleaned, start, end
        )
    return json.dumps(split_row, ensure_ascii=False)


def save_csv(filepath: Path, rows: list[dict]) -> None:
    if not rows:
        filepath.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k == INDEX_COL:
                continue
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    if MISSING_COLUMNS_COL in fieldnames:
        fieldnames = [k for k in fieldnames if k != MISSING_COLUMNS_COL]
        fieldnames.append(MISSING_COLUMNS_COL)
    with filepath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def safe_print(s: str) -> None:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    sys.stdout.write(s.encode(enc, "backslashreplace").decode(enc) + "\n")


def default_chunkflow_threads() -> int:
    """0 = ChunkFlow picks parallel chunk thread count (Python backend: ThreadPoolExecutor)."""
    if os.environ.get("CHUNKFLOW_THREADS") is not None:
        return int(os.environ["CHUNKFLOW_THREADS"])
    return 0


def main() -> None:
    base = Path(__file__).resolve().parent
    inp = base / INPUT_CSV

    db_file = base / DB_PATH
    if os.environ.get("RERA_FORCE_REPROCESS", "").lower() in ("1", "true", "yes"):
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(db_file) + suffix) if suffix else db_file
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        print("RERA_FORCE_REPROCESS: cleared ChunkFlow DB", flush=True)

    print(f"Loading {inp.name}...", flush=True)
    raw = load_csv(inp)
    print(f"Loaded {len(raw)} rows", flush=True)

    data: list[dict] = []
    for i, row in enumerate(raw):
        d = dict(row)
        d[INDEX_COL] = i
        data.append(d)

    backend = os.environ.get("CHUNKFLOW_BACKEND", "auto").strip() or "auto"
    threads = default_chunkflow_threads()
    cp = ChunkProcessor(
        db_path=str(base / DB_PATH),
        log_path=str(base / LOG_PATH),
        chunk_size=50,
        num_threads=threads,
    )

    print(
        f"ChunkFlow: backend env={backend!r}, num_threads={threads} "
        f"(0=auto parallel chunks), chunk_size=50",
        flush=True,
    )
    print("Processing...", flush=True)
    summary = cp.process(data, process_rera_row, serialise=json.dumps)
    print(
        "ChunkFlow summary: "
        f"chunks={summary.total_chunks} completed={summary.completed} "
        f"skipped={summary.skipped} failed={summary.failed} "
        f"elapsed={summary.elapsed_seconds:.2f}s",
        flush=True,
    )

    results: list[dict] = cp.read_results()
    results.sort(key=lambda r: int(r.get(INDEX_COL, 0)))

    split_full: list[dict] = []
    for r in results:
        r = dict(r)
        r.pop(INDEX_COL, None)
        split_full.append(r)

    missing_rows = [r for r in split_full if r.get("Data Quality") == "missing"]
    clean_rows = [r for r in split_full if r.get("Data Quality") == "clean"]

    out_m = base / OUT_MISSING
    out_c = base / OUT_CLEAN
    out_s = base / OUT_SPLIT

    save_csv(out_m, missing_rows)
    save_csv(out_c, clean_rows)
    save_csv(out_s, split_full)

    print(f"  Missing rows ({len(missing_rows)}): {out_m.name}", flush=True)
    print(f"  Clean rows   ({len(clean_rows)}): {out_c.name}", flush=True)
    print(f"  All + dates split ({len(split_full)}): {out_s.name}", flush=True)
    print(f"  ChunkFlow DB (working): {DB_PATH}", flush=True)
    print(f"  ChunkFlow log: {LOG_PATH}", flush=True)

    if summary.failed and not split_full:
        print(
            "Warning: chunks failed and no rows read back. See log / delete DB and retry.",
            file=sys.stderr,
        )

    if split_full:
        safe_print("\nSample (first row, dates split):")
        first = split_full[0]
        for k in list(first.keys())[:15]:
            v = first[k]
            s = str(v)
            safe_print(f"  {k}: {s[:120]}{'...' if len(s) > 120 else ''}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
