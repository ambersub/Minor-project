"""
example.py — demonstrates chunkflow with advanced features.

Showcases:
  - Basic arithmetic operations (add, sub, mul, div)
  - Extended math operations (sqrt, abs, power, floor, ceil)
  - Delimiter-based processing (CSV, TSV, pipe-delimited, etc.)
  - Row-level filtering
  - Custom logic with transform functions

Run after building the extension:
    pip install -e .
    python example.py
"""

import json
import math
from chunkflow.chunking import ChunkProcessor
from chunkflow import csv_math


# ============================================================================
# 1. BASIC TRANSFORM + CUSTOM LOGIC
# ============================================================================

def enrich_record(raw: str) -> str:
    """Parse JSON, compute values, return JSON."""
    rec = json.loads(raw)
    rec["sqrt_value"] = math.sqrt(abs(rec["value"]))
    rec["category"]   = "even" if rec["id"] % 2 == 0 else "odd"
    rec["label"]      = f"item-{rec['id']:06d}"
    return json.dumps(rec)


# ============================================================================
# 2. CSV MATH OPERATIONS - Row-level processing
# ============================================================================

def demo_csv_math_operations():
    """Demonstrate arithmetic operations on CSV rows."""
    print("\n=== CSV Math Operations ===\n")

    # Example CSV row: "10,20,label"
    row = "10,20,label"
    
    # Binary operations (col1 op col2, store in col_out)
    print(f"Original:    {row}")
    print(f"Add cols:    {csv_math.apply_csv_row_math_binary(row, 'add', 0, 1, 3)}")
    print(f"Multiply:    {csv_math.apply_csv_row_math_binary(row, 'mul', 0, 1, 3)}")
    
    # Scalar operations (col op scalar, store in col_out)
    row2 = "5.0,100,test"
    print(f"\nScalar ops on {row2}:")
    print(f"Col0 * 2:    {csv_math.apply_csv_row_math_scalar(row2, 'mul', 0, 2.0, 3)}")
    print(f"Col1 / 10:   {csv_math.apply_csv_row_math_scalar(row2, 'div', 1, 10.0, 3)}")


def demo_extended_math_operations():
    """Demonstrate extended math operations (sqrt, abs, floor, ceil, power)."""
    print("\n=== Extended Math Operations ===\n")

    row = "16.5,25.0,test"
    
    # Unary operations
    print(f"Original:       {row}")
    print(f"Sqrt col 1:     {csv_math.apply_csv_row_math_unary(row, 'sqrt', 1, 3)}")
    print(f"Abs col 0:      {csv_math.apply_csv_row_math_unary(row, 'abs', 0, 3)}")
    print(f"Floor col 0:    {csv_math.apply_csv_row_math_unary(row, 'floor', 0, 3)}")
    print(f"Ceil col 0:     {csv_math.apply_csv_row_math_unary(row, 'ceil', 0, 3)}")
    
    # Power operation
    print(f"Col 1 ^ 3:      {csv_math.apply_csv_row_math_power(row, 1, 3.0, 3)}")
    print(f"Col 0 ^ 0.5:    {csv_math.apply_csv_row_math_power(row, 0, 0.5, 3)}")


def demo_delimiter_processing():
    """Demonstrate delimiter-based processing (not just CSV)."""
    print("\n=== Delimiter-Based Processing ===\n")

    # CSV (comma-delimited)
    csv_row = "John,30,Engineer"
    fields = csv_math.split_csv_row(csv_row)
    print(f"CSV row:      {csv_row}")
    print(f"Parsed:       {fields}")
    print(f"Rejoined:     {csv_math.join_csv_row(fields)}\n")

    # TSV (tab-delimited)
    tsv_row = "John\t30\tEngineer"
    fields = csv_math.split_delimited_row(tsv_row, "\t")
    print(f"TSV row:      {repr(tsv_row)}")
    print(f"Parsed:       {fields}")
    print(f"Rejoined:     {csv_math.join_delimited_row(fields, '\t')}\n")

    # Pipe-delimited
    pipe_row = "John|30|Engineer"
    fields = csv_math.split_delimited_row(pipe_row, "|")
    print(f"Pipe row:     {pipe_row}")
    print(f"Parsed:       {fields}")
    print(f"Rejoined:     {csv_math.join_delimited_row(fields, '|')}\n")

    # Semicolon-delimited
    semi_row = "John;30;Engineer"
    fields = csv_math.split_delimited_row(semi_row, ";")
    print(f"Semicolon:    {semi_row}")
    print(f"Parsed:       {fields}")
    print(f"Rejoined:     {csv_math.join_delimited_row(fields, ';')}")


def demo_filtering():
    """Demonstrate row-level filtering."""
    print("\n=== Row-Level Filtering ===\n")

    # Sample rows: "name,age,salary"
    rows = [
        "Alice,25,50000",
        "Bob,35,60000",
        "Charlie,28,55000",
        "Diana,32,70000",
        "Eve,22,45000",
    ]

    print("Original rows:")
    for r in rows:
        print(f"  {r}")

    # Filter by field value (age == 28)
    print("\nFilter by field (age == 28):")
    filtered = csv_math.filter_rows_by_field(rows, 1, "28", delimiter=",")
    for r in filtered:
        print(f"  {r}")

    # Filter by numeric range (salary >= 55000)
    print("\nFilter by range (salary >= 55000 and <= 70000):")
    filtered = csv_math.filter_rows_by_range(rows, 2, 55000, 70000, delimiter=",")
    for r in filtered:
        print(f"  {r}")

    # Custom predicate (names starting with 'A' or 'D')
    print("\nFilter with custom predicate (names start with A or D):")
    def name_starts_ad(row: str) -> bool:
        fields = csv_math.split_csv_row(row)
        return len(fields) > 0 and fields[0][0] in "AD"

    filtered = csv_math.filter_rows(rows, name_starts_ad)
    for r in filtered:
        print(f"  {r}")


# ============================================================================
# 3. CHUNKED PROCESSING WITH CUSTOM LOGIC
# ============================================================================

def demo_chunked_processing():
    """Demonstrate chunked parallel processing."""
    print("\n=== Chunked Processing ===\n")

    # Generate sample dataset
    dataset = [{"id": i, "value": i * 7 - 3} for i in range(100)]

    # Configure and run processor
    cp = ChunkProcessor(
        db_path="demo_results.db",
        log_path="demo_run.log",
        chunk_size=25,
        num_threads=0,
    )

    print("Processing 100 records in chunks of 25...")
    summary = cp.process(dataset, enrich_record)
    print(f"\n{summary}")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("ChunkFlow Advanced Features Demo")
    print("=" * 70)

    # Run demonstrations
    demo_csv_math_operations()
    demo_extended_math_operations()
    demo_delimiter_processing()
    demo_filtering()
    demo_chunked_processing()

    print("\n" + "=" * 70)
    print("Demo complete! Check demo_results.db and demo_run.log for outputs.")
    print("=" * 70)

