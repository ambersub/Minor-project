# 📖 ChunkFlow Complete Hands-On Tutorial

Welcome to the official step-by-step tutorial for **ChunkFlow**. This guide covers everything from installation and basic row-level CSV arithmetic to multi-delimiter parsing, row filtering, and building resilient, resumable chunked data pipelines.

---

## 📚 Table of Contents
1. [Installation & Build Verification](#1-installation--build-verification)
2. [Module 1: Basic CSV Row Arithmetic](#module-1-basic-csv-row-arithmetic)
3. [Module 2: Extended Math Operations (Unary & Power)](#module-2-extended-math-operations-unary--power)
4. [Module 3: Processing Custom Delimiters (TSV, Pipe, Semicolon)](#module-3-processing-custom-delimiters-tsv-pipe-semicolon)
5. [Module 4: Advanced Row-Level Filtering](#module-4-advanced-row-level-filtering)
6. [Module 5: Stateful Chunk Processing & Checkpoint Resumption](#module-5-stateful-chunk-processing--checkpoint-resumption)
7. [Module 6: Advanced Real-World Data Pipeline Example](#module-6-advanced-real-world-data-pipeline-example)
8. [Troubleshooting & Common Questions](#troubleshooting--common-questions)

---

## 1. Installation & Build Verification

Before running code examples, ensure you have Python 3.9+ and a C++17 compiler installed.

```bash
# Step 1: Install pybind11 requirement
pip install pybind11 >= 2.11

# Step 2: Clone repository and navigate to root directory
cd minor-project

# Step 3: Install chunkflow in editable mode (compiles C++ chunkflow_core extension)
pip install -e .
```

Verify the C++ core extension imported correctly:
```python
import chunkflow_core
print("ChunkFlow Extension loaded successfully!")
```

---

## Module 1: Basic CSV Row Arithmetic

`chunkflow.csv_math` provides fast, row-wise operations on numeric columns. Column indices are 0-based.

### 1.1 Binary Arithmetic Between Two Columns
Calculate operations between two numeric columns: `col_left op col_right → col_out`.

```python
from chunkflow import csv_math

# Sample CSV row: "Sales,Quantity,Profit"
row = "250.00,5,50.00"

# Add Sales (col 0) and Profit (col 2), append result as column 3
out_row = csv_math.apply_csv_row_math_binary(row, "add", 0, 2, 3)
print("Binary Addition:", out_row)
# Result: "250.00,5,50.00,300"

# Supported operation strings: "add", "sub", "mul", "div", "+", "-", "*", "/"
sub_row = csv_math.apply_csv_row_math_binary(row, "sub", 0, 2, 3)
print("Binary Subtraction:", sub_row)
# Result: "250.00,5,50.00,200"
```

### 1.2 Scalar Arithmetic On a Single Column
Apply a constant scalar value to a column: `col op scalar → col_out`.

```python
# Multiply Quantity (col 1) by scalar factor 1.10 (10% quantity surge), replace col 1
scaled_row = csv_math.apply_csv_row_math_scalar(row, "mul", 1, 1.10, 1)
print("Scalar Scaling:", scaled_row)
# Result: "250.00,5.5,50.00"
```

### 1.3 Multi-Threaded Batch Row Processing
Process lists of CSV strings across CPU cores using OpenMP:

```python
rows = [
    "Sales,Quantity",
    "100.00,2",
    "250.50,4",
    "500.00,10"
]

# Calculate Sales / Quantity for all rows, skipping the header (skip_header=True)
results = csv_math.apply_csv_rows_math_binary(
    rows, "div", 0, 1, 2, skip_header=True
)

for line in results:
    print(line)
# Output:
# Sales,Quantity
# 100.00,2,50
# 250.50,4,62.625
# 500.00,10,50
```

---

## Module 2: Extended Math Operations (Unary & Power)

ChunkFlow supports unary mathematical functions (`sqrt`, `abs`, `floor`, `ceil`) and exponential powers (`pow`).

```python
from chunkflow import csv_math

row = "16.0,-25.7,3"

# Square root of column 0 -> output column 3
sqrt_row = csv_math.apply_csv_row_math_unary(row, "sqrt", 0, 3)
print("Sqrt:", sqrt_row)
# Result: "16.0,-25.7,3,4"

# Absolute value of column 1 -> replace column 1
abs_row = csv_math.apply_csv_row_math_unary(row, "abs", 1, 1)
print("Abs:", abs_row)
# Result: "16.0,25.7,3"

# Power: Column 2 raised to exponent 3.0 (3^3 = 27)
pow_row = csv_math.apply_csv_row_math_power(row, 2, 3.0, 3)
print("Power:", pow_row)
# Result: "16.0,-25.7,3,27"
```

---

## Module 3: Processing Custom Delimiters (TSV, Pipe, Semicolon)

Process datasets using non-comma delimiters with quote handling support.

```python
from chunkflow import csv_math

# --- Tab-Separated Values (TSV) ---
tsv_row = "John Doe\tDeveloper\t75000"
fields = csv_math.split_delimited_row(tsv_row, "\t")
print("TSV Fields:", fields)
# Result: ['John Doe', 'Developer', '75000']

# Rejoin fields as Pipe-Delimited text
pipe_row = csv_math.join_delimited_row(fields, "|")
print("Pipe Row:", pipe_row)
# Result: "John Doe|Developer|75000"

# --- European Semicolon Format ---
semi_row = "2026-10-06;Widget A;199.99"
semi_fields = csv_math.split_delimited_row(semi_row, ";")
print("Semicolon Fields:", semi_fields)
# Result: ['2026-10-06', 'Widget A', '199.99']
```

---

## Module 4: Advanced Row-Level Filtering

Filter rows efficiently using string equality, numeric ranges, or custom Python predicates.

```python
from chunkflow import csv_math

rows = [
    "Alice,28,Engineering,85000",
    "Bob,35,Marketing,62000",
    "Charlie,42,Engineering,110000",
    "Diana,24,Support,48000"
]

# 1. Field Filter: Keep rows where Department (col 2) == "Engineering"
eng_rows = csv_math.filter_rows_by_field(rows, 2, "Engineering", delimiter=",")
print("Engineering Rows:", eng_rows)

# 2. Numeric Range Filter: Keep rows where Salary (col 3) is between 60,000 and 100,000
salary_rows = csv_math.filter_rows_by_range(rows, 3, 60000, 100000, delimiter=",")
print("Salary Filter:", salary_rows)

# 3. Custom Predicate Filter: Age (col 1) > 30 and Name starts with 'C'
def my_filter(row_str: str) -> bool:
    fields = csv_math.split_csv_row(row_str)
    return int(fields[1]) > 30 and fields[0].startswith("C")

predicate_rows = csv_math.filter_rows(rows, my_filter)
print("Predicate Filter:", predicate_rows)
```

---

## Module 5: Stateful Chunk Processing & Checkpoint Resumption

When working with large files, `chunkflow_core.process()` manages chunking, execution progress logging, and automatic job resumption.

```python
import chunkflow_core

# 1. Dataset records
records = [
    "101,15.5,2",
    "102,20.0,4",
    "103,30.2,5",
    "104,40.0,8",
    "105,50.1,10"
]

# 2. Define transform callback
def transform_row(row_str: str) -> str:
    fields = row_str.split(",")
    val_float = float(fields[1])
    val_int = int(fields[2])
    total = val_float * val_int
    return f"{row_str},{total:.2f}"

# 3. Run chunked execution
summary = chunkflow_core.process(
    records=records,
    transform_fn=transform_row,
    output_path="output_results.csv",
    log_path="run_progress.log",
    chunk_size=2,                # 2 records per chunk
    num_threads=4,               # 4 OpenMP threads
    checkpoint_path="run.checkpoint",
    resume=False                 # Fresh start
)

print(f"Chunks Done : {summary['done']}")
print(f"Elapsed     : {summary['elapsed_seconds']:.4f}s")
```

### Resuming Interrupted Jobs
If processing is interrupted (e.g. power failure or exception), re-run with `resume=True`:

```python
summary = chunkflow_core.process(
    records=records,
    transform_fn=transform_row,
    output_path="output_results.csv",
    log_path="run_progress.log",
    chunk_size=2,
    num_threads=4,
    checkpoint_path="run.checkpoint",
    resume=True                  # Automatically skips completed chunks
)
```

---

## Module 6: Advanced Real-World Data Pipeline Example

Combining mathematical transformations, string trimming, column concatenation, and file I/O into a complete pipeline.

```python
from pathlib import Path
import chunkflow_core
from chunkflow import csv_math

def process_sales_pipeline(input_file: Path, output_file: Path):
    # 1. Read input CSV lines
    lines = [ln.strip() for ln in input_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    
    # 2. Trim whitespace from customer names (col 0)
    lines = chunkflow_core.trim_csv_rows_column(lines, 0, skip_header=True)
    
    # 3. Concatenate First Name (col 0) and Last Name (col 1) with space -> output col 2
    lines = chunkflow_core.concat_csv_rows_columns(lines, 0, 1, " ", 2, skip_header=True)
    
    # 4. Calculate Total = Price (col 3) * Quantity (col 4) -> output col 5
    lines = csv_math.apply_csv_rows_math_binary(lines, "mul", 3, 4, 5, skip_header=True)
    
    # 5. Filter out orders where Total (col 5) is less than 50.0
    filtered_lines = [lines[0]] + csv_math.filter_rows_by_range(lines[1:], 5, 50.0, float("inf"), delimiter=",")
    
    # 6. Write final CSV result
    output_file.write_text("\n".join(filtered_lines) + "\n", encoding="utf-8")
    print(f"Pipeline complete! Output written to {output_file}")
```

---

## Troubleshooting & Common Questions

#### Q: I get a DLL load error when importing `chunkflow_core` on Windows.
**A**: Ensure your MinGW-w64 `bin` directory containing runtime DLLs (`libstdc++-6.dll`, `libgcc_s_seh-1.dll`) is in your system `PATH`, or set `CHUNKFLOW_MINGW_BIN`.

#### Q: How do I handle CSV files with headers in batch operations?
**A**: Pass `skip_header=True` to `apply_csv_rows_math_*` or `aggregate_csv_column` to keep line 0 unmodified.

#### Q: How do I avoid division by zero errors?
**A**: Filter out rows with zero denominators using `filter_rows_by_range(rows, col_denom, 0.0001, float('inf'))` prior to executing division math.
