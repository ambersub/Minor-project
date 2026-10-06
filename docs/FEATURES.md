# ChunkFlow Advanced Features

## Overview

ChunkFlow now supports **row-level processing** with:
- ✅ Basic arithmetic operations
- ✅ Extended math functions
- ✅ Delimiter-based processing (CSV, TSV, pipe, semicolon, etc.)
- ✅ Advanced filtering capabilities
- ✅ Custom logic handling

---

## 1. Basic Arithmetic Operations

### Binary Operations
Apply operations between two columns: `col1 op col2 → col_out`

```python
from chunkflow import csv_math

row = "10,20,label"

# Add columns
result = csv_math.apply_csv_row_math_binary(row, "add", 0, 1, 3)
# Result: "10,20,label,30"

# Supported operations: add, +, sub, -, mul, *, div, /
result = csv_math.apply_csv_row_math_binary(row, "mul", 0, 1, 3)
# Result: "10,20,label,200"
```

**Parameters:**
- `row` (str): CSV row string
- `op` (str): Operation - "add" | "sub" | "mul" | "div" | "+" | "-" | "*" | "/"
- `col_left` (int): First column index (0-based)
- `col_right` (int): Second column index (0-based)
- `col_out` (int): Output column index (append if equals field count)

### Scalar Operations
Apply operation to one column with a scalar: `col op scalar → col_out`

```python
row = "5.0,100,test"

# Multiply column by scalar
result = csv_math.apply_csv_row_math_scalar(row, "mul", 0, 2.0, 3)
# Result: "5.0,100,test,10.0"

# Divide column by scalar
result = csv_math.apply_csv_row_math_scalar(row, "div", 1, 10.0, 3)
# Result: "5.0,100,test,10.0"
```

**Batch Operations:**
```python
rows = ["10,20", "30,40", "50,60"]

# Apply to all rows (skip header if needed)
results = csv_math.apply_csv_rows_math_binary(
    rows, "add", 0, 1, 2, skip_header=False
)
# Results: ["10,20,30", "30,40,70", "50,60,110"]
```

---

## 2. Extended Math Operations

### Unary Functions
Apply single-column operations: `op(col) → col_out`

```python
row = "16.5,25.0,test"

# Square root
result = csv_math.apply_csv_row_math_unary(row, "sqrt", 1, 3)
# Result: "16.5,25.0,test,5.0" (sqrt(25) = 5)

# Absolute value
result = csv_math.apply_csv_row_math_unary(row, "abs", 0, 3)

# Floor function
result = csv_math.apply_csv_row_math_unary(row, "floor", 0, 3)

# Ceiling function
result = csv_math.apply_csv_row_math_unary(row, "ceil", 0, 3)
```

**Supported Operations:** `sqrt` | `abs` | `floor` | `ceil`

### Power Operation
Raise a column to a power: `col ^ exponent → col_out`

```python
row = "2.0,5.0,test"

# Square: 5^2 = 25
result = csv_math.apply_csv_row_math_power(row, 1, 2.0, 3)
# Result: "2.0,5.0,test,25.0"

# Square root via power: 25^0.5 = 5
result = csv_math.apply_csv_row_math_power(row, 1, 0.5, 3)
```

### Batch Extended Operations
```python
rows = ["16.5", "25.0", "36.0"]

# Apply sqrt to all rows
results = csv_math.apply_csv_rows_math_unary(
    rows, "sqrt", 0, 1, skip_header=False
)
# Results: ["16.5,4.06...", "25.0,5.0", "36.0,6.0"]
```

---

## 3. Delimiter-Based Processing

Parse and process rows with any delimiter, not just commas.

### Split by Custom Delimiter
```python
from chunkflow import csv_math

# CSV (default comma)
csv_row = "John,30,Engineer"
fields = csv_math.split_csv_row(csv_row)
# Result: ["John", "30", "Engineer"]

# TSV (tab-delimited)
tsv_row = "John\t30\tEngineer"
fields = csv_math.split_delimited_row(tsv_row, "\t")
# Result: ["John", "30", "Engineer"]

# Pipe-delimited
pipe_row = "John|30|Engineer"
fields = csv_math.split_delimited_row(pipe_row, "|")
# Result: ["John", "30", "Engineer"]

# Semicolon-delimited
semi_row = "John;30;Engineer"
fields = csv_math.split_delimited_row(semi_row, ";")
# Result: ["John", "30", "Engineer"]
```

### Join with Delimiter
```python
fields = ["John", "30", "Engineer"]

# Rejoin with different delimiters
csv_out = csv_math.join_delimited_row(fields, ",")
tsv_out = csv_math.join_delimited_row(fields, "\t")
pipe_out = csv_math.join_delimited_row(fields, "|")

# Result: "John,30,Engineer" | "John\t30\tEngineer" | "John|30|Engineer"
```

### Quote Support
```python
# Enable quote handling for delimiters
fields = csv_math.split_delimited_row(row, ",", use_quotes=True)
```

---

## 4. Row-Level Filtering

Filter rows based on conditions.

### Filter by Field Value
Keep rows where a column equals a specific value.

```python
rows = [
    "Alice,25,50000",
    "Bob,35,60000",
    "Charlie,28,55000",
    "Diana,32,70000",
    "Eve,22,45000",
]

# Filter where age (col 1) == "28"
filtered = csv_math.filter_rows_by_field(rows, 1, "28", delimiter=",")
# Result: ["Charlie,28,55000"]

# Filter by name
filtered = csv_math.filter_rows_by_field(rows, 0, "Alice", delimiter=",")
# Result: ["Alice,25,50000"]
```

### Filter by Numeric Range
Keep rows where a numeric column is within a range.

```python
# Filter where salary (col 2) >= 55000 and <= 70000
filtered = csv_math.filter_rows_by_range(rows, 2, 55000, 70000, delimiter=",")
# Result: ["Charlie,28,55000", "Diana,32,70000"]

# Filter where age (col 1) <= 30
filtered = csv_math.filter_rows_by_range(rows, 1, 0, 30, delimiter=",")
# Result: ["Alice,25,50000", "Charlie,28,55000", "Eve,22,45000"]
```

### Custom Filtering with Predicate
Use a Python function for complex filtering logic.

```python
# Filter names starting with 'A' or 'D'
def name_filter(row: str) -> bool:
    fields = csv_math.split_csv_row(row)
    return len(fields) > 0 and fields[0][0] in "AD"

filtered = csv_math.filter_rows(rows, name_filter)
# Result: ["Alice,25,50000", "Diana,32,70000"]

# Complex condition: age >= 28 AND name contains 'i'
def complex_filter(row: str) -> bool:
    fields = csv_math.split_csv_row(row)
    if len(fields) < 2:
        return False
    try:
        age = float(fields[1])
        name = fields[0]
        return age >= 28 and 'i' in name.lower()
    except:
        return False

filtered = csv_math.filter_rows(rows, complex_filter)
# Result: ["Charlie,28,55000", "Diana,32,70000"]
```

---

## 5. Custom Logic with Transforms

Combine the above features in custom transform functions.

### Row-Level Transform Example
```python
from chunkflow.chunking import ChunkProcessor
from chunkflow import csv_math
import json

def custom_transform(row_str: str) -> str:
    """Complex row transformation."""
    data = json.loads(row_str)
    
    # Extract values
    col_a = float(data["value_a"])
    col_b = float(data["value_b"])
    
    # Apply math operations
    result = col_a + col_b
    result_sqrt = result ** 0.5
    result_abs = abs(result)
    
    # Add computed values
    data["sum"] = result
    data["sqrt_sum"] = result_sqrt
    data["abs_sum"] = result_abs
    
    # Filter condition (skip if result < 10)
    if result < 10:
        return ""  # Empty string means skip
    
    return json.dumps(data)

# Use with chunked processor
cp = ChunkProcessor(chunk_size=500)
summary = cp.process(dataset, custom_transform)
```

### Chained Operations
```python
# Parse row
fields = csv_math.split_csv_row(row)

# Apply math
row_with_sum = csv_math.apply_csv_row_math_binary(row, "add", 0, 1, 2)

# Apply sqrt
row_with_sqrt = csv_math.apply_csv_row_math_unary(row_with_sum, "sqrt", 2, 3)

# Convert to different delimiter
fields = csv_math.split_csv_row(row_with_sqrt)
result = csv_math.join_delimited_row(fields, "\t")
```

---

## 6. Practical Examples

### Data Enrichment Pipeline
```python
def enrich_sales_data(row: str) -> str:
    """Enrich sales records with calculated fields."""
    # Parse: quantity,unit_price,discount_percent
    row = csv_math.apply_csv_row_math_binary(row, "mul", 0, 1, 3)  # subtotal
    row = csv_math.apply_csv_row_math_scalar(row, "mul", 3, 0.01, 4)  # discount amount
    row = csv_math.apply_csv_row_math_binary(row, "sub", 3, 4, 5)  # total
    return row

# Input: "10,25.50,20" (qty, price, discount%)
# Output: "10,25.50,20,255.0,51.0,204.0" (adds subtotal, discount, total)
```

### Statistical Aggregation
```python
def normalize_values(row: str) -> str:
    """Normalize numeric columns to 0-1 range."""
    row = csv_math.apply_csv_row_math_unary(row, "sqrt", 0, 2)  # sqrt
    row = csv_math.apply_csv_row_math_scalar(row, "div", 2, 100.0, 2)  # scale
    row = csv_math.apply_csv_row_math_unary(row, "floor", 2, 2)  # quantize
    return row
```

### Data Quality Filtering
```python
# Filter invalid records
rows = dataset  # Your data
rows = csv_math.filter_rows_by_range(rows, 0, 0, 1000000, ",")  # Reasonable IDs
rows = csv_math.filter_rows_by_range(rows, 2, -1000, 1000000, ",")  # Valid amounts
rows = csv_math.filter_rows_by_field(rows, 3, "ACTIVE", ",")  # Active status

# Continue processing filtered rows
```

---

## API Reference

### Math Operations
| Function | Purpose | Signature |
|----------|---------|-----------|
| `apply_csv_row_math_binary` | bin op on 2 cols | `(row, op, col_l, col_r, col_out) → str` |
| `apply_csv_row_math_scalar` | col op scalar | `(row, op, col, scalar, col_out) → str` |
| `apply_csv_row_math_unary` | unary op on col | `(row, op, col, col_out) → str` |
| `apply_csv_row_math_power` | col^exponent | `(row, col, exp, col_out) → str` |

### Batch Operations
| Function | Purpose | Signature |
|----------|---------|-----------|
| `apply_csv_rows_math_binary` | binary op on list | `(rows, op, col_l, col_r, col_out, skip_header) → list[str]` |
| `apply_csv_rows_math_scalar` | scalar op on list | `(rows, op, col, scalar, col_out, skip_header) → list[str]` |
| `apply_csv_rows_math_unary` | unary op on list | `(rows, op, col, col_out, skip_header) → list[str]` |

### Parsing & Formatting
| Function | Purpose | Signature |
|----------|---------|-----------|
| `split_csv_row` | Parse CSV | `(line) → list[str]` |
| `join_csv_row` | Format CSV | `(fields) → str` |
| `split_delimited_row` | Parse with delimiter | `(line, delimiter, use_quotes) → list[str]` |
| `join_delimited_row` | Format with delimiter | `(fields, delimiter) → str` |

### Filtering
| Function | Purpose | Signature |
|----------|---------|-----------|
| `filter_rows` | Custom predicate | `(rows, predicate_fn) → list[str]` |
| `filter_rows_by_field` | Exact match | `(rows, col, value, delimiter) → list[str]` |
| `filter_rows_by_range` | Numeric range | `(rows, col, min, max, delimiter) → list[str]` |

---

## Performance Tips

1. **Batch operations** for lists of rows (vs row-by-row)
2. **Use filtering early** to reduce downstream processing
3. **Combine operations** within transforms to minimize passes
4. **Leverage chunked processing** for parallel execution
5. **Choose appropriate delimiter** for your data format

---

## Building the Extension

```bash
pip install -e .
```

This compiles the C++ code with pybind11 and installs the module.

---

## Troubleshooting

- **Division by zero**: Check denominator values before operations
- **Invalid column index**: Ensure index < field count
- **Type errors**: Numeric operations require valid numbers
- **Delimiter issues**: Ensure delimiter matches your data format

