# 📁 ChunkFlow File Directory & Module Reference

This document provides a comprehensive, file-by-file reference for every file and directory in the **ChunkFlow** codebase.

---

## 🗂️ Complete Directory Map

```
minor-project/
├── chunkflow/                             # 🐍 Python Package Root
│   ├── __init__.py                        # Top-level package exports & API scope
│   ├── chunking.py                        # ChunkProcessor class & RunSummary metadata container
│   └── csv_math.py                        # Python bindings to chunkflow_core C++ extension
│
├── src/                                   # ⚡ Native C++ Extension Core
│   └── chunkflow_core.cpp                 # C++17 parsing engine, OpenMP kernels, & pybind11 module
│
├── tests/                                 # 🧪 Comprehensive Test Harness
│   ├── fixtures/
│   │   └── superstore_sample.csv          # Sample Superstore dataset fixture (21 columns)
│   ├── test_100k_chunkflow.py             # 100k dataset benchmark using ChunkFlow C++ core
│   ├── test_100k_multiprocessing.py       # 100k dataset benchmark using multiprocessing.Pool
│   ├── test_chunkflow_features.py         # Pytest unit tests for unary, concat, trim & checkpointing
│   ├── test_chunkflow_math.py             # Pytest unit tests for basic CSV binary & scalar math
│   ├── test_dataset_threading_checkpoint.py # Resilient multi-threading & resume checkpoint test
│   └── test_superstore_sample_addition.py  # Integration test reading/writing physical CSV files
│
├── docs/                                  # 📚 Project Documentation Suite
│   ├── PROJECT_FLOW.md                    # In-depth architectural & execution flow documentation
│   ├── FILE_DIRECTORY.md                  # Detailed directory & file reference (This document)
│   ├── TECH_STACK.md                      # Complete technology stack & compiler breakdown
│   └── TUTORIAL.md                        # Step-by-step user guide & API tutorial
│
├── datasetgen.py                          # Synthetic CSV dataset generator (100k / 1M records)
├── setup.py                               # Setuptools C++ extension build script with OpenMP flags
├── setup.cfg                              # Package configuration metadata fallback
├── pyproject.toml                         # PEP 517 build system setup specification
├── ENHANCEMENTS.md                        # Summary of added features (unary math, delimiters, filtering)
├── FEATURES.md                            # High-level API feature documentation and code examples
├── README_100k_benchmark.md               # Benchmark results comparing ChunkFlow vs Multiprocessing
└── README.md                              # Main repository landing page & executive summary
```

---

## 📄 Exhaustive File Specifications

### 1. Python Application Package (`chunkflow/`)

#### 🔹 [`chunkflow/__init__.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/__init__.py)
* **Lines of Code**: ~40 lines
* **Purpose**: Primary module initialization file defining the public API interface of `chunkflow`.
* **Key Exports**:
  - `split_csv_row`: Splits a single comma-separated CSV string.
  - `join_csv_row`: Joins string lists into an RFC4180 CSV line.
  - `apply_csv_row_math_binary` & `apply_csv_row_math_scalar`: Row-level binary/scalar arithmetic.
  - `apply_csv_rows_math_binary` & `apply_csv_rows_math_scalar`: Batch row list arithmetic.
  - `csv_math_backend`: String indicating the underlying engine (defaults to `"cpp"`).
* **Dependencies**: Imports from `chunkflow.csv_math`.

#### 🔹 [`chunkflow/csv_math.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/csv_math.py)
* **Lines of Code**: ~56 lines
* **Purpose**: Python binding layer delegating math, parsing, and filtering directly to `chunkflow_core`.
* **Key Functions Bound**:
  - **Parsing**: `split_csv_row`, `join_csv_row`, `split_delimited_row`, `join_delimited_row`.
  - **Basic Arithmetic**: `apply_csv_row_math_binary`, `apply_csv_row_math_scalar`, `apply_csv_rows_math_binary`, `apply_csv_rows_math_scalar`.
  - **Extended Arithmetic**: `apply_csv_row_math_unary`, `apply_csv_rows_math_unary`, `apply_csv_row_math_power`.
  - **Row Filtering**: `filter_rows` (predicate), `filter_rows_by_field` (exact string match), `filter_rows_by_range` (numeric range).
* **Dependencies**: `import chunkflow_core as _core`.

#### 🔹 [`chunkflow/chunking.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/chunking.py)
* **Lines of Code**: ~154 lines
* **Purpose**: Provides Python class abstractions for dataset processing with statistical metrics.
* **Key Components**:
  - `@dataclass class RunSummary`: Stores execution stats (`db_path`, `log_path`, `total_chunks`, `completed`, `skipped`, `failed`, `elapsed_seconds`) and calculates `success_rate`.
  - `class ChunkProcessor`: High-level manager configuring chunk sizes (`chunk_size=500`), thread counts (`num_threads=0`), and processing callbacks via `process()`.

---

### 2. C++ Extension Core (`src/`)

#### 🔹 [`src/chunkflow_core.cpp`](file:///c:/Users/Amber/Desktop/minor-project/src/chunkflow_core.cpp)
* **Lines of Code**: ~1079 lines
* **Purpose**: The main engine of ChunkFlow written in native C++17.
* **Key Sections & Algorithms**:
  - **CSV Parser Engine** (`split_csv_row`, `join_csv_row`, `split_delimited_row`, `join_delimited_row`): Performs character-by-character tokenization, RFC4180 quotation checks, quote escaping (`""`), and whitespace trimming (`trim_inplace`).
  - **Double Parsing & Formatting** (`parse_double_field`, `format_double_cell`): High-precision string-to-double parsing using `std::stod` and precision formatting via `std::ostringstream`.
  - **Math Enums & Functions** (`MathOpBinary`, `MathOpScalar`, `MathOpUnary`): Implementation of `apply_binary()`, `apply_scalar()`, `apply_unary()`, and `apply_csv_row_math_power()`.
  - **OpenMP Parallel Kernels** (`apply_csv_rows_math_*`): `#pragma omp parallel for` multi-threaded execution across row vectors with critical section exception safety.
  - **Filtering Engine** (`filter_rows`, `filter_rows_by_field`, `filter_rows_by_range`): Evaluates predicate lambdas via GIL acquisition or conducts fast numeric range and field checks directly in C++.
  - **Logger Class** (`Logger`): Thread-safe file logger formatting messages with `[YYYY-MM-DD HH:MM:SS] [LEVEL]` timestamps.
  - **Core Process Function** (`process()`): Splits input records into chunks, handles optional checkpoint resume state checking (`checkpoint_path`), releases/acquires Python GIL, and streams outputs to disk.
  - **Pybind11 Module Definitions** (`PYBIND11_MODULE(chunkflow_core, m)`): Exposes all C++ functions directly to Python.

---

### 3. Build & Packaging Infrastructure

#### 🔹 [`setup.py`](file:///c:/Users/Amber/Desktop/minor-project/setup.py)
* **Lines of Code**: ~90 lines
* **Purpose**: Configures pybind11 compilation flags, platform libraries, and OpenMP linking.
* **Compiler Flags Setup**:
  - **Windows (MinGW)**: `-O3`, `-std=c++17`, `-fopenmp`, `-DMS_WIN64`, `-D_hypot=hypot`, `-static-libgcc`, `-static-libstdc++`.
  - **macOS**: `-O3`, `-std=c++17`, `-Xpreprocessor`, `-fopenmp`, `-lomp`.
  - **Linux (GCC)**: `-O3`, `-std=c++17`, `-fopenmp`.

#### 🔹 [`pyproject.toml`](file:///c:/Users/Amber/Desktop/minor-project/pyproject.toml)
* **Purpose**: PEP 517 build system configuration specifying `setuptools` and `pybind11>=2.11` as build dependencies.

#### 🔹 [`setup.cfg`](file:///c:/Users/Amber/Desktop/minor-project/setup.cfg)
* **Purpose**: Package distribution configuration fallback options.

---

### 4. Utilities & Benchmarks

#### 🔹 [`datasetgen.py`](file:///c:/Users/Amber/Desktop/minor-project/datasetgen.py)
* **Purpose**: Generates synthetic benchmark CSV datasets (`chunkflow_test_100k.csv`) containing 100,000 to 1,000,000 rows with columns: `id`, `val_float`, `val_int`, `category`, `label`.

#### 🔹 [`README_100k_benchmark.md`](file:///c:/Users/Amber/Desktop/minor-project/README_100k_benchmark.md)
* **Purpose**: Documents benchmark methodology and performance comparisons demonstrating **~2.64x speedup** of ChunkFlow vs standard Python `multiprocessing`.

---

### 5. Test Harness (`tests/`)

#### 🔹 [`tests/fixtures/superstore_sample.csv`](file:///c:/Users/Amber/Desktop/minor-project/tests/fixtures/superstore_sample.csv)
* **Purpose**: Realistic 21-column Superstore CSV dataset containing string titles, dates, numbers, quotes, and commas used across unit tests.

#### 🔹 [`tests/test_chunkflow_math.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_chunkflow_math.py)
* **Purpose**: Unit tests verifying basic binary addition, subtraction, division, scalar scaling, quoted cell preservation, and zero-division error raising.

#### 🔹 [`tests/test_chunkflow_features.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_chunkflow_features.py)
* **Purpose**: Unit tests verifying batch math, column aggregation (`sum`, `average`), column concatenation, whitespace trimming, and checkpoint recovery.

#### 🔹 [`tests/test_dataset_threading_checkpoint.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_dataset_threading_checkpoint.py)
* **Purpose**: Multi-threaded integration test verifying chunk recovery when resuming an intentionally interrupted job.

#### 🔹 [`tests/test_superstore_sample_addition.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_superstore_sample_addition.py)
* **Purpose**: End-to-end file integration test reading physical CSV files, executing `chunkflow_core` transformations, and verifying resulting CSV output files.

#### 🔹 [`tests/test_100k_chunkflow.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_100k_chunkflow.py)
* **Purpose**: Benchmark script timing `chunkflow_core.process()` over 100,000 CSV rows using 4 OpenMP threads.

#### 🔹 [`tests/test_100k_multiprocessing.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_100k_multiprocessing.py)
* **Purpose**: Benchmark comparison script timing standard Python `multiprocessing.Pool` over 100,000 CSV rows.
