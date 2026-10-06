# 📁 ChunkFlow File Directory & Module Reference

This document provides a comprehensive, file-by-file reference for every file and directory in the **ChunkFlow** codebase, including an explicit list of **which libraries are used in each file**.

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
│   ├── PROJECT_FLOW.md                    # Architectural & execution flow documentation
│   ├── FILE_DIRECTORY.md                  # Detailed directory & file reference (This document)
│   ├── TECH_STACK.md                      # Technology stack & compiler breakdown
│   ├── LIBRARIES_REPORT.md                # Comprehensive audit of all libraries used
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

## 📄 Exhaustive File Specifications & Library Usages

### 1. Python Application Package (`chunkflow/`)

#### 🔹 [`chunkflow/__init__.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/__init__.py)
* **Lines of Code**: ~40 lines
* **Purpose**: Primary module initialization file defining the public API interface of `chunkflow`.
* **Libraries Used In This File**:
  - `__future__.annotations`: PEP 563 deferred evaluation of type annotations.
  - `chunkflow.csv_math`: Internal module re-exporting public functions.

#### 🔹 [`chunkflow/csv_math.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/csv_math.py)
* **Lines of Code**: ~56 lines
* **Purpose**: Python binding layer delegating math, parsing, and filtering directly to `chunkflow_core`.
* **Libraries Used In This File**:
  - `__future__.annotations`: PEP 563 deferred evaluation of type annotations.
  - `chunkflow_core` (`_core`): Native C++ extension module compiled via `pybind11` and `setuptools`. Binds `split_csv_row`, `join_csv_row`, `split_delimited_row`, `join_delimited_row`, `apply_csv_row_math_binary`, `apply_csv_row_math_scalar`, `apply_csv_rows_math_binary`, `apply_csv_rows_math_scalar`, `apply_csv_row_math_unary`, `apply_csv_rows_math_unary`, `apply_csv_row_math_power`, `filter_rows`, `filter_rows_by_field`, `filter_rows_by_range`.

#### 🔹 [`chunkflow/chunking.py`](file:///c:/Users/Amber/Desktop/minor-project/chunkflow/chunking.py)
* **Lines of Code**: ~154 lines
* **Purpose**: Provides Python class abstractions for dataset processing with statistical metrics.
* **Libraries Used In This File**:
  - `json` (`json.dumps`, `json.loads`): Record serialization and deserialization.
  - `sqlite3` (`sqlite3.connect`): Database queries in `read_results()`, `chunk_status()`, and `retry_failed()`.
  - `textwrap` (`textwrap.dedent`): Formatting `RunSummary.__str__()` CLI output.
  - `dataclasses` (`@dataclass`): Data container `RunSummary`.
  - `typing` (`Any`, `Callable`, `Iterable`, `Optional`): Function parameter and return type hints.
  - `chunkflow_core` (`_core.process`): Under-the-hood C++ dataset chunk processing engine.

---

### 2. C++ Extension Core (`src/`)

#### 🔹 [`src/chunkflow_core.cpp`](file:///c:/Users/Amber/Desktop/minor-project/src/chunkflow_core.cpp)
* **Lines of Code**: ~1079 lines
* **Purpose**: The main engine of ChunkFlow written in native C++17.
* **Libraries Used In This File**:
  - `<pybind11/pybind11.h>`, `<pybind11/functional.h>`, `<pybind11/stl.h>` (`pybind11`): Module creation (`PYBIND11_MODULE`), Python object wrappers (`py::object`, `py::str`, `py::dict`), GIL acquisition (`py::gil_scoped_acquire`), and C++/Python STL conversions.
  - `<omp.h>` (`OpenMP`): Multi-core parallel loop execution (`#pragma omp parallel for`), critical section safety (`#pragma omp critical`), and thread management (`omp_set_num_threads`, `omp_get_max_threads`).
  - `<cmath>` (C++ STL): Floating-point operations (`std::sqrt`, `std::fabs`, `std::floor`, `std::ceil`, `std::pow`).
  - `<algorithm>` (C++ STL): Case lowercasing (`std::transform`, `std::tolower`) and chunk sizing (`std::min`).
  - `<chrono>` (C++ STL): Steady clock performance timing (`steady_clock::now()`) and calendar dates (`system_clock`).
  - `<fstream>` (C++ STL): Stream disk file I/O for `std::ofstream` and `std::ifstream`.
  - `<mutex>` (C++ STL): Thread-safe log writing via `std::mutex` and `std::lock_guard`.
  - `<optional>` (C++ STL): Non-throwing numeric conversion (`std::optional<double>`).
  - `<sstream>` (C++ STL): High-precision cell formatting (`std::ostringstream`).
  - `<vector>` & `<unordered_set>` (C++ STL): Memory array storage (`std::vector`) and $O(1)$ checkpoint chunk set (`std::unordered_set<int>`).
  - `<stdexcept>` (C++ STL): Exception throwing (`std::invalid_argument`, `std::runtime_error`).

---

### 3. Build & Packaging Infrastructure

#### 🔹 [`setup.py`](file:///c:/Users/Amber/Desktop/minor-project/setup.py)
* **Lines of Code**: ~90 lines
* **Purpose**: Configures pybind11 compilation flags, platform libraries, and OpenMP linking.
* **Libraries Used In This File**:
  - `setuptools` (`Extension`, `setup`, `find_packages`): Defining `chunkflow_core` C++ extension module.
  - `pybind11` (`pybind11.get_include()`): Providing header include directories for C++ compilation.
  - `sys`: Detecting operating system platform (`sys.platform`).
  - `os`: Reading environment variables (`os.environ`).
  - `pathlib.Path`: Resolving file paths.

#### 🔹 [`pyproject.toml`](file:///c:/Users/Amber/Desktop/minor-project/pyproject.toml)
* **Purpose**: PEP 517 build system configuration specifying `setuptools` and `pybind11>=2.11` as build dependencies.

#### 🔹 [`setup.cfg`](file:///c:/Users/Amber/Desktop/minor-project/setup.cfg)
* **Purpose**: Package distribution configuration fallback options.

---

### 4. Utilities & Benchmarks

#### 🔹 [`datasetgen.py`](file:///c:/Users/Amber/Desktop/minor-project/datasetgen.py)
* **Purpose**: Generates synthetic benchmark CSV datasets (`chunkflow_test_100k.csv`) containing 100,000 to 1,000,000 rows.
* **Libraries Used In This File**:
  - `pandas` (`pd.DataFrame`, `df.to_csv()`): Exporting synthetic CSV dataset.
  - `numpy` (`np.arange`, `np.random.uniform`, `np.random.randint`, `np.random.choice`): Generating float, int, category, and label arrays.

#### 🔹 [`README_100k_benchmark.md`](file:///c:/Users/Amber/Desktop/minor-project/README_100k_benchmark.md)
* **Purpose**: Documents benchmark methodology and performance comparisons demonstrating **~2.64x speedup** of ChunkFlow vs standard Python `multiprocessing`.

---

### 5. Test Harness (`tests/`)

#### 🔹 [`tests/test_chunkflow_math.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_chunkflow_math.py)
* **Purpose**: Unit tests verifying basic binary addition, subtraction, division, scalar scaling, quoted cell preservation, and zero-division error raising.
* **Libraries Used In This File**: `pytest` (`pytest.approx`, `pytest.raises`), `pathlib.Path`, `chunkflow.csv_math`.

#### 🔹 [`tests/test_chunkflow_features.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_chunkflow_features.py)
* **Purpose**: Unit tests verifying batch math, column aggregation (`sum`, `average`), column concatenation, whitespace trimming, and checkpoint recovery.
* **Libraries Used In This File**: `pytest` (`tmp_path`), `os`, `chunkflow_core`.

#### 🔹 [`tests/test_dataset_threading_checkpoint.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_dataset_threading_checkpoint.py)
* **Purpose**: Multi-threaded integration test verifying chunk recovery when resuming an intentionally interrupted job.
* **Libraries Used In This File**: `pytest` (`tmp_path`), `os`, `chunkflow_core`.

#### 🔹 [`tests/test_superstore_sample_addition.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_superstore_sample_addition.py)
* **Purpose**: End-to-end file integration test reading physical CSV files, executing `chunkflow_core` transformations, and verifying resulting CSV output files.
* **Libraries Used In This File**: `pytest` (`pytest.approx`, `pytest.raises`, `pytest.skip`), `csv` (`csv.reader`), `sys`, `io.StringIO`, `pathlib.Path`, `os` (`os.add_dll_directory`), `shutil` (`shutil.which("gcc")`), `chunkflow_core`.

#### 🔹 [`tests/test_100k_chunkflow.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_100k_chunkflow.py)
* **Purpose**: Benchmark script timing `chunkflow_core.process()` over 100,000 CSV rows using 4 OpenMP threads.
* **Libraries Used In This File**: `os`, `time` (`time.time()`), `chunkflow_core`.

#### 🔹 [`tests/test_100k_multiprocessing.py`](file:///c:/Users/Amber/Desktop/minor-project/tests/test_100k_multiprocessing.py)
* **Purpose**: Benchmark comparison script timing standard Python `multiprocessing.Pool` over 100,000 CSV rows.
* **Libraries Used In This File**: `os`, `time` (`time.time()`), `multiprocessing` (`multiprocessing.Pool`).
