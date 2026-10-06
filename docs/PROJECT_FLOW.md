# 🌊 ChunkFlow Architecture & Project Flow

This document provides an exhaustive explanation of the execution workflow, architectural design, data lifecycle, library usage locations, and error recovery mechanics of **ChunkFlow**.

---

## 📐 Architecture Overview

ChunkFlow combines high-level Python ergonomics with low-level C++17 native performance. The execution pipeline crosses from Python managed memory into native unmanaged C++ data structures via **pybind11**, processes arrays using **OpenMP multi-threading**, and persists results with minimal overhead.

```mermaid
flowchart TD
    subgraph Python ["🐍 Python Application Layer"]
        UserScript["User Script / Application Code"] -->|Call API| PythonAPI["chunkflow / chunkflow.csv_math / ChunkProcessor"]
        PythonAPI -->|Pass Python Objects| PyBind["pybind11 C++ Wrapper Layer"]
    end

    subgraph C++ Core ["⚡ C++ Core Execution Kernel (chunkflow_core.cpp)"]
        PyBind -->|Convert to std::string / std::vector| Router{"Kernel Function Dispatcher"}
        
        Router -->|Row Splitting & Parsing| Parser["RFC4180 CSV & Custom Delimiter Parser (std::string / std::vector)"]
        Router -->|Row-level Math| MathKernel["Binary / Scalar / Unary / Power SIMD Kernels (std::cmath)"]
        Router -->|Batch Datasets| OpenMPThreadEngine["OpenMP Parallel Loop Engine (#pragma omp parallel for)"]
        Router -->|Row Filtering| FilterEngine["Field Matcher / Range Filter / GIL Predicate Evaluator (py::gil_scoped_acquire)"]
        Router -->|Chunked Workflows| ChunkEngine["Chunk Processor Loop & State Engine (std::fstream / std::unordered_set)"]

        OpenMPThreadEngine -->|Thread Worker 0..N| FastBuffer["Shared Memory Buffer (std::vector<std::string>)"]
        ChunkEngine -->|GIL Scoped Callback| PythonLambda["Python Transform Callable (py::object / pybind11)"]
        ChunkEngine -->|Log State| Logger["Logger Engine (Thread-Safe std::ofstream & std::mutex)"]
        ChunkEngine -->|Checkpoint State| CheckpointEngine["Checkpoint File Sync (std::ofstream .checkpoint)"]
    end

    subgraph IOLayer ["💾 Persistence & Disk Output"]
        FastBuffer -->|Bulk Stream Write| OutputCSV["Destination CSV / Data File"]
        Logger -->|Append Timestamp Logs| LogFile["run.log / Custom Log"]
        CheckpointEngine -->|Write Chunk IDs| CheckpointFile["run.checkpoint / Checkpoint Store"]
    end
```

---

## 🔄 Detailed Step-by-Step Data Flow with Library Annotations

### Step 1: Input Ingestion & Type Marshaling
1. The user script invokes a function from `chunkflow.csv_math` or `chunkflow.chunking` with Python string rows, column indices, mathematical operators, or lambda callbacks.
2. **`pybind11` (`<pybind11/pybind11.h>`, `<pybind11/stl.h>`, `<pybind11/functional.h>`)** intercepts the call:
   - Python string lists are converted to C++ `std::vector<std::string>` containers.
   - Numeric inputs (`int`, `float`) are translated into native C++ integer types and `double`.
   - Python callables are wrapped in `py::object` function handles.

### Step 2: C++ Parsing & Tokenization Mechanics
For CSV and custom-delimited rows, ChunkFlow avoids regular expressions or overhead-heavy string tokens:
- **`split_csv_row()`** & **`split_delimited_row()`**: Read characters sequentially from C++ **`std::string`** (`<string>`).
- **`trim_inplace()`**: Strips whitespace using C++ **`<cctype>`** (`std::isspace`) without extra memory allocation.
- Results are stored in C++ **`std::vector<std::string>`** (`<vector>`).

```mermaid
sequenceDiagram
    autonumber
    actor User as Python User
    participant PyAPI as chunkflow.csv_math
    participant PyBind as pybind11 Layer
    participant CPP as C++ Engine (chunkflow_core)
    participant Disk as File System (std::fstream)

    User->>PyAPI: apply_csv_rows_math_binary(rows, "add", 0, 1, 2)
    PyAPI->>PyBind: Marshal list[str] to std::vector<std::string>
    PyBind->>CPP: Invoke apply_csv_rows_math_binary()
    Note over CPP: OpenMP parallel loop (#pragma omp parallel for via <omp.h>)
    par Thread 0
        CPP->>CPP: Parse Row 0..N/4 (split_csv_row using std::string)
        CPP->>CPP: Compute double addition (std::stod & double operator+)
        CPP->>CPP: Format output double (format_double_cell using std::ostringstream)
    and Thread 1..3
        CPP->>CPP: Parse & Compute remaining row chunks concurrently (<omp.h>)
    end
    CPP-->>PyBind: Return std::vector<std::string>
    PyBind-->>PyAPI: Return Python list[str]
    PyAPI-->>User: Processed CSV lines returned
```

### Step 3: Numeric Kernel Execution
- Cell contents are parsed via C++ **`<string>`** (`std::stod`) wrapped in non-throwing **`<optional>`** (`std::optional<double>`).
- Operations execute native floating point math via C++ **`<cmath>`**:
  - **Binary Math**: `apply_binary()` performs addition (`+`), subtraction (`-`), multiplication (`*`), or division (`/`). Checks for division-by-zero errors.
  - **Scalar Math**: `apply_scalar()` performs scalar operations (`value op scalar`).
  - **Unary Math**: `apply_unary()` executes **`<cmath>`** routines (`std::sqrt` with negative input validation, `std::fabs`, `std::floor`, `std::ceil`).
  - **Power Scaling**: `apply_csv_row_math_power()` executes **`<cmath>`** (`std::pow(value, exponent)`).
- Outputs are formatted into C++ **`std::string`** buffers using C++ **`<sstream>`** (`std::ostringstream` with `precision(17)`).

### Step 4: OpenMP Multi-Threaded Parallel Processing
- When processing list-based operations (`apply_csv_rows_math_*`), ChunkFlow applies **`OpenMP` (`<omp.h>`)** compiler pragmas:
  ```cpp
  #pragma omp parallel for
  for (int i = 0; i < (int)rows.size(); ++i) {
      // Independent row processing across CPU threads without GIL contention
  }
  ```
- **Thread Safety & Exception Propagation**:
  - Thread iterations write directly to distinct array indices in `std::vector<std::string> out(rows.size())`.
  - Exception handling uses **`OpenMP`** critical blocks (`#pragma omp critical`) to update atomic error flags safely across threads.

### Step 5: Chunked Dataset Processing & Resumable Checkpoints
For large dataset workflows triggered via `chunkflow_core.process()`:

```mermaid
stateDiagram-v2
    [*] --> Initializing: Load records & check checkpoint file
    Initializing --> CheckpointFound: resume=True & run.checkpoint exists (std::ifstream)
    Initializing --> FreshRun: resume=False or missing checkpoint

    CheckpointFound --> SkippingChunks: Read completed chunk IDs (std::unordered_set<int>)
    FreshRun --> ProcessingChunks: Start from Chunk 0

    SkippingChunks --> ProcessingChunks: Resume next pending chunk

    state ProcessingChunks {
        [*] --> AcquireGIL: Acquire Python GIL (py::gil_scoped_acquire)
        AcquireGIL --> RunTransform: Call transform_fn(record) (pybind11 py::object)
        RunTransform --> ReleaseGIL: Release GIL & buffer output (std::vector)
        ReleaseGIL --> WriteOutput: Stream records to output file (std::ofstream)
    }

    ProcessingChunks --> LogDone: Chunk succeeds (Logger & std::mutex)
    ProcessingChunks --> LogFail: Exception raised in transform_fn

    LogDone --> UpdateCheckpoint: Write Chunk ID to run.checkpoint (std::ofstream)
    LogFail --> AbortOrContinue: Log error to run.log (Logger)

    UpdateCheckpoint --> [*]: All chunks processed
```

1. **Chunk Splitting**: Records are partitioned into `std::vector<Chunk>` buffers using C++ **`<algorithm>`** (`std::min`).
2. **Resumption Verification**: C++ **`<fstream>`** (`std::ifstream`) reads `checkpoint_path` into a C++ **`<unordered_set>`** (`std::unordered_set<int>`) for $O(1)$ chunk lookup.
3. **GIL Synchronization**: The thread acquires the Python GIL via **`pybind11`** (`py::gil_scoped_acquire`), executes the Python callable via `py::object`, and releases the GIL upon exiting the scope.
4. **Logging & Checkpoint Sync**:
   - Timestamps are formatted using C++ **`<chrono>`** (`system_clock`) and **`<ctime>`** (`std::strftime`).
   - Logging writes to disk using C++ **`<fstream>`** (`std::ofstream`) protected by C++ **`<mutex>`** (`std::mutex` and `std::lock_guard`).
   - Completed chunk IDs are appended to `checkpoint_path` via `std::ofstream`.

---

## 🔒 Memory Safety & Exception Handling

- **Zero Memory Leaks**: All dynamic allocations leverage C++ RAII via **`<vector>`** (`std::vector`) and **`<string>`** (`std::string`).
- **String Reserve Optimization**: Buffer string memory is pre-reserved (`cur.reserve(64)`, `out.reserve(fields.size() * 16)`) to eliminate reallocations.
- **Python Exception Interception**: **`pybind11`** catches C++ `std::exception` and `py::error_already_set`, converting them into native Python `ValueError` or `RuntimeError` objects.
