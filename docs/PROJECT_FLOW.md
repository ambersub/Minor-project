# 🌊 ChunkFlow Architecture & Project Flow

This document provides an exhaustive explanation of the execution workflow, architectural design, data lifecycle, and error recovery mechanics of **ChunkFlow**.

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
        
        Router -->|Row Splitting & Parsing| Parser["RFC4180 CSV & Custom Delimiter Parser"]
        Router -->|Row-level Math| MathKernel["Binary / Scalar / Unary / Power SIMD Kernels"]
        Router -->|Batch Datasets| OpenMPThreadEngine["OpenMP Parallel Loop Engine (#pragma omp parallel for)"]
        Router -->|Row Filtering| FilterEngine["Field Matcher / Range Filter / GIL Predicate Evaluator"]
        Router -->|Chunked Workflows| ChunkEngine["Chunk Processor Loop & State Engine"]

        OpenMPThreadEngine -->|Thread Worker 0..N| FastBuffer["Shared Memory Buffer (std::vector<std::string>)"]
        ChunkEngine -->|GIL Scoped Callback| PythonLambda["Python Transform Callable"]
        ChunkEngine -->|Log State| Logger["Logger Engine (Thread-Safe std::ofstream)"]
        ChunkEngine -->|Checkpoint State| CheckpointEngine["Checkpoint File Sync (.checkpoint)"]
    end

    subgraph IOLayer ["💾 Persistence & Disk Output"]
        FastBuffer -->|Bulk Stream Write| OutputCSV["Destination CSV / Data File"]
        Logger -->|Append Timestamp Logs| LogFile["run.log / Custom Log"]
        CheckpointEngine -->|Write Chunk IDs| CheckpointFile["run.checkpoint / Checkpoint Store"]
    end
```

---

## 🔄 Detailed Step-by-Step Data Flow

### Step 1: Input Ingestion & Type Marshaling
1. The user invokes a function from `chunkflow.csv_math` or `chunkflow.chunking` with Python string rows, column indices, mathematical operators, or lambda callbacks.
2. **pybind11** intercepts the call:
   - Python strings are converted to `std::string` or `std::vector<std::string>`.
   - Numeric inputs (`int`, `float`) are translated into native C++ integer types and `double`.
   - Python callables are wrapped in `py::object` function handles.

### Step 2: C++ Parsing & Tokenization Mechanics
For CSV and custom-delimited rows, ChunkFlow avoids regular expressions or overhead-heavy string tokens:
- **`split_csv_row()`**: Reads characters sequentially. Tracks quote states (`in_quotes`), handles RFC4180 escaped quotes (`""`), and trims whitespace in-place via `trim_inplace()`.
- **`split_delimited_row()`**: Handles multi-character or non-comma delimiters (such as `\t`, `|`, `;`) with optional quote support.

```mermaid
sequenceDiagram
    autonumber
    actor User as Python User
    participant PyAPI as chunkflow.csv_math
    participant PyBind as pybind11 Layer
    participant CPP as C++ Engine (chunkflow_core)
    participant Disk as File System

    User->>PyAPI: apply_csv_rows_math_binary(rows, "add", 0, 1, 2)
    PyAPI->>PyBind: Marshal list[str] to std::vector<std::string>
    PyBind->>CPP: Invoke apply_csv_rows_math_binary()
    Note over CPP: OpenMP parallel loop (#pragma omp parallel for)
    par Thread 0
        CPP->>CPP: Parse Row 0..N/4 (split_csv_row)
        CPP->>CPP: Compute double addition (a + b)
        CPP->>CPP: Format output double (format_double_cell)
    and Thread 1..3
        CPP->>CPP: Parse & Compute remaining row chunks concurrently
    end
    CPP-->>PyBind: Return std::vector<std::string>
    PyBind-->>PyAPI: Return Python list[str]
    PyAPI-->>User: Processed CSV lines returned
```

### Step 3: Numeric Kernel Execution
- Cell contents are converted to IEEE 754 floating-point double precision values using high-speed string-to-double conversion (`parse_double_field` wrapping `std::stod`).
- Execution is dispatched to specialized static functions:
  - **Binary Math**: `apply_binary()` performs addition (`a + b`), subtraction (`a - b`), multiplication (`a * b`), or division (`a / b`). Checks for division-by-zero errors.
  - **Scalar Math**: `apply_scalar()` performs scalar transformations (`value op scalar`).
  - **Unary Math**: `apply_unary()` executes `sqrt()` (with negative number safety checks), `abs()`, `floor()`, or `ceil()`.
  - **Power Scaling**: `apply_csv_row_math_power()` executes `std::pow(value, exponent)`.

### Step 4: OpenMP Multi-Threaded Parallel Processing
- When processing list-based operations (`apply_csv_rows_math_*`), ChunkFlow applies OpenMP pragmas:
  ```cpp
  #pragma omp parallel for
  for (int i = 0; i < (int)rows.size(); ++i) {
      // Independent row processing without GIL interference
  }
  ```
- **Thread Safety & Exception Propagation**:
  - Thread iterations operate on separate indices of the output vector `std::vector<std::string> out(rows.size())`.
  - If a thread catches an exception (e.g. non-numeric data or division by zero), it sets an `error_flag` inside a `#pragma omp critical` block, ensuring clean cleanup and raising a standard Python exception.

### Step 5: Chunked Dataset Processing & Resumable Checkpoints
For large dataset workflows triggered via `chunkflow_core.process()`:

```mermaid
stateDiagram-v2
    [*] --> Initializing: Load records & check checkpoint file
    Initializing --> CheckpointFound: resume=True & run.checkpoint exists
    Initializing --> FreshRun: resume=False or missing checkpoint

    CheckpointFound --> SkippingChunks: Read completed chunk IDs
    FreshRun --> ProcessingChunks: Start from Chunk 0

    SkippingChunks --> ProcessingChunks: Resume next pending chunk

    state ProcessingChunks {
        [*] --> AcquireGIL: Acquire Python GIL
        AcquireGIL --> RunTransform: Call transform_fn(record)
        RunTransform --> ReleaseGIL: Release GIL & buffer output
        ReleaseGIL --> WriteOutput: Stream records to output file
    }

    ProcessingChunks --> LogDone: Chunk succeeds
    ProcessingChunks --> LogFail: Exception raised in transform_fn

    LogDone --> UpdateCheckpoint: Write Chunk ID to run.checkpoint
    LogFail --> AbortOrContinue: Log error to run.log

    UpdateCheckpoint --> [*]: All chunks processed
```

1. **Chunk Splitting**: Records are segmented into fixed-size chunks (e.g. 5,000 records per chunk).
2. **Resumption Verification**: If `resume=True`, ChunkFlow parses `checkpoint_path` to build an `unordered_set<int>` of completed chunk IDs.
3. **GIL Synchronization**: When calling back into Python user logic (`transform_fn`), the C++ thread acquires the Python GIL using `py::gil_scoped_acquire gil`, executes the transform, and releases the GIL immediately afterward.
4. **Logging & Checkpoint Sync**:
   - Every chunk completion or failure event is logged with timestamps (`YYYY-MM-DD HH:MM:SS`) to `log_path`.
   - Successful chunk IDs are appended immediately to `checkpoint_path`.

---

## 🔒 Memory Safety & Exception Handling

- **Zero Memory Leaks**: All dynamic allocations leverage C++ RAII (Resource Acquisition Is Initialization) via `std::vector` and `std::string`.
- **String Reserve Optimization**: Buffer string memory is reserved in advance (`cur.reserve(64)`, `out.reserve(fields.size() * 16)`) to eliminate reallocations during CSV construction.
- **Python Exception Interception**: C++ catches both standard `std::exception` instances and `py::error_already_set` exceptions, converting them into native Python `ValueError` or `RuntimeError` messages.
