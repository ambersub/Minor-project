# Chunkflow 100k Benchmark

This benchmark demonstrates the performance of `chunkflow` compared to Python's standard `multiprocessing` module when processing a CSV file with 100,000 records.

## Benchmark Scripts

We have provided two test scripts inside the `tests/` directory:

1. **`test_100k_chunkflow.py`**: Uses `chunkflow_core.process` with OpenMP multi-threading and built-in SQLite checkpointing.
2. **`test_100k_multiprocessing.py`**: Uses Python's built-in `multiprocessing.Pool` to process the data in parallel, writing results back to disk sequentially.

### The Workload

Both scripts perform the following task on `chunkflow_test_100k.csv`:
- Load 100,000 records into memory.
- Parse each row.
- Extract `val_float` and `val_int` values.
- Compute the product of these two values.
- Append the new value to the row.

## Results

We ran these benchmarks using 4 threads/processes on the same 100,000 rows.

| Tool | Setup/Processing Method | Time Taken (Seconds) | Speedup vs Python |
| :--- | :--- | :--- | :--- |
| **Python `multiprocessing`** | `multiprocessing.Pool` (4 processes) | ~0.50s | 1.00x |
| **`chunkflow`** | `chunkflow_core` OpenMP (4 threads) | **~0.19s** | **~2.64x faster** |

*(Note: Exact execution times may vary depending on background processes and hardware state, but chunkflow consistently outperforms multiprocessing due to avoiding Python's inter-process serialization overhead).*

## Why Chunkflow is Faster

1. **No GIL Limitations in Core**: OpenMP threads run efficiently in C++ without hitting the Python Global Interpreter Lock (GIL) until the callback is evaluated.
2. **Reduced Inter-Process Overhead**: Python's `multiprocessing` must pickle and unpickle every single string passed between the main process and workers. `chunkflow` utilizes lightweight shared-memory multithreading inside the C++ extension.
3. **Optimized I/O**: `chunkflow` delegates writing the outputs to an optimized C++ backend instead of doing loop appends in Python.
4. **Resiliency**: Built-in SQLite checkpointing means `chunkflow` will gracefully resume an interrupted job (which `multiprocessing` cannot do natively), with minimal overhead impact.

## How to Run the Benchmarks

You can run the benchmarks yourself by executing the following commands from the root directory:

```bash
# Run the Chunkflow benchmark
python tests/test_100k_chunkflow.py

# Run the Multiprocessing benchmark
python tests/test_100k_multiprocessing.py
```
