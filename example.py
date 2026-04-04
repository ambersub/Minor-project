"""
example.py — demonstrates chunkflow end-to-end.

Run after building the extension:
    pip install -e .
    python example.py
"""

import json
import math
from chunkflow.chunking import ChunkProcessor


# ---------------------------------------------------------------------------
# 1. Define your transform (str -> str)
# ---------------------------------------------------------------------------

def enrich_record(raw: str) -> str:
    """Parse JSON, do some work, return JSON."""
    rec = json.loads(raw)
    rec["sqrt_value"] = math.sqrt(abs(rec["value"]))
    rec["category"]   = "even" if rec["id"] % 2 == 0 else "odd"
    rec["label"]      = f"item-{rec['id']:06d}"
    return json.dumps(rec)


# ---------------------------------------------------------------------------
# 2. Build a dataset (any iterable: lists, generators, CSV rows, DB cursors)
# ---------------------------------------------------------------------------

dataset = [{"id": i, "value": i * 7 - 3} for i in range(5_000)]


# ---------------------------------------------------------------------------
# 3. Configure and run
# ---------------------------------------------------------------------------

cp = ChunkProcessor(
    db_path="results.db",   # single SQLite file for all results
    log_path="run.log",     # plain-text progress log
    chunk_size=250,         # records per parallel chunk
    num_threads=0,          # 0 = auto (one thread per CPU core via OpenMP)
)

summary = cp.process(dataset, enrich_record)
print(summary)


# ---------------------------------------------------------------------------
# 4. Read results back
# ---------------------------------------------------------------------------

results = cp.read_results()
print(f"\nFirst result : {results[0]}")
print(f"Last  result : {results[-1]}")
print(f"Total records: {len(results)}")


# ---------------------------------------------------------------------------
# 5. Inspect chunk statuses
# ---------------------------------------------------------------------------

statuses = cp.chunk_status()
done   = sum(1 for s in statuses if s["status"] == "DONE")
failed = sum(1 for s in statuses if s["status"] == "FAILED")
print(f"\nChunk report: {done} DONE, {failed} FAILED out of {len(statuses)} total")


# ---------------------------------------------------------------------------
# 6. Resume demo — re-running skips all DONE chunks instantly
# ---------------------------------------------------------------------------

print("\n-- Resume run (all chunks already DONE, should be instant) --")
summary2 = cp.process(dataset, enrich_record)
print(summary2)
