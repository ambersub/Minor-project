"""
chunkflow/__init__.py

High-level wrapper: C++ extension ``chunkflow_core`` or a Python backend with the
same SQLite schema. The Python backend splits input into fixed-size chunks and
processes multiple chunks concurrently via ``ThreadPoolExecutor`` (SQLite
commits stay on the main thread).

Usage
-----
    import json
    from chunkflow import ChunkProcessor

    # The transform must accept a str and return a str.
    # Use JSON (or any text format) to serialise structured records.
    def double_value(raw: str) -> str:
        rec = json.loads(raw)
        rec["value"] *= 2
        return json.dumps(rec)

    cp = ChunkProcessor(
        db_path="results.db",
        log_path="run.log",
        chunk_size=500,     # records per chunk
        num_threads=0,      # 0 = auto (one thread per CPU core)
    )

    # Pass any iterable of serialisable objects
    data = [{"id": i, "value": i * 3} for i in range(10_000)]
    summary = cp.process(data, double_value, serialise=json.dumps)
    print(summary)

Reading results back
--------------------
    import sqlite3, json
    con = sqlite3.connect("results.db")
    rows = con.execute("SELECT record FROM results").fetchall()
    records = [json.loads(r[0]) for r in rows]
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Optional

try:
    import chunkflow_core as _core
except ImportError:
    _core = None


def _append_log(log_path: str, level: str, message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Align with C++ Logger: level is 4 chars e.g. "INFO ", "ERROR"
    lvl = (level + "    ")[:5]
    line = f"[{ts}] [{lvl}] {message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)


def _transform_chunk(
    c_id: int,
    items: list[str],
    transform: Callable[[str], str],
) -> tuple[int, Optional[list[str]], Optional[str]]:
    """Run *transform* on every string in *items* (one chunk). Used from worker threads."""
    out: list[str] = []
    try:
        for item in items:
            out.append(transform(item))
        return (c_id, out, None)
    except Exception as e:
        return (c_id, None, f"Python exception: {type(e).__name__}: {e}")


def _python_executor_workers(num_threads: int) -> int:
    """Max parallel chunks for the Python backend (ThreadPoolExecutor)."""
    env = os.environ.get("CHUNKFLOW_MAX_WORKERS", "").strip()
    if env:
        return max(1, int(env))
    if num_threads > 0:
        return num_threads
    return max(1, min(32, (os.cpu_count() or 4)))


def _process_pure_python(
    records: list[str],
    transform: Callable[[str], str],
    db_path: str,
    log_path: str,
    chunk_size: int,
    num_threads: int = 0,
) -> dict[str, Any]:
    """
    Same chunking and SQLite schema as chunkflow_core.

    Chunks are processed in parallel with ``ThreadPoolExecutor`` (one future per
    chunk). SQLite writes run on the main thread only. *num_threads* sets the
    pool size when > 0; otherwise auto (CPU-based cap). Override with
    ``CHUNKFLOW_MAX_WORKERS``.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    _append_log(log_path, "INFO", "---------- chunkflow session started (Python backend) ----------")

    total = 0
    done_count = 0
    skip_count = 0
    fail_count = 0
    elapsed = 0.0
    con: sqlite3.Connection | None = None
    try:
        con = sqlite3.connect(db_path, timeout=60.0)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id   INTEGER PRIMARY KEY,
                status     TEXT    NOT NULL DEFAULT 'PENDING',
                records    INTEGER NOT NULL DEFAULT 0,
                error_msg  TEXT
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id   INTEGER NOT NULL,
                record     TEXT    NOT NULL,
                FOREIGN KEY(chunk_id) REFERENCES chunks(chunk_id)
            );
            """
        )
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.commit()

        chunks: list[tuple[int, list[str]]] = []
        cid = 0
        for i in range(0, len(records), chunk_size):
            chunks.append((cid, records[i : i + chunk_size]))
            cid += 1

        total = len(chunks)
        workers = _python_executor_workers(num_threads)
        _append_log(
            log_path,
            "INFO",
            f"Records: {len(records)} | Chunks: {total} | Chunk size: {chunk_size} | "
            f"backend=Python | chunk_threads={workers}",
        )

        for c_id, items in chunks:
            con.execute(
                "INSERT OR IGNORE INTO chunks(chunk_id, status, records) VALUES (?, 'PENDING', ?)",
                (c_id, len(items)),
            )
        con.commit()

        done_count = 0
        skip_count = 0
        fail_count = 0
        t0 = time.perf_counter()

        pending: list[tuple[int, list[str]]] = []
        for c_id, items in chunks:
            row = con.execute(
                "SELECT status FROM chunks WHERE chunk_id = ?",
                (c_id,),
            ).fetchone()
            status = row[0] if row else ""
            if status == "DONE":
                _append_log(log_path, "INFO", f"Chunk {c_id} skipped (already DONE)")
                skip_count += 1
                continue
            pending.append((c_id, items))

        def _commit_chunk_result(
            c_id: int,
            out_records: Optional[list[str]],
            error_msg: Optional[str],
        ) -> None:
            nonlocal done_count, fail_count
            if error_msg is not None:
                con.execute(
                    "UPDATE chunks SET status = 'FAILED', error_msg = ? WHERE chunk_id = ?",
                    (error_msg, c_id),
                )
                con.commit()
                _append_log(log_path, "ERROR", f"Chunk {c_id} FAILED — {error_msg}")
                fail_count += 1
                return
            assert out_records is not None
            con.execute("BEGIN")
            for rec in out_records:
                con.execute(
                    "INSERT INTO results(chunk_id, record) VALUES (?, ?)",
                    (c_id, rec),
                )
            con.execute(
                "UPDATE chunks SET status = 'DONE', records = ? WHERE chunk_id = ?",
                (len(out_records), c_id),
            )
            con.execute("COMMIT")
            _append_log(
                log_path,
                "INFO",
                f"Chunk {c_id} DONE ({len(out_records)} records written)",
            )
            done_count += 1

        if not pending:
            pass
        elif workers <= 1:
            for c_id, items in pending:
                _append_log(
                    log_path,
                    "INFO",
                    f"Chunk {c_id} started ({len(items)} records)",
                )
                cid, outs, err = _transform_chunk(c_id, items, transform)
                _commit_chunk_result(cid, outs, err)
        else:
            _append_log(
                log_path,
                "INFO",
                f"Starting {len(pending)} chunks on ThreadPoolExecutor(max_workers={workers})",
            )
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {
                    pool.submit(_transform_chunk, c_id, items, transform): c_id
                    for c_id, items in pending
                }
                for fut in as_completed(future_map):
                    c_id, out_records, error_msg = fut.result()
                    _append_log(
                        log_path,
                        "INFO",
                        f"Chunk {c_id} finished transform, committing…",
                    )
                    _commit_chunk_result(c_id, out_records, error_msg)

        elapsed = time.perf_counter() - t0
        _append_log(
            log_path,
            "INFO",
            f"Finished — done={done_count} skipped={skip_count} failed={fail_count} elapsed={elapsed}s",
        )
    finally:
        if con is not None:
            con.close()

    _append_log(log_path, "INFO", "---------- chunkflow session ended (Python backend) ----------")

    return {
        "total_chunks": total,
        "done": done_count,
        "skipped": skip_count,
        "failed": fail_count,
        "elapsed_seconds": elapsed,
    }


def _use_python_backend() -> bool:
    b = os.environ.get("CHUNKFLOW_BACKEND", "auto").strip().lower()
    if b == "python":
        return True
    if b == "cpp":
        return False
    # auto: C++ extension is unreliable on Windows (hang / access violation); use Python there.
    if b == "auto" or b == "":
        return sys.platform == "win32"
    raise ValueError(f"Unknown CHUNKFLOW_BACKEND={b!r} (use auto, python, or cpp)")


# ---------------------------------------------------------------------------
# Run summary
# ---------------------------------------------------------------------------


@dataclass
class RunSummary:
    """Returned by :meth:`ChunkProcessor.process`."""

    db_path: str = ""
    log_path: str = ""
    total_chunks: int = 0
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    elapsed_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_chunks == 0:
            return 0.0
        return (self.completed + self.skipped) / self.total_chunks * 100

    def __str__(self) -> str:
        # ASCII only: safe on Windows cp1252 consoles
        return textwrap.dedent(f"""
        -- ChunkFlow Run Summary --
          Output DB      : {self.db_path}
          Log file       : {self.log_path}
          Total chunks   : {self.total_chunks}
          Completed      : {self.completed}
          Skipped (cache): {self.skipped}
          Failed         : {self.failed}
          Elapsed        : {self.elapsed_seconds:.2f}s
          Success rate   : {self.success_rate:.1f}%
        -----------------------------
        """).strip()


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------


class ChunkProcessor:
    """
    Chunked dataset processor: C++ core (OpenMP) or Python backend.

    On Windows, ``CHUNKFLOW_BACKEND`` defaults to ``auto``, which selects the
    Python backend (same SQLite schema, sequential) because ``chunkflow_core``
    can hang or crash when mixing OpenMP with the CPython GIL. Set
    ``CHUNKFLOW_BACKEND=cpp`` to force the extension on Unix-like systems
    where it is built.

    Parameters
    ----------
    db_path : str
        Path to the SQLite output file.  If it already exists the run will
        resume, skipping chunks already marked DONE.
    log_path : str
        Path to the plain-text progress log.  Always appended to so that
        multi-run history is preserved.
    chunk_size : int
        Number of records per chunk.  Default 500.
    num_threads : int
        **C++ backend:** OpenMP thread count (``0`` = library default).
        **Python backend:** ``ThreadPoolExecutor`` size for parallel *chunks*
        (``0`` = auto, capped by CPU count; override with ``CHUNKFLOW_MAX_WORKERS``).
    """

    def __init__(
        self,
        db_path: str = "chunkflow_results.db",
        log_path: str = "chunkflow.log",
        chunk_size: int = 500,
        num_threads: int = 0,
    ) -> None:
        self.db_path = db_path
        self.log_path = log_path
        self.chunk_size = chunk_size
        self.num_threads = num_threads

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(
        self,
        data: Iterable[Any],
        transform: Callable[[str], str],
        *,
        serialise: Callable[[Any], str] = json.dumps,
    ) -> RunSummary:
        """
        Process every item in *data* through *transform* in parallel.

        Parameters
        ----------
        data : iterable
            Any Python iterable.  Each item is first serialised to a string
            using *serialise*, then passed as-is to *transform*.
        transform : callable[[str], str]
            Your processing function.  Receives one serialised record string,
            returns one result string.  **Must be picklable** if you later
            switch to process-based parallelism.
        serialise : callable[[Any], str], optional
            Converts each raw item to a string before handing it to the C++
            layer.  Defaults to ``json.dumps``.

        Returns
        -------
        RunSummary
        """
        records: list[str] = [serialise(item) for item in data]

        if _use_python_backend():
            raw = _process_pure_python(
                records,
                transform,
                self.db_path,
                self.log_path,
                self.chunk_size,
                self.num_threads,
            )
        else:
            if _core is None:
                raise ImportError(
                    "chunkflow_core is not built but CHUNKFLOW_BACKEND=cpp.\n"
                    "Run: pip install .   or   set CHUNKFLOW_BACKEND=python"
                )
            raw = _core.process(
                records,
                transform,
                self.db_path,
                self.log_path,
                self.chunk_size,
                self.num_threads,
            )

        return RunSummary(
            db_path=self.db_path,
            log_path=self.log_path,
            total_chunks=raw["total_chunks"],
            completed=raw["done"],
            skipped=raw["skipped"],
            failed=raw["failed"],
            elapsed_seconds=raw["elapsed_seconds"],
        )

    # ------------------------------------------------------------------
    # Convenience read-back helpers
    # ------------------------------------------------------------------

    def read_results(
        self,
        *,
        deserialise: Callable[[str], Any] = json.loads,
        chunk_id: Optional[int] = None,
    ) -> list[Any]:
        """
        Read processed records back from the SQLite database.

        Parameters
        ----------
        deserialise : callable[[str], Any], optional
            Inverse of the *serialise* function passed to :meth:`process`.
            Defaults to ``json.loads``.
        chunk_id : int, optional
            If given, only records from that specific chunk are returned.

        Returns
        -------
        list
            Deserialised result objects in insertion order.
        """
        con = sqlite3.connect(self.db_path)
        try:
            if chunk_id is None:
                rows = con.execute(
                    "SELECT record FROM results ORDER BY id"
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT record FROM results WHERE chunk_id=? ORDER BY id",
                    (chunk_id,),
                ).fetchall()
        finally:
            con.close()
        return [deserialise(r[0]) for r in rows]

    def chunk_status(self) -> list[dict]:
        """
        Return a list of dicts describing every chunk's status.

        Each dict has the keys: ``chunk_id``, ``status``, ``records``,
        ``error_msg``.
        """
        con = sqlite3.connect(self.db_path)
        try:
            rows = con.execute(
                "SELECT chunk_id, status, records, error_msg "
                "FROM chunks ORDER BY chunk_id"
            ).fetchall()
        finally:
            con.close()
        return [
            {
                "chunk_id":  r[0],
                "status":    r[1],
                "records":   r[2],
                "error_msg": r[3],
            }
            for r in rows
        ]

    def retry_failed(
        self,
        transform: Callable[[str], str],
        *,
        serialise: Callable[[Any], str] = json.dumps,
    ) -> RunSummary:
        """
        Reset all FAILED chunks to PENDING and re-run :meth:`process`.

        This is a convenience wrapper: connect to the DB, flip statuses, then
        call :meth:`process` with an empty iterable (the C++ layer will pick
        up the now-PENDING chunks on its own resume logic).

        .. note::
            Because this resets only FAILED chunks, DONE chunks remain cached
            and will be skipped automatically.
        """
        con = sqlite3.connect(self.db_path)
        con.execute("UPDATE chunks SET status='PENDING' WHERE status='FAILED'")
        con.commit()
        # Re-run with empty new data — C++ layer uses DB state for resuming
        return self.process([], transform, serialise=serialise)


__all__ = ["ChunkProcessor", "RunSummary"]
