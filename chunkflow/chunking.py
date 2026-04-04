"""
SQLite-backed chunked record processing (optional; not part of the CSV-focused API).

Use ``from chunkflow.chunking import ChunkProcessor`` when you need parallel chunks
and a resume-friendly SQLite store.
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
    lvl = (level + "    ")[:5]
    line = f"[{ts}] [{lvl}] {message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)


def _transform_chunk(
    c_id: int,
    items: list[str],
    transform: Callable[[str], str],
) -> tuple[int, Optional[list[str]], Optional[str]]:
    out: list[str] = []
    try:
        for item in items:
            out.append(transform(item))
        return (c_id, out, None)
    except Exception as e:
        return (c_id, None, f"Python exception: {type(e).__name__}: {e}")


def _python_executor_workers(num_threads: int) -> int:
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
    if b == "auto" or b == "":
        return sys.platform == "win32"
    raise ValueError(f"Unknown CHUNKFLOW_BACKEND={b!r} (use auto, python, or cpp)")


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


class ChunkProcessor:
    """
    Chunked dataset processor: C++ core (OpenMP) or Python backend.

    On Windows, ``CHUNKFLOW_BACKEND`` defaults to ``auto``, which selects the
    Python backend because ``chunkflow_core`` can be unreliable there.
    Set ``CHUNKFLOW_BACKEND=cpp`` to force the extension on Unix-like systems.
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

    def process(
        self,
        data: Iterable[Any],
        transform: Callable[[str], str],
        *,
        serialise: Callable[[Any], str] = json.dumps,
    ) -> RunSummary:
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

    def read_results(
        self,
        *,
        deserialise: Callable[[str], Any] = json.loads,
        chunk_id: Optional[int] = None,
    ) -> list[Any]:
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
        con = sqlite3.connect(self.db_path)
        con.execute("UPDATE chunks SET status='PENDING' WHERE status='FAILED'")
        con.commit()
        return self.process([], transform, serialise=serialise)


__all__ = ["ChunkProcessor", "RunSummary"]
