"""
SQLite-backed chunked record processing (optional; not part of the CSV-focused API).

Use ``from chunkflow.chunking import ChunkProcessor`` when you need parallel chunks
and a resume-friendly SQLite store.

NOTE: This module requires the C++ chunkflow_core extension to be built and installed.
"""

from __future__ import annotations

import json
import sqlite3
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

import chunkflow_core as _core


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
    Chunked dataset processor using the C++ core with OpenMP parallelization.

    Requires the C++ chunkflow_core extension to be built and installed.
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
