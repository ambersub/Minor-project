import os
import pytest
import chunkflow_core

SAMPLE_DATA = [
    "0,943.3758929793112,11,High,1",
    "1,818.359382413195,12,Ultra,1",
    "2,405.5891074696544,47,Ultra,0",
    "3,594.6480985952993,79,Ultra,0",
    "4,722.1682392480897,28,Low,0",
    "5,171.145749701912,42,High,0",
    "6,492.2453642124452,94,Medium,1"
]

def test_threading_and_checkpointing(tmp_path):
    out_csv = str(tmp_path / "out.csv")
    log_file = str(tmp_path / "run.log")
    checkpoint_file = str(tmp_path / "run.checkpoint")
    
    # We will use a chunk size of 3 and 2 threads.
    # Total 7 records -> 3 chunks (chunk 0: ids 0,1,2, chunk 1: ids 3,4,5, chunk 2: id 6)
    
    # First attempt: we intentionally fail when processing id "4" (in chunk 1)
    def transform_fail(row):
        fields = row.split(",")
        if fields[0] == "4":
            raise ValueError("Intentional crash at id 4")
        return row + ",processed"

    # Process with 2 threads, don't resume (fresh start)
    summary_run_1 = chunkflow_core.process(
        SAMPLE_DATA, 
        transform_fail, 
        out_csv, 
        log_file, 
        chunk_size=3, 
        num_threads=2, 
        checkpoint_path=checkpoint_file, 
        resume=False
    )
    
    # Check what was written.
    # At least chunk 0 should be completed (or might be completed depending on thread scheduling).
    # But id 4 will definitely fail, so id 4 won't be in the output.
    with open(out_csv, "r") as f:
        content = f.read()
    
    assert "4,722.1682392480897,28,Low,0,processed" not in content
    
    # Check that failed count is at least 1
    assert summary_run_1["failed"] >= 1
    
    # Second attempt: we provide a fixed transform and resume
    def transform_fixed(row):
        return row + ",processed"
        
    summary_run_2 = chunkflow_core.process(
        SAMPLE_DATA, 
        transform_fixed, 
        out_csv, 
        log_file, 
        chunk_size=3, 
        num_threads=2, 
        checkpoint_path=checkpoint_file, 
        resume=True
    )
    
    # The total processed and skipped should equal the total number of chunks (3)
    assert summary_run_2["total_chunks"] == 3
    assert summary_run_2["done"] + summary_run_2["skipped"] == 3
    assert summary_run_2["failed"] == 0
    
    with open(out_csv, "r") as f:
        final_content = [line.strip() for line in f.readlines() if line.strip()]
        
    # We should have exactly 7 lines in the output CSV, all correctly suffixed with ",processed"
    # Because of multithreading, order isn't guaranteed, so we check membership.
    assert len(final_content) == 7
    for row in SAMPLE_DATA:
        expected_row = f"{row},processed"
        assert expected_row in final_content
