import os
import pytest
import chunkflow_core

def test_apply_csv_rows_math_scalar():
    rows = ["10,20", "30,40"]
    # Multiply column 0 by 1.10
    result = chunkflow_core.apply_csv_rows_math_scalar(rows, "mul", 0, 1.10, 2)
    assert result == ["10,20,11", "30,40,33"]

def test_apply_csv_rows_math_binary():
    rows = ["100,50", "200,75"]
    # Subtract col 1 from col 0
    result = chunkflow_core.apply_csv_rows_math_binary(rows, "sub", 0, 1, 2)
    assert result == ["100,50,50", "200,75,125"]

def test_aggregate_csv_column():
    rows = ["Header,Value", "A,10", "B,20", "C,30"]
    # Sum of col 1 skipping header
    sum_res = chunkflow_core.aggregate_csv_column(rows, "sum", 1, True)
    assert sum_res == 60.0
    
    avg_res = chunkflow_core.aggregate_csv_column(rows, "average", 1, True)
    assert avg_res == 20.0

def test_concat_csv_rows_columns():
    rows = ["John,Doe", "Jane,Smith"]
    result = chunkflow_core.concat_csv_rows_columns(rows, 0, 1, " ", 2)
    assert result == ['John,Doe,John Doe', 'Jane,Smith,Jane Smith']

def test_trim_csv_rows_column():
    rows = ["  Amber  ,25", " Smith ,30"]
    result = chunkflow_core.trim_csv_rows_column(rows, 0)
    assert result == ["Amber,25", "Smith,30"]

def test_checkpointing(tmp_path):
    records = ["A,1", "B,2", "C,3", "D,4"]
    out_csv = str(tmp_path / "out.csv")
    log_file = str(tmp_path / "run.log")
    checkpoint_file = str(tmp_path / "run.checkpoint")
    
    # Process 2 chunks
    def transform(x):
        # simulate failure on chunk 1
        if x.startswith("C"):
            raise ValueError("Simulated failure")
        return x + ",ok"
        
    chunkflow_core.process(records, transform, out_csv, log_file, chunk_size=2, checkpoint_path=checkpoint_file, resume=False)
    
    # Assert chunk 0 is done, chunk 1 failed
    with open(out_csv, "r") as f:
        out_content = f.read()
    assert "A,1,ok" in out_content
    assert "C,3,ok" not in out_content
    
    # Now resume with a fixed transform
    def fixed_transform(x):
        return x + ",fixed"
        
    chunkflow_core.process(records, fixed_transform, out_csv, log_file, chunk_size=2, checkpoint_path=checkpoint_file, resume=True)
    
    # Verify everything
    with open(out_csv, "r") as f:
        out_content = f.read()
    assert "A,1,ok" in out_content
    assert "B,2,ok" in out_content
    assert "C,3,fixed" in out_content
    assert "D,4,fixed" in out_content
