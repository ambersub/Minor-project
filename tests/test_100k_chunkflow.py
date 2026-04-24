import os
import time
import chunkflow_core

def transform_row(row):
    """
    Simple transformation: 
    Parse the CSV row, do some basic math on val_float and val_int,
    and return the processed string.
    """
    # row format: id,val_float,val_int,category,label
    fields = row.strip().split(",")
    if len(fields) < 5:
        return row + ",error"
    
    try:
        val_float = float(fields[1])
        val_int = int(fields[2])
        # Arbitrary computation
        new_val = val_float * val_int
        return f"{row.strip()},{new_val:.2f}"
    except ValueError:
        return row.strip() + ",error"

def main():
    csv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "chunkflow_test_100k.csv")
    out_csv = "out_chunkflow_100k.csv"
    log_file = "run_chunkflow_100k.log"
    checkpoint_file = "run_chunkflow_100k.checkpoint"
    
    print(f"Reading {csv_file}...")
    with open(csv_file, "r", encoding="utf-8") as f:
        header = f.readline()
        records = f.readlines()
        
    # Remove newlines
    records = [r.strip() for r in records if r.strip()]
    
    print(f"Loaded {len(records)} records. Starting ChunkFlow processing...")
    
    start_time = time.time()
    
    # Process using chunkflow_core with multiple threads
    summary = chunkflow_core.process(
        records,
        transform_row,
        out_csv,
        log_file,
        chunk_size=5000,
        num_threads=4, # Use 4 OpenMP threads
        checkpoint_path=checkpoint_file,
        resume=False
    )
    
    end_time = time.time()
    
    print("\n--- ChunkFlow Execution Summary ---")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    print(f"Total chunks: {summary['total_chunks']}")
    print(f"Completed chunks: {summary['done']}")
    print(f"Failed chunks: {summary['failed']}")
    
if __name__ == "__main__":
    main()
