import os
import time
from multiprocessing import Pool

def transform_row(row):
    """
    Simple transformation: 
    Parse the CSV row, do some basic math on val_float and val_int,
    and return the processed string.
    """
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
    out_csv = "out_multiprocessing_100k.csv"
    
    print(f"Reading {csv_file}...")
    with open(csv_file, "r", encoding="utf-8") as f:
        header = f.readline()
        records = f.readlines()
        
    records = [r.strip() for r in records if r.strip()]
    
    print(f"Loaded {len(records)} records. Starting Multiprocessing...")
    
    start_time = time.time()
    
    # Process using Python multiprocessing Pool
    with Pool(processes=4) as pool:
        processed_records = pool.map(transform_row, records)
        
    with open(out_csv, "w", encoding="utf-8") as f:
        for rec in processed_records:
            f.write(rec + "\n")
            
    end_time = time.time()
    
    print("\n--- Python Multiprocessing Execution Summary ---")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    print(f"Processed {len(processed_records)} records")

if __name__ == "__main__":
    main()
