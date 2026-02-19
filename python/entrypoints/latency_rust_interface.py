import os
import sys
import sqlite3
import multiprocessing as mp
from ..hftf.latency_analysis import process_file
import time as time
import shutil

def process_files_for_date(date, basedir, db_path):
    """Process all files for a given date."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"Processing date: {date}")
        datedir = os.path.join(basedir, 'Latency_ULHPC/MTL_Measurement/data/lob_files', date)
        filenames = [os.path.join(datedir, stock) for stock in os.listdir(datedir)]

        num_cpus = max(1, int(os.getenv("SLURM_CPUS_PER_TASK", 1)))
        with mp.Pool(processes=num_cpus) as pool:
            results = pool.map(process_file, filenames)

        batch_size = 100  # Batch size for database inserts
        flattened_results = [j for k in results for j in k]
        for i in range(0, len(flattened_results), batch_size):
            batch = flattened_results[i:i + batch_size]
            cursor.executemany(
                "INSERT INTO results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 
                batch
            )
        conn.commit()
        print(f"Finished processing date: {date}")
        
        shutil.rmtree(datedir)
        print(f"Deleted {datedir}")

    except Exception as e:
        print(f"Error processing date {date}: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 2:
        print("Usage: python latency_rust_interface.py <date>")
        sys.exit(1)

    date = sys.argv[1]
    basedir = os.getcwd()
    db_path = "/home/users/swarnick/Latency_ULHPC/MTL_Measurement/results/sql_db/data.db"

    process_files_for_date(date, basedir, db_path)
    end = time.time() - start
    print(f'Finished analyzing {date}. {end}')
