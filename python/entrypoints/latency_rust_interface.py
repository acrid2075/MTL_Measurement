import os
import sys
import sqlite3
import multiprocessing as mp
import shutil
from ..hftf.latency_analysis import process_file


def process_files_for_date(date, project_root):

    datedir = os.path.join(project_root, "data/lob_files", date)
    db_path = os.path.join(project_root, "results/sql_db/data.db")

    if not os.path.isdir(datedir):
        print(f"No directory for {date}, skipping.")
        return

    print(f"\nProcessing date: {date}")

    filenames = [
        os.path.join(datedir, f)
        for f in os.listdir(datedir)
        if f.endswith(".csv")
    ]

    if not filenames:
        print(f"No CSV files found for {date}")
        return

    num_cpus = max(1, int(os.getenv("SLURM_CPUS_PER_TASK", 1)))

    # Run multiprocessing
    with mp.Pool(processes=num_cpus) as pool:
        results = pool.map(process_file, filenames)

    flattened = [row for sublist in results for row in sublist]

    if not flattened:
        print(f"No results generated for {date}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.executemany(
            """
            INSERT INTO results
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            flattened
        )

        conn.commit()
        print(f"Committed results for {date}")

    except Exception as e:
        conn.rollback()
        print(f"Database error on {date}: {e}")
        raise

    finally:
        conn.close()

    # Only delete AFTER successful commit
    shutil.rmtree(datedir)
    print(f"Deleted lob_files for {date}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: latency_rust_interface.py YYYYMMDD")
        sys.exit(1)

    date = sys.argv[1]
    project_root = os.getcwd()
    process_files_for_date(date, project_root)