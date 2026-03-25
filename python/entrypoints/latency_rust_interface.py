import os
import sys
import sqlite3
import multiprocessing as mp
import shutil
from ..hftf.latency_analysis import process_file


def _process_file_safe(filename):
    try:
        return filename, process_file(filename), None
    except Exception as exc:
        return filename, [], str(exc)


def process_files_for_date(date, project_root):
    LOB_ROOT = os.environ.get("MTL_LOB_ROOT", "data/lob_files")
    folder = os.path.join(LOB_ROOT, date)
    datedir = folder
    db_path = os.environ.get(
        "MTL_DB_PATH",
        os.path.join(project_root, "results", "sql_db", "data.db")
    )

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
        results = pool.map(_process_file_safe, filenames)

    failures = [(name, err) for name, _rows, err in results if err is not None]
    if failures:
        print(f"Encountered {len(failures)} failed files on {date}")
        for name, err in failures[:10]:
            print(f"  FAILED: {name} :: {err}")
        raise RuntimeError(f"Aborting {date}: file-level failures detected")

    flattened = [row for _name, sublist, _err in results for row in sublist]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            ticker TEXT,
            date TEXT,
            latency INTEGER,
            eod_profit REAL,
            std_profit REAL,
            trade_count INTEGER,
            max_price REAL,
            min_price REAL,
            avg_trend_length REAL,
            num_trends INTEGER,
            efficient_count INTEGER,
            event_count INTEGER
        )
        """)

        conn.commit()

        if not flattened:
            print(f"No results generated for {date}")
            return

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
    shutil.rmtree(datedir, ignore_errors=True)

    print(f"Deleted lob_files for {date}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: latency_rust_interface.py YYYYMMDD")
        sys.exit(1)

    date = sys.argv[1]
    project_root = os.getcwd()
    process_files_for_date(date, project_root)