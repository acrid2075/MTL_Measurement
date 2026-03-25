#!/bin/bash
set -euo pipefail

# Canary runner for MTLO.slurm on ULHPC.
# Usage:
#   bash slurm/canary_run.sh 20241015
#
# What it does:
# 1) Backs up the date list and writes a single canary date.
# 2) Submits one array task of MTLO.slurm.
# 3) Waits for completion.
# 4) Prints key log markers, warning scan, metrics, and DB outlier checks.

PROJECT_DIR=/home/users/swarnick/Latency_ULHPC/MTL_Measurement
DATE_LIST="$PROJECT_DIR/slurm/full_dates_2018_2024.txt"
SLURM_FILE="$PROJECT_DIR/slurm/MTLO.slurm"
SCRATCH_ROOT=/scratch/users/swarnick/MTL_Measurement

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 YYYYMMDD"
  exit 1
fi

DATE="$1"
if [[ ! "$DATE" =~ ^[0-9]{8}$ ]]; then
  echo "ERROR: date must be YYYYMMDD, got '$DATE'"
  exit 1
fi

if [[ ! -f "$DATE_LIST" ]]; then
  echo "ERROR: date list not found at $DATE_LIST"
  exit 2
fi

if [[ ! -f "$SLURM_FILE" ]]; then
  echo "ERROR: slurm file not found at $SLURM_FILE"
  exit 2
fi

cd "$PROJECT_DIR"

if command -v rg >/dev/null 2>&1; then
  SEARCH_TOOL="rg"
else
  SEARCH_TOOL="grep"
fi

BACKUP="${DATE_LIST}.bak.$(date +%Y%m%d_%H%M%S)"
cp "$DATE_LIST" "$BACKUP"
restore_date_list() {
  if [[ -f "$BACKUP" ]]; then
    cp "$BACKUP" "$DATE_LIST"
  fi
}
trap restore_date_list EXIT

printf "%s\n" "$DATE" > "$DATE_LIST"
echo "Wrote canary date list: $DATE_LIST"
echo "Backup saved at: $BACKUP"

DONE_MARKER="$SCRATCH_ROOT/status/completed/${DATE}.done"
FAIL_MARKER="$SCRATCH_ROOT/status/failed/${DATE}.fail"
METRICS_FILE="$SCRATCH_ROOT/status/metrics/${DATE}.tsv"
DB_PATH="$SCRATCH_ROOT/sql_db/${DATE}.sqlite"
DATE_DIR="$SCRATCH_ROOT/lob_files/${DATE}"

# Ensure canary run does real work instead of exiting on stale completion markers.
rm -f "$DONE_MARKER" "$FAIL_MARKER" "$METRICS_FILE" "$DB_PATH"
rm -rf "$DATE_DIR"
echo "Cleared stale artifacts for $DATE"

JOBID=$(sbatch --array=1 "$SLURM_FILE" | awk '{print $4}')
if [[ -z "${JOBID:-}" ]]; then
  echo "ERROR: failed to submit canary job"
  exit 3
fi
echo "Submitted JOBID=$JOBID for date $DATE"

echo "Waiting for job to finish..."
while squeue -j "$JOBID" -h | grep -q .; do
  sleep 5
done
echo "Job $JOBID finished."

SLURM_OUT="$PROJECT_DIR/slurm/results/logs/mtl_full_${JOBID}_1.out"
SLURM_ERR="$PROJECT_DIR/slurm/results/logs/mtl_full_${JOBID}_1.err"
DATE_LOG=$(ls -t "$SCRATCH_ROOT"/logs/"${DATE}"_job"${JOBID}"_task*.log 2>/dev/null | head -n 1 || true)

echo
echo "==== Key stage markers ===="
if [[ -n "${DATE_LOG:-}" && -f "$DATE_LOG" ]]; then
  echo "Using DATE_LOG: $DATE_LOG"
  if [[ "$SEARCH_TOOL" == "rg" ]]; then
    rg -n "Rust parse|Python latency analysis|CSV_COUNT|ROW_COUNT|SUCCESS|No rows inserted|Expected parsed directory missing" "$DATE_LOG" || true
  else
    grep -nE "Rust parse|Python latency analysis|CSV_COUNT|ROW_COUNT|SUCCESS|No rows inserted|Expected parsed directory missing" "$DATE_LOG" || true
  fi
else
  echo "DATE LOG missing for JOBID=$JOBID DATE=$DATE"
fi

echo
echo "==== Warning/Error scan ===="
if [[ "$SEARCH_TOOL" == "rg" ]]; then
  rg -n "WARN:|Failed on|RuntimeError|Traceback|Database error|Aborting|ERROR:" "$DATE_LOG" "$SLURM_OUT" "$SLURM_ERR" 2>/dev/null || true
else
  grep -nE "WARN:|Failed on|RuntimeError|Traceback|Database error|Aborting|ERROR:" "$DATE_LOG" "$SLURM_OUT" "$SLURM_ERR" 2>/dev/null || true
fi

echo
echo "==== Status artifacts ===="
for f in "$DONE_MARKER" "$FAIL_MARKER" "$METRICS_FILE" "$DB_PATH"; do
  if [[ -e "$f" ]]; then
    ls -lh "$f"
  else
    echo "MISSING: $f"
  fi
done

echo
echo "==== Metrics TSV ===="
if [[ -f "$METRICS_FILE" ]]; then
  awk '{print}' "$METRICS_FILE"
fi

echo
echo "==== DB sanity checks ===="
if [[ -f "$DB_PATH" ]]; then
  python3 - <<PY
import sqlite3
db = r"$DB_PATH"
conn = sqlite3.connect(db)
cur = conn.cursor()

print("\\n[row_count]")
for row in cur.execute("SELECT COUNT(*) FROM results"):
    print(row)

print("\\n[top_abs_eod_profit]")
for row in cur.execute("""
    SELECT ticker, latency, eod_profit
    FROM results
    ORDER BY ABS(eod_profit) DESC
    LIMIT 20
"""):
    print(row)

print("\\n[extreme_price_ratio]")
for row in cur.execute("""
    SELECT ticker, latency, max_price, min_price,
           CASE WHEN min_price > 0 THEN max_price/min_price ELSE NULL END AS ratio
    FROM results
    ORDER BY ratio DESC
    LIMIT 20
"""):
    print(row)

conn.close()
PY
fi

echo
echo "Canary completed. Original date list restored from backup."
