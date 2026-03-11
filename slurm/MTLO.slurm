#!/bin/bash
#SBATCH --job-name=MTL_full
#SBATCH --output=/home/users/swarnick/Latency_ULHPC/MTL_Measurement/slurm/results/logs/mtl_full_%A_%a.out
#SBATCH --error=/home/users/swarnick/Latency_ULHPC/MTL_Measurement/slurm/results/logs/mtl_full_%A_%a.err
#SBATCH --partition=batch
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00

source /etc/profile
module load env/legacy/2020b
module load tools/Singularity/3.8.1

set -euo pipefail
export LC_ALL=C
export LANG=C

PROJECT_DIR=/home/users/swarnick/Latency_ULHPC/MTL_Measurement
CONTAINER=$PROJECT_DIR/rust_python.sif
DATE_LIST=$PROJECT_DIR/slurm/full_dates_2018_2024.txt
RUST_BIN=$HOME/.cargo/target/release/lob_parser

export MTL_SCRATCH_ROOT=/scratch/users/swarnick/MTL_Measurement
export MTL_LOB_ROOT=$MTL_SCRATCH_ROOT/lob_files
export MTL_DB_ROOT=$MTL_SCRATCH_ROOT/sql_db
export MTL_LOG_ROOT=$MTL_SCRATCH_ROOT/logs
export MTL_STATUS_ROOT=$MTL_SCRATCH_ROOT/status
export MTL_DONE_ROOT=$MTL_STATUS_ROOT/completed
export MTL_FAIL_ROOT=$MTL_STATUS_ROOT/failed
export MTL_METRICS_ROOT=$MTL_STATUS_ROOT/metrics
export MTL_DB_PATH=

mkdir -p \
  "$MTL_LOB_ROOT" \
  "$MTL_DB_ROOT" \
  "$MTL_LOG_ROOT" \
  "$MTL_DONE_ROOT" \
  "$MTL_FAIL_ROOT" \
  "$MTL_METRICS_ROOT" \
  "$PROJECT_DIR/slurm/results/logs"

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

if [[ ! -f "$CONTAINER" ]]; then
  echo "ERROR: container not found at $CONTAINER"
  exit 2
fi

if [[ ! -x "$RUST_BIN" ]]; then
  echo "ERROR: Rust binary not found at $RUST_BIN"
  exit 2
fi

if [[ ! -f "$DATE_LIST" ]]; then
  echo "ERROR: date list not found at $DATE_LIST"
  exit 2
fi

DATE_OFFSET=${DATE_OFFSET:-0}
LINE_NO=$((DATE_OFFSET + SLURM_ARRAY_TASK_ID))
DATE=$(sed -n "${LINE_NO}p" "$DATE_LIST")

if [[ -z "${DATE:-}" ]]; then
  echo "No date assigned to array task ${SLURM_ARRAY_TASK_ID} (line ${LINE_NO})"
  exit 1
fi

export MTL_DB_PATH="$MTL_DB_ROOT/${DATE}.sqlite"

DATE_DIR="$MTL_LOB_ROOT/$DATE"
DONE_MARKER="$MTL_DONE_ROOT/${DATE}.done"
FAIL_MARKER="$MTL_FAIL_ROOT/${DATE}.fail"
METRICS_FILE="$MTL_METRICS_ROOT/${DATE}.tsv"
DATE_LOG="$MTL_LOG_ROOT/${DATE}_job${SLURM_JOB_ID}_task${SLURM_ARRAY_TASK_ID}.log"

cd "$PROJECT_DIR"
exec > >(tee -a "$DATE_LOG") 2>&1

echo "============================================================"
echo "DATE:         $DATE"
echo "JOB:          ${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
echo "NODE:         ${SLURM_NODELIST:-unknown}"
echo "CPUS/TASK:    ${SLURM_CPUS_PER_TASK:-unknown}"
echo "PROJECT_DIR:  $PROJECT_DIR"
echo "CONTAINER:    $CONTAINER"
echo "RUST_BIN:     $RUST_BIN"
echo "LOB_ROOT:     $MTL_LOB_ROOT"
echo "DB_PATH:      $MTL_DB_PATH"
echo "START:        $(date --iso-8601=seconds)"
echo "============================================================"

if [[ -f "$DONE_MARKER" ]]; then
  echo "Already completed: $DATE"
  exit 0
fi

rm -rf "$DATE_DIR"
rm -f "$FAIL_MARKER"
rm -f "$MTL_DB_PATH"
rm -f "$METRICS_FILE"

START_TS=$(date +%s)

# init shard
singularity exec \
  --bind /work/projects:/work/projects \
  --bind /scratch:/scratch \
  --bind "$HOME:$HOME" \
  --bind "$PROJECT_DIR:$PROJECT_DIR" \
  --pwd "$PROJECT_DIR" \
  "$CONTAINER" \
  python3 - <<'PY'
import os, sqlite3
db = os.environ["MTL_DB_PATH"]
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("""
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
conn.close()
print(f"Initialized DB: {db}")
PY

echo "---- Rust parse: $DATE ----"
PARSE_START=$(date +%s)
singularity exec \
  --bind /work/projects:/work/projects \
  --bind /scratch:/scratch \
  --bind "$HOME:$HOME" \
  --bind "$PROJECT_DIR:$PROJECT_DIR" \
  --pwd "$PROJECT_DIR" \
  "$CONTAINER" \
  "$RUST_BIN" "$DATE"
PARSE_END=$(date +%s)
PARSE_SECONDS=$((PARSE_END - PARSE_START))

if [[ ! -d "$DATE_DIR" ]]; then
  echo "Expected parsed directory missing after Rust stage: $DATE_DIR"
  touch "$FAIL_MARKER"
  exit 1
fi

CSV_COUNT=$(find "$DATE_DIR" -maxdepth 1 -type f -name '*.csv' | wc -l | tr -d ' ')
DATE_SIZE_BYTES=$(du -sb "$DATE_DIR" | awk '{print $1}')
DATE_SIZE_HUMAN=$(du -sh "$DATE_DIR" | awk '{print $1}')

echo "CSV_COUNT after parse: $CSV_COUNT"
echo "Parsed dir size: $DATE_SIZE_HUMAN"
echo "Parse time (s): $PARSE_SECONDS"

if [[ "$CSV_COUNT" -eq 0 ]]; then
  echo "No CSVs produced for $DATE"
  touch "$FAIL_MARKER"
  exit 1
fi

echo "---- Python latency analysis: $DATE ----"
ANALYSIS_START=$(date +%s)
singularity exec \
  --bind /work/projects:/work/projects \
  --bind /scratch:/scratch \
  --bind "$HOME:$HOME" \
  --bind "$PROJECT_DIR:$PROJECT_DIR" \
  --pwd "$PROJECT_DIR" \
  "$CONTAINER" \
  python3 -m python.entrypoints.latency_rust_interface "$DATE"
ANALYSIS_END=$(date +%s)
ANALYSIS_SECONDS=$((ANALYSIS_END - ANALYSIS_START))

ROW_COUNT=$(
singularity exec \
  --bind /work/projects:/work/projects \
  --bind /scratch:/scratch \
  --bind "$HOME:$HOME" \
  --bind "$PROJECT_DIR:$PROJECT_DIR" \
  --pwd "$PROJECT_DIR" \
  "$CONTAINER" \
  python3 - <<'PY'
import os, sqlite3
db = os.environ["MTL_DB_PATH"]
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM results")
print(cur.fetchone()[0])
conn.close()
PY
)

DB_SIZE_BYTES=$(du -sb "$MTL_DB_PATH" | awk '{print $1}')
DB_SIZE_HUMAN=$(du -sh "$MTL_DB_PATH" | awk '{print $1}')
END_TS=$(date +%s)
TOTAL_SECONDS=$((END_TS - START_TS))

echo "ROW_COUNT in shard: $ROW_COUNT"
echo "DB size: $DB_SIZE_HUMAN"
echo "Analysis time (s): $ANALYSIS_SECONDS"
echo "Total time (s): $TOTAL_SECONDS"

if [[ "$ROW_COUNT" -le 0 ]]; then
  echo "No rows inserted for $DATE"
  touch "$FAIL_MARKER"
  printf "%s\tFAIL\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\n" \
    "$DATE" "$PARSE_SECONDS" "$ANALYSIS_SECONDS" "$TOTAL_SECONDS" \
    "$CSV_COUNT" "0" "$DB_SIZE_BYTES" \
    "$(date --date="@$START_TS" --iso-8601=seconds)" \
    "$(date --date="@$END_TS" --iso-8601=seconds)" > "$METRICS_FILE"
  exit 1
fi

if [[ -d "$DATE_DIR" ]]; then
  echo "Parsed directory still exists after Python stage; removing it now."
  rm -rf "$DATE_DIR"
fi

touch "$DONE_MARKER"
rm -f "$FAIL_MARKER"

printf "%s\tOK\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\n" \
  "$DATE" "$PARSE_SECONDS" "$ANALYSIS_SECONDS" "$TOTAL_SECONDS" \
  "$CSV_COUNT" "$ROW_COUNT" "$DB_SIZE_BYTES" \
  "$(date --date="@$START_TS" --iso-8601=seconds)" \
  "$(date --date="@$END_TS" --iso-8601=seconds)" > "$METRICS_FILE"

echo "SUCCESS: $DATE"
echo "DONE marker: $DONE_MARKER"
echo "END: $(date --iso-8601=seconds)"
echo "============================================================"