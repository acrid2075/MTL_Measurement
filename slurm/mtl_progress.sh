#!/bin/bash
set -uo pipefail

PROJECT_DIR=/home/users/swarnick/Latency_ULHPC/MTL_Measurement
DATE_LIST=$PROJECT_DIR/slurm/full_dates_2018_2024.txt
STATUS_ROOT=/scratch/users/swarnick/MTL_Measurement/status
METRICS_ROOT=$STATUS_ROOT/metrics
DONE_ROOT=$STATUS_ROOT/completed
FAIL_ROOT=$STATUS_ROOT/failed

PARALLELISM=${1:-12}

TOTAL=0
if [[ -f "$DATE_LIST" ]]; then
  TOTAL=$(wc -l < "$DATE_LIST" | tr -d '[:space:]')
fi

DONE=0
if [[ -d "$DONE_ROOT" ]]; then
  DONE=$(find "$DONE_ROOT" -type f -name '*.done' 2>/dev/null | wc -l | tr -d ' ')
fi

FAIL=0
if [[ -d "$FAIL_ROOT" ]]; then
  FAIL=$(find "$FAIL_ROOT" -type f -name '*.fail' 2>/dev/null | wc -l | tr -d ' ')
fi

REMAIN=$((TOTAL - DONE))

AVG_TOTAL=0
if [[ -d "$METRICS_ROOT" ]] && compgen -G "$METRICS_ROOT/*.tsv" > /dev/null; then
  AVG_TOTAL=$(
    awk -F '\t' 'BEGIN{n=0; s=0}
      $2=="OK" {s += $5; n += 1}
      END {if(n>0) printf "%.2f", s/n; else print "0"}' \
      "$METRICS_ROOT"/*.tsv
  )
fi

ETA_HOURS=$(
python3 - <<PY
total = float("${AVG_TOTAL}")
remain = int("${REMAIN}")
parallel = int("${PARALLELISM}")
eta = 0.0 if parallel <= 0 else (total * remain) / parallel / 3600.0
print(f"{eta:.2f}")
PY
)

echo "Total dates:        $TOTAL"
echo "Completed:          $DONE"
echo "Failed markers:     $FAIL"
echo "Remaining:          $REMAIN"
echo "Avg seconds/date:   $AVG_TOTAL"
echo "ETA hours @ x$PARALLELISM: $ETA_HOURS"
echo
squeue -u "$USER" -o "%.18i %.12P %.20j %.8T %.10M %.6D %R" || true