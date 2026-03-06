#!/bin/bash
#SBATCH --job-name=latency_pipeline
#SBATCH --output=results/logs/latency_%A_%a.log
#SBATCH --error=results/logs/latency_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=16
#SBATCH --ntasks=1
#SBATCH --partition=batch
#SBATCH --array=1-7

export LC_ALL=C
export LANG=C

set -eo pipefail
source /etc/profile

module load env/legacy/2020b
module load tools/Singularity/3.8.1

PROJECT_DIR=/home/users/swarnick/Latency_ULHPC/MTL_Measurement
CONTAINER=/home/users/swarnick/rust_python.sif
LOB_DIR=$PROJECT_DIR/data/lob_files

YEARS=(2018 2019 2020 2021 2022 2023 2024)
YEAR=${YEARS[$((SLURM_ARRAY_TASK_ID-1))]}

cd "$PROJECT_DIR"

mkdir -p results/logs

echo "===================================="
echo "Year: $YEAR"
echo "Node: $SLURM_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Start: $(date)"
echo "===================================="

# Collect dates for this year only
DATES=$(ls /work/projects/nasdaq/itchdata | \
    grep "^S${YEAR}[0-9]\{4\}-v50.txt.gz$" | \
    sed 's/S\(.*\)-v50.txt.gz/\1/' | \
    sort)

if [ -z "$DATES" ]; then
    echo "No ITCH files found for $YEAR"
    exit 0
fi

for DATE in $DATES; do

    echo "------------------------------------"
    echo "Processing $DATE"
    echo "------------------------------------"

    # 1) Rust parse
    singularity exec \
        --bind /work/projects:/work/projects \
        --bind "$PROJECT_DIR:$PROJECT_DIR" \
        --bind "$HOME:$HOME" \
        --pwd "$PROJECT_DIR" \
        "$CONTAINER" \
        cargo run --release -- "$DATE"

    # 2) Python latency analysis
    singularity exec \
        --bind "$PROJECT_DIR:$PROJECT_DIR" \
        --bind "$HOME:$HOME" \
        --pwd "$PROJECT_DIR" \
        "$CONTAINER" \
        python -m python.entrypoints.latency_rust_interface "$DATE"

    # 3) Delete CSVs to save disk
    rm -rf "$LOB_DIR/$DATE"

    echo "Finished $DATE"

done

echo "===================================="
echo "Completed Year $YEAR"
echo "End: $(date)"
echo "===================================="
