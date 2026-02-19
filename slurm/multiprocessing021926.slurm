#!/bin/bash
#SBATCH --job-name=latency_analysis
#SBATCH --output=logs/latency_analysis_%j.log
#SBATCH --error=logs/latency_analysis_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=16
#SBATCH --ntasks=1
#SBATCH --partition=batch

set -euo pipefail

source /etc/profile

# Load python environment
module load env/legacy/2020b
module load tools/Singularity/3.8.1

PROJECT_DIR=/home/users/swarnick/Latency_ULHPC/MTL_Measurement
PYTHON_DIR=$PROJECT_DIR/python
LOB_DIR=$PROJECT_DIR/data/lob_files

cd "$PROJECT_DIR"

echo "=============================================="
echo "Latency Analysis Job Started"
echo "Node: $SLURM_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Start time: $(date)"
echo "=============================================="

# Ensure logs directory exists
mkdir -p logs

# Ensure lob_files exists
if [ ! -d "$LOB_DIR" ]; then
    echo "ERROR: lob_files directory does not exist."
    exit 1
fi

# Find all day folders
DATES=$(ls "$LOB_DIR")

if [ -z "$DATES" ]; then
    echo "No days found in lob_files."
    exit 0
fi

for DATE in $DATES; do
    echo "----------------------------------------------"
    echo "Processing date: $DATE"
    echo "----------------------------------------------"

    python -m python.entrypoints.latency_rust_interface "$DATE"

    echo "Finished date: $DATE"
done

echo "==============================
