# Maximum Tolerable Latency Measurement

A reproducible, **Slurm-native** research pipeline for large-scale data parsing and algorithmic analysis using **Rust + Python** on an HPC cluster.

This repository is organized around **job stages and artifacts**, not ad-hoc scripts. It is designed to support:

- Parallel Rust parsing via Slurm job arrays  
- Parallel Python algorithm execution  
- Explicit job dependencies  
- Restartable, failure-tolerant workflows  
- Clean separation between parsing, computation, and visualization  

---

## High-level pipeline

The workflow is divided into **three stages**, each producing immutable outputs:

```text
Stage 1: Parse (Rust)
  raw data → parsed artifacts

Stage 2: Compute (Python)
  parsed artifacts → per-shard results

Stage 3: Post-process (Python)
  results → merged metrics → figures

.
├── data/                    # Raw and intermediate data artifacts
├── python/                  # Algorithms, analysis, visualization
├── results/                 # Outputs (metrics, figures, logs)
├── rust/                    # High-performance parser
└── slurm/                   # Slurm job scripts (no logic)
