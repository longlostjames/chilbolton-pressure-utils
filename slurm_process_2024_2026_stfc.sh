#!/bin/bash
#SBATCH --job-name=ptb110_stfc
#SBATCH --partition=standard
#SBATCH --account=ncas_radar
#SBATCH --qos=standard
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --array=2024-2026
#SBATCH --output=logs/ptb110_stfc_%A_%a.out
#SBATCH --error=logs/ptb110_stfc_%A_%a.err

YEAR=${SLURM_ARRAY_TASK_ID}

# Load conda environment
source /home/users/cjwalden/miniforge3/etc/profile.d/conda.sh
conda activate cao_3_11

echo "Python version: $(python --version)"
echo "chilbolton-pressure-utils version: $(python -c 'import chilbolton_pressure_utils; print(chilbolton_pressure_utils.__version__)')"
echo "Working directory: $(pwd)"

RAW_DATA_BASE=/gws/pw/j07/ncas_obs_vol2/cao/raw_data/met_cao/data/long-term/new_daily_split
OUTPUT_BASE=/gws/pw/j07/ncas_obs_vol2/cao/processing/ncas-pressure-1/data/long-term/level1a

echo "Processing year ${YEAR} (CR1000X STFC)"
process-ptb110-year-stfc -y ${YEAR} \
    --raw-data-base ${RAW_DATA_BASE} \
    --output-base ${OUTPUT_BASE}

echo "Done year ${YEAR}"
