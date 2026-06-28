#!/bin/bash
#SBATCH --job-name=ptb110_stfc_month
#SBATCH --partition=standard
#SBATCH --account=ncas_radar
#SBATCH --qos=standard
#SBATCH --time=02:00:00
#SBATCH --mem=8G
#SBATCH --array=5
#SBATCH --output=logs/ptb110_stfc_month_%A_%a.out
#SBATCH --error=logs/ptb110_stfc_month_%A_%a.err

# Year to process — override at submission time with:
#   sbatch --export=YEAR=2025 slurm_process_stfc_month.sh
YEAR=${YEAR:-2025}

MONTH=${SLURM_ARRAY_TASK_ID}

# Load conda environment
source /home/users/cjwalden/miniforge3/etc/profile.d/conda.sh
conda activate cao_3_11

echo "Python version: $(python --version)"
echo "chilbolton-pressure-utils version: $(python -c 'import chilbolton_pressure_utils; print(chilbolton_pressure_utils.__version__)')"
echo "Working directory: $(pwd)"

RAW_DATA_BASE=/gws/ssde/j25a/chil_atmos/raw_data/cao-surface-met/data/long-term/new_daily_split
OUTPUT_BASE=/gws/ssde/j25a/chil_atmos/processing/stfc-pressure-1/data/20240401_longterm

echo "Processing ${YEAR}-$(printf '%02d' ${MONTH}) (CR1000X STFC)"
process-ptb110-month-stfc -y ${YEAR} -m ${MONTH} \
    --raw-data-base ${RAW_DATA_BASE} \
    --output-base ${OUTPUT_BASE}

echo "Done ${YEAR}-$(printf '%02d' ${MONTH})"
