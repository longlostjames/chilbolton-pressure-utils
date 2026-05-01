#!/bin/bash
#SBATCH --job-name=ptb110_qlooks
#SBATCH --partition=standard
#SBATCH --account=ncas_radar
#SBATCH --qos=standard
#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --array=2019-2026
#SBATCH --output=logs/ptb110_qlooks_%A_%a.out
#SBATCH --error=logs/ptb110_qlooks_%A_%a.err

YEAR=${SLURM_ARRAY_TASK_ID}

# Load conda environment
source /home/users/cjwalden/miniforge3/etc/profile.d/conda.sh
conda activate cao_3_11

INPUT_DIR=/gws/pw/j07/ncas_obs_vol2/cao/processing/ncas-pressure-1/data/long-term/level1a/
OUTPUT_DIR=/gws/pw/j07/ncas_obs_vol2/cao/processing/ncas-pressure-1/data/long-term/level1a/quicklooks/

echo "Generating quicklooks for year ${YEAR} (CR1000X)"
make-ptb110-quicklooks -y ${YEAR} -i ${INPUT_DIR} -o ${OUTPUT_DIR}

echo "Done year ${YEAR}"
