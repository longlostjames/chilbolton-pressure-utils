#!/bin/bash
#SBATCH --job-name=ptb110_qlooks_f5
#SBATCH --partition=standard
#SBATCH --account=ncas_radar
#SBATCH --qos=standard
#SBATCH --time=4:00:00
#SBATCH --mem=8G
#SBATCH --array=2016
#SBATCH --output=logs/ptb110_qlooks_f5_%A_%a.out
#SBATCH --error=logs/ptb110_qlooks_f5_%A_%a.err

YEAR=${SLURM_ARRAY_TASK_ID}

source activate cao_3_11

INPUT_DIR=/gws/pw/j07/ncas_obs_vol2/cao/processing/ncas-pressure-1/data/long-term/level1_f5/
OUTPUT_DIR=/gws/pw/j07/ncas_obs_vol2/cao/processing/ncas-pressure-1/data/long-term/level1_f5/quicklooks/

echo "Generating quicklooks for year ${YEAR} (Format5)"
make-ptb110-quicklooks -y ${YEAR} -i ${INPUT_DIR} -o ${OUTPUT_DIR}

echo "Done year ${YEAR}"
