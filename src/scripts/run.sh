#!/bin/bash
CONDA_PATH="$HOME/anaconda3/etc/profile.d/conda.sh"
REPO="/Users/toby/Dev/lionel/src"

source "$CONDA_PATH"
conda activate lionel
cd $REPO
python -m dags.dataload_tasks
python -m dags.prediction_tasks
conda deactivate