#!/bin/bash
CONDA_PATH="$HOME/anaconda3/etc/profile.d/conda.sh"
REPO="/Users/toby/Dev/lionel/"

source "$CONDA_PATH"
conda activate lionel
cd $REPO
python -m lionel.dags.dataload_tasks
conda deactivate