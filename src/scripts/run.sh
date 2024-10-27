#!/bin/bash
CONDA_PATH="$HOME/anaconda3/etc/profile.d/conda.sh"
ROOT="$HOME/Dev/lionel"
SRC="$ROOT/src"
export LUIGI_CONFIG_PATH="$ROOT/luigi.cfg"

source "$CONDA_PATH"
conda activate lionel
cd $SRC

# Start the central scheduler in the background
luigid & 

# Run the data load and prediction tasks
python -m dags.dataload_tasks 
python -m dags.prediction_tasks

# end the schedulder
PID=$(ps aux | grep '[l]uigid' | awk '{print $2}')
kill "$PID"

conda deactivate