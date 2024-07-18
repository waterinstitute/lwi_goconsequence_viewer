#!/bin/bash
source /home/WATER/lpenuelacantor/miniconda3/etc/profile.d/conda.sh
conda activate lwi_vector
BASEPATH="/home/WATER/lpenuelacantor/projects/lwi/consequence_viewer/"
#env_file="$BASEPATH/.env"
#export $(grep -v '^#' $env_file | xargs -d '\n')
echo $PYTHONPATH
hostname -I
export file=$(basename "$1")
export file_name="${file%.*}"
python "$1" "${@:2}" &>>"/home/WATER/lpenuelacantor/projects/lwi/consequence_viewer/logs/vector_pipeline/$file_name-$(date +%F).log"
