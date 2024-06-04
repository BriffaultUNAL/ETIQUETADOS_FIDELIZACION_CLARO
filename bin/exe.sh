#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

start_time=$(date +%s)

echo -e "Inicio de ejecucion: $(date)\n"

VENV="$ACT_DIR/venv"
source "$VENV/bin/activate"
python3 "$ACT_DIR/main.py"
deactivate

echo "Process executed"

end_time=$(date +%s)
runtime=$((end_time-start_time))
formatted_runtime=$(date -u -d @"$runtime" +'%M minutos y %S segundos')
echo -e "Fin de ejecucion: $(date)\n"
echo -e "Tiempo de ejecucion: $formatted_runtime \n \n \n"