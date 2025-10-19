#!/bin/bash
# ============================================
# Descripción: Itera sobre valores de "part_chunk" en un archivo YAML
#              y ejecuta uv run main.py para cada chunk.
# ============================================

set -euo pipefail

YAML_FILE="./pipelines/limpieza/raw_messages.yaml"
MAIN_SCRIPT="./main.py"
START=0
END=9

if [[ ! -f "$YAML_FILE" ]]; then
    echo " No se encontró el archivo YAML en: $YAML_FILE"
    exit 1
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
    echo " No se encontró el archivo main.py en: $MAIN_SCRIPT"
    exit 1
fi

for i in $(seq $START $END); do
    echo " Ejecutando chunk $i..."

    sed -i -E "s/^([[:space:]]*part_chunk:)[[:space:]]*[0-9]+/\1 ${i}/" "$YAML_FILE"

    uv run "$MAIN_SCRIPT" --yaml $YAML_FILE

    echo " Chunk $i completado."
done

echo " Todos los chunks se ejecutaron correctamente."