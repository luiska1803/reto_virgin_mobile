#!/bin/bash
# ============================================
# Descripción: Itera sobre valores de "part_chunk" en un archivo YAML
#              y ejecuta uv run main.py para cada chunk.
# ============================================

set -euo pipefail

CARGA_CLIENT_YAML_FILE="./carga/carga_client_table.yaml"
CARGA_MESSAGES_YAML_FILE="./carga/carga_messages_table.yaml"
MAIN_SCRIPT="./main.py"
START=0
END=9

if [[ ! -f "$CARGA_CLIENT_YAML_FILE" ]]; then
    echo " No se encontró el archivo YAML en: $CARGA_CLIENT_YAML_FILE"
    exit 1
fi

if [[ ! -f "$CARGA_MESSAGES_YAML_FILE" ]]; then
    echo " No se encontró el archivo YAML en: $CARGA_MESSAGES_YAML_FILE"
    exit 1
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
    echo " No se encontró el archivo main.py en: $MAIN_SCRIPT"
    exit 1
fi

for YAML_FILE in "$CARGA_CLIENT_YAML_FILE" "$CARGA_MESSAGES_YAML_FILE"; do
    for i in $(seq $START $END); do
        echo " Ejecutando chunk $i..."

        sed -i -E "s/^([[:space:]]*part_chunk:)[[:space:]]*[0-9]+/\1 ${i}/" "$YAML_FILE"

        uv run "$MAIN_SCRIPT" --yaml $YAML_FILE

        echo " Chunk $i completado."
    done
done

echo " Todos los chunks se ejecutaron correctamente."