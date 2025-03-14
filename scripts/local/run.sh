#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

# Valores por defecto
SCRIPT_PATH=${1:-scripts/glue/oracle_extraction.py}
INPUT_FILE=${2:-test/input/input.json}

section_header "EJECUCIÓN DE ETL LOCAL"
info_message "Ejecutando ETL Oracle Extraction en entorno local"
info_message "Script: $SCRIPT_PATH"
info_message "Input: $INPUT_FILE"

# Verificar si el contenedor está en ejecución
if ! docker ps | grep -q glue_local; then
    warning_message "El contenedor glue_local no está en ejecución"
    info_message "Iniciando contenedor..."
    "$(dirname "$0")/up.sh"
fi

# Verificar archivo de entrada
if [ ! -f "$INPUT_FILE" ]; then
    error_message "El archivo de entrada $INPUT_FILE no existe"
    exit 1
fi

# Generar argumentos a partir del archivo de entrada
TMP_ARGS_FILE=$(mktemp)

info_message "Generando argumentos desde archivo de entrada..."
python3 scripts/utils/generate_args.py \
    "oracle-extraction-dev" \
    "mado-gtfs-dev-eu-west-1-992807582431-bronze" \
    "$INPUT_FILE" \
    "$TMP_ARGS_FILE"

if [ $? -ne 0 ]; then
    error_message "Error al generar argumentos"
    rm -f "$TMP_ARGS_FILE"
    exit 1
fi

# Crear directorio de salida
mkdir -p test/output/local

section_header "EJECUTANDO SCRIPT"
highlight_message "Ejecutando script en contenedor glue_local..."
docker exec -i glue_local bash -c "cd /home/glue_user/workspace && python3 '$SCRIPT_PATH' $(cat $TMP_ARGS_FILE | tr '\n' ' ')"

RESULT=$?

# Limpiar archivo temporal
rm -f "$TMP_ARGS_FILE"

if [ $RESULT -eq 0 ]; then
    section_header "EJECUCIÓN FINALIZADA"
    success_message "Ejecución completada correctamente"
else
    section_header "ERROR DE EJECUCIÓN"
    error_message "Error durante la ejecución (código $RESULT)"
fi

exit $RESULT
