#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

# Parámetros con valores predeterminados
SCRIPT_PATH=${1:-scripts/glue/oracle_extraction.py}
INPUT_FILE=${2:-test/input/input.json}
OUTPUT_DIR="test/output/local"

section_header "EJECUCIÓN DE ETL LOCAL"
info_message "Script: $SCRIPT_PATH"
info_message "Input: $INPUT_FILE"

# Validar archivos
validate_file_exists "$SCRIPT_PATH" "script ETL" || exit 1
validate_file_exists "$INPUT_FILE" "archivo de entrada" || exit 1

# Asegurar que el contenedor esté en ejecución
ensure_container_running "glue_local" "$(dirname "$0")/up.sh" || exit 1

# Generar argumentos a partir del archivo de entrada
info_message "Generando argumentos para el job..."
TMP_ARGS_FILE=$(mktemp)
trap 'rm -f "$TMP_ARGS_FILE"' EXIT

python3 scripts/utils/generate_args.py \
    "oracle-extraction-dev" \
    "mado-gtfs-dev-eu-west-1-992807582431-bronze" \
    "$INPUT_FILE" \
    "$TMP_ARGS_FILE" || {
    error_message "Error al generar argumentos"
    exit 1
}

# Crear directorio de salida
mkdir -p "$OUTPUT_DIR"

# Ejecutar script en contenedor
section_header "EJECUTANDO SCRIPT ETL"
highlight_message "Ejecutando script en contenedor..."

docker exec -i glue_local bash -c "cd /home/glue_user/workspace && python3 '$SCRIPT_PATH' $(cat $TMP_ARGS_FILE | tr '\n' ' ')"
RESULT=$?

# Mostrar resultado
if [ $RESULT -eq 0 ]; then
    section_header "EJECUCIÓN COMPLETADA"
    success_message "✓ El proceso ETL ha finalizado correctamente"
else
    section_header "ERROR EN EJECUCIÓN"
    error_message "✗ El proceso ETL ha fallado con código $RESULT"
    info_message "Revise los logs para más detalles"
fi

exit $RESULT
