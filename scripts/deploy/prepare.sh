#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

STACK_NAME=$1
S3_BUCKET=$2

# Validar argumentos requeridos
validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "S3_BUCKET" "$S3_BUCKET" || exit 1

# Mostrar información del script
show_script_info "PREPARACIÓN DE PRUEBA GLUE" "Generar argumentos para prueba" \
    "Stack" "$STACK_NAME" \
    "Bucket S3" "$S3_BUCKET"

section_header "VERIFICACIÓN DE DIRECTORIOS"
# Verificar que existan las carpetas necesarias
info_message "Verificando estructura de directorios..."
mkdir -p test/input
mkdir -p test/output
mkdir -p test/logs
success_message "Directorios verificados correctamente"

section_header "VERIFICACIÓN DE ARCHIVOS DE ENTRADA"

STATE_INPUT="test/input/state_input.json"
TASK_INPUT="test/input/task_input.json"
PARAMS_INPUT="test/input/parameters.json"

# Verificar si existen los archivos necesarios
if [ ! -f "$STATE_INPUT" ]; then
    warning_message "No se encontró el archivo de State Input: $STATE_INPUT"
    warning_message "Por favor, cree este archivo con el formato adecuado."
    warning_message "Ejemplo de contenido:"
    echo '{
  "status": "success",
  "execution_id": "e77d9118-b401-4cf3-8cf6-2c5df68615f2",
  "P_EMPRESA": "4",
  "P_VERSION": "20250310_20250316",
  "P_CONTR": "1",
  "temp_dir": "GTFS_TEMP/preprocessed/explotation=4/contract=1/version=20250310_20250316/e77d9118-b401-4cf3-8cf6-2c5df68615f2",
  "unique_routes": ["411", "434", "610"],
  "has_shapes": true,
  "has_data": true
}'
    finalize_script 1 "" "PREPARACIÓN FALLIDA"
    exit 1
fi

if [ ! -f "$TASK_INPUT" ]; then
    warning_message "No se encontró el archivo de Task Input: $TASK_INPUT"
    warning_message "Por favor, cree este archivo con el formato adecuado."
    warning_message "Ejemplo de contenido:"
    echo '{
  "JobName": "data-GTFS-data-flow-macro-glue-job",
  "Arguments": {
    "--P_CONTR": "1",
    "--P_EMPRESA": "4",
    "--P_VERSION": "20250310_20250316",
    "--temp_dir": "GTFS_TEMP/preprocessed/explotation=4/contract=1/version=20250310_20250316/e77d9118-b401-4cf3-8cf6-2c5df68615f2",
    "--execution_id": "e77d9118-b401-4cf3-8cf6-2c5df68615f2"
  }
}'
    finalize_script 1 "" "PREPARACIÓN FALLIDA"
    exit 1
fi

# Verificar si existe el archivo de Parameters (opcional)
if [ ! -f "$PARAMS_INPUT" ]; then
    info_message "No se encontró el archivo de Parameters. Creando uno vacío."
    echo '{
  "JobName": "data-GTFS-data-flow-macro-glue-job",
  "Arguments": {
    "--P_EMPRESA.$": "$.P_EMPRESA",
    "--P_VERSION.$": "$.P_VERSION",
    "--P_CONTR.$": "$.P_CONTR",
    "--temp_dir.$": "$.temp_dir",
    "--execution_id.$": "$.execution_id"
  }
}' > "$PARAMS_INPUT"
fi

section_header "GENERACIÓN DE ARGUMENTOS"
info_message "Generando argumentos para el trabajo Glue..."
info_message "Utilizando archivos de entrada:"
info_message "- State Input: $STATE_INPUT"
info_message "- Task Input: $TASK_INPUT"
info_message "- Parameters: $PARAMS_INPUT"

# Ejecutar el script de generación de argumentos
python3 scripts/utils/generate_args.py \
    "$STACK_NAME" \
    "$S3_BUCKET" \
    "$STATE_INPUT" \
    "$TASK_INPUT" \
    "$PARAMS_INPUT" \
    "test/input/args.json"

show_result $? "Argumentos generados correctamente en test/input/args.json" "Error al generar argumentos"

# Asegurarse de que el args.json es válido
if ! command -v jq &>/dev/null || ! jq empty test/input/args.json 2>/dev/null; then
    error_message "El archivo args.json generado no es un JSON válido"
    finalize_script 1 "" "PREPARACIÓN FALLIDA"
    exit 1
fi

# Mostrar vista previa de los argumentos
section_header "VISTA PREVIA DE ARGUMENTOS"
info_message "Estructura de argumentos generados:"
if command -v jq &>/dev/null; then
    jq . test/input/args.json | head -20
    TOTAL_LINES=$(wc -l <test/input/args.json)
    if [ $TOTAL_LINES -gt 20 ]; then
        warning_message "... (archivo truncado, contiene $TOTAL_LINES líneas en total)"
    fi
else
    head -20 test/input/args.json
    TOTAL_LINES=$(wc -l <test/input/args.json)
    if [ $TOTAL_LINES -gt 20 ]; then
        warning_message "... (archivo truncado, contiene $TOTAL_LINES líneas en total)"
    fi
fi

# Finalizar el script
finalize_script 0 "ARGUMENTOS PREPARADOS CORRECTAMENTE" ""
success_message "Ahora puede ejecutar: make test"
exit 0