#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

LOCAL_PATH=$1
S3_BUCKET=$2
GLUE_SCRIPTS_PATH=$3

# Validar argumentos requeridos
validate_required_var "LOCAL_PATH" "$LOCAL_PATH" || exit 1
validate_required_var "S3_BUCKET" "$S3_BUCKET" || exit 1
validate_required_var "GLUE_SCRIPTS_PATH" "$GLUE_SCRIPTS_PATH" || exit 1

# Mostrar información del script
show_script_info "CARGA DE SCRIPT A S3" "Subir script a bucket S3" \
    "Archivo local" "$LOCAL_PATH" \
    "Bucket S3" "$S3_BUCKET" \
    "Ruta destino" "$GLUE_SCRIPTS_PATH"

section_header "VALIDACIÓN DE ARCHIVO"
# Verificar que el archivo existe
if [ ! -f "$LOCAL_PATH" ]; then
    error_message "El archivo $LOCAL_PATH no existe"
    finalize_script 1 "" "CARGA FALLIDA"
    exit 1
fi

# Obtener el nombre del script
SCRIPT_NAME=$(basename $LOCAL_PATH)
S3_PATH="${GLUE_SCRIPTS_PATH}/${SCRIPT_NAME}"

section_header "CARGA A S3"
info_message "Subiendo archivo a S3..."

aws s3 cp $LOCAL_PATH s3://${S3_BUCKET}/${S3_PATH}
show_result $? "Archivo cargado correctamente: s3://${S3_BUCKET}/${S3_PATH}" "Error al cargar el archivo"

# Finalizar el script
finalize_script 0 "CARGA COMPLETADA EXITOSAMENTE" ""
exit 0
