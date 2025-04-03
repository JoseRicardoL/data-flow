#!/bin/bash
# Script para descubrir combinaciones de datos GTFS en S3
# Encuentra todas las combinaciones disponibles de explotación, contrato y versión

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/bash_utils.sh"

# Verificar parámetros
if [ "$#" -lt 1 ]; then
    error_message "Uso: $0 <bucket_name> [region] [env]"
    error_message "  bucket_name: Nombre del bucket S3 (obligatorio)"
    error_message "  region: Región AWS (opcional, por defecto: eu-west-1)"
    error_message "  env: Entorno (dev|test|prod) (opcional, por defecto: dev)"
    exit 1
fi

BUCKET="$1"
REGION="${2:-eu-west-1}"
ENV="${3:-dev}"

section_header "DESCUBRIMIENTO DE DATOS GTFS"
info_message "Bucket: ${BUCKET}"
info_message "Región: ${REGION}"
info_message "Entorno: ${ENV}"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Ejecutar script de descubrimiento en Python
# CORREGIDO: Ahora se usa correctamente el parámetro --region
python3 "$(dirname "$0")/../discovery.py" "${BUCKET}" --region "${REGION}"
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    error_message "Error en el proceso de descubrimiento"
    exit 1
fi

# Verificar si se encontraron combinaciones
if [ -f "${COMBINATIONS_FILE}" ]; then
    TOTAL_COMBINATIONS=$(jq '.total' ${COMBINATIONS_FILE})
    success_message "Se descubrieron ${TOTAL_COMBINATIONS} combinaciones para procesar"
else
    error_message "No se generó el archivo de combinaciones"
    exit 1
fi

# Actualizar el estado
update_status

# Mostrar resumen
show_status_summary

exit 0