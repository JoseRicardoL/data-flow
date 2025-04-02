#!/bin/bash
# Script principal para ejecutar todo el proceso de batch
# Este script orquesta todos los pasos del procesamiento

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/batch_utils.sh"

# Verificar parámetros
if [ "$#" -lt 5 ]; then
    error_message "Uso: $0 <bucket> <lambda_name> <macro_job> <macro_stops_job> <region> [batch_size] [max_monitor_checks]"
    error_message "  bucket: Nombre del bucket S3 (obligatorio)"
    error_message "  lambda_name: Nombre de la función Lambda del preprocesador (obligatorio)"
    error_message "  macro_job: Nombre del job Glue para macro (obligatorio)"
    error_message "  macro_stops_job: Nombre del job Glue para macro_stops (obligatorio)"
    error_message "  region: Región AWS (obligatorio)"
    error_message "  batch_size: Número de combinaciones a procesar en paralelo (opcional, por defecto: 5)"
    error_message "  max_monitor_checks: Número máximo de combinaciones a verificar por monitoreo (opcional, por defecto: 20)"
    exit 1
fi

BUCKET="$1"
LAMBDA_NAME="$2"
MACRO_JOB="$3"
MACRO_STOPS_JOB="$4"
REGION="$5"
BATCH_SIZE="${6:-5}"
MAX_MONITOR_CHECKS="${7:-20}"

section_header "PROCESO BATCH COMPLETO DE GTFS"
info_message "Bucket: ${BUCKET}"
info_message "Función Lambda: ${LAMBDA_NAME}"
info_message "Job Macro: ${MACRO_JOB}"
info_message "Job MacroStops: ${MACRO_STOPS_JOB}"
info_message "Región: ${REGION}"
info_message "Tamaño de lote: ${BATCH_SIZE}"
info_message "Máximo de verificaciones por monitoreo: ${MAX_MONITOR_CHECKS}"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Paso 1: Descubrir combinaciones
section_header "1. DESCUBRIMIENTO DE COMBINACIONES"
info_message "Ejecutando descubrimiento..."

# Ejecutar script de descubrimiento
"$(dirname "$0")/discover.sh" "${BUCKET}" "${REGION}"
DISCOVERY_RESULT=$?

if [ $DISCOVERY_RESULT -ne 0 ]; then
    error_message "Error en el proceso de descubrimiento"
    exit 1
fi

# Paso 2: Preprocesar combinaciones
section_header "2. PREPROCESAMIENTO"
info_message "Ejecutando preprocesamiento..."

# Ejecutar script de preprocesamiento
"$(dirname "$0")/process.sh" "${LAMBDA_NAME}" "${REGION}" "${BATCH_SIZE}"
PREPROCESS_RESULT=$?

if [ $PREPROCESS_RESULT -ne 0 ]; then
    error_message "Error en el proceso de preprocesamiento"
    exit 1
fi

# Paso 3: Ejecutar jobs de macro y macro_stops
section_header "3. EJECUCIÓN DE JOBS"
info_message "Ejecutando jobs de macro y macro_stops..."

# Ejecutar script de ejecución
"$(dirname "$0")/execute.sh" "${MACRO_JOB}" "${MACRO_STOPS_JOB}" "${BUCKET}" "${REGION}" "${BATCH_SIZE}"
EXECUTE_RESULT=$?

if [ $EXECUTE_RESULT -ne 0 ]; then
    error_message "Error en el proceso de ejecución de jobs"
    exit 1
fi

# Paso 4: Monitorear jobs en ejecución
section_header "4. MONITOREO DE JOBS"
info_message "Iniciando monitoreo de jobs en ejecución..."

# Monitorear hasta que no haya jobs en ejecución o se alcance un límite
MAX_MONITOR_ITERATIONS=30
MONITOR_ITERATION=1

while [ $MONITOR_ITERATION -le $MAX_MONITOR_ITERATIONS ]; do
    info_message "Iteración de monitoreo ${MONITOR_ITERATION}/${MAX_MONITOR_ITERATIONS}"
    
    # Ejecutar script de monitoreo
    "$(dirname "$0")/monitor.sh" "${REGION}" "${MAX_MONITOR_CHECKS}"
    
    # Verificar si hay combinaciones en procesamiento
    PROCESSING_COUNT=$(jq '.combinations | map(select(.status == "processing")) | length' ${STATUS_FILE})
    
    if [ "$PROCESSING_COUNT" -eq 0 ]; then
        success_message "No hay más jobs en ejecución"
        break
    fi
    
    info_message "Todavía hay ${PROCESSING_COUNT} jobs en ejecución"
    info_message "Esperando 60 segundos antes de la siguiente verificación..."
    sleep 60
    
    MONITOR_ITERATION=$((MONITOR_ITERATION+1))
done

if [ $MONITOR_ITERATION -gt $MAX_MONITOR_ITERATIONS ]; then
    warning_message "Se alcanzó el límite máximo de iteraciones de monitoreo"
    warning_message "Algunos jobs pueden seguir en ejecución"
fi

# Paso 5: Generar informes
section_header "5. GENERACIÓN DE INFORMES"
info_message "Generando informes finales..."

# Ejecutar script de informes
"$(dirname "$0")/report.sh"
REPORT_RESULT=$?

if [ $REPORT_RESULT -ne 0 ]; then
    error_message "Error en la generación de informes"
    exit 1
fi

# Mostrar resumen final
section_header "RESUMEN FINAL DEL PROCESO BATCH"
show_status_summary

success_message "Proceso batch completado"
exit 0