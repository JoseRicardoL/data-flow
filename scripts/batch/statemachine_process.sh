#!/bin/bash
# Script para procesar combinaciones GTFS usando la máquina de estados

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

# Verificar parámetros
if [ "$#" -lt 3 ]; then
    error_message "Uso: $0 <operation> <state_table> <state_machine_arn> [options]"
    error_message "  operation: Operación a realizar (register, start, summary, reset)"
    error_message "  state_table: Nombre de la tabla DynamoDB de estado"
    error_message "  state_machine_arn: ARN de la máquina de estados"
    error_message ""
    error_message "Opciones:"
    error_message "  --bucket <bucket>               : Bucket S3 (requerido para register y start)"
    error_message "  --combinations-file <file>      : Archivo JSON de combinaciones (default: batch_processing/combinations.json)"
    error_message "  --region <region>               : Región AWS (default: eu-west-1)"
    error_message "  --max-start <num>               : Máximo de ejecuciones a iniciar (default: 1)"
    exit 1
fi

OPERATION="$1"
STATE_TABLE="$2"
STATE_MACHINE_ARN="$3"
shift 3

# Parámetros por defecto
BUCKET=""
COMBINATIONS_FILE="batch_processing/combinations.json"
REGION="eu-west-1"
MAX_START=1

# Procesar opciones adicionales
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --combinations-file)
            COMBINATIONS_FILE="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --max-start)
            MAX_START="$2"
            shift 2
            ;;
        *)
            error_message "Opción desconocida: $1"
            exit 1
            ;;
    esac
done

section_header "PROCESAMIENTO CON MÁQUINA DE ESTADOS"
info_message "Operación: ${OPERATION}"
info_message "Tabla DynamoDB: ${STATE_TABLE}"
info_message "Máquina de estados ARN: ${STATE_MACHINE_ARN}"
if [ -n "$BUCKET" ]; then
    info_message "Bucket S3: ${BUCKET}"
fi
info_message "Archivo de combinaciones: ${COMBINATIONS_FILE}"
info_message "Región: ${REGION}"
info_message "Máximo de ejecuciones a iniciar: ${MAX_START}"

# Verificar dependencias
for cmd in python3 aws jq; do
    if ! command -v $cmd &> /dev/null; then
        error_message "El comando $cmd no está instalado. Instálalo para continuar."
        exit 1
    fi
done

# Verificar si las combinaciones existen antes de ejecutar
if [ "$OPERATION" = "register" ] || [ "$OPERATION" = "start" ]; then
    if [ ! -f "$COMBINATIONS_FILE" ]; then
        error_message "El archivo de combinaciones $COMBINATIONS_FILE no existe."
        info_message "Ejecuta primero 'make discover-gtfs' para generar combinaciones."
        exit 1
    fi
    
    # Verificar si hay combinaciones para procesar
    COMBINATIONS_COUNT=$(jq '.combinations | length' "$COMBINATIONS_FILE")
    if [ "$COMBINATIONS_COUNT" -eq 0 ]; then
        warning_message "No hay combinaciones en el archivo $COMBINATIONS_FILE."
        info_message "Ejecuta 'make discover-gtfs' para descubrir nuevas combinaciones."
        exit 1
    else
        success_message "Se encontraron $COMBINATIONS_COUNT combinaciones para procesar."
    fi
fi

# Ejecutar el script Python según la operación
case "$OPERATION" in
    register)
        if [ -z "$BUCKET" ]; then
            error_message "El parámetro --bucket es requerido para la operación register"
            exit 1
        fi
        
        info_message "Registrando combinaciones en la tabla DynamoDB..."
        python3 "$(dirname "$0")/register_combinations.py" register \
            --bucket "$BUCKET" \
            --state-table "$STATE_TABLE" \
            --state-machine-arn "$STATE_MACHINE_ARN" \
            --combinations-file "$COMBINATIONS_FILE" \
            --region "$REGION"
        ;;
        
    start)
        if [ -z "$BUCKET" ]; then
            error_message "El parámetro --bucket es requerido para la operación start"
            exit 1
        fi
        
        info_message "Iniciando procesamiento de combinaciones..."
        python3 "$(dirname "$0")/register_combinations.py" start \
            --bucket "$BUCKET" \
            --state-table "$STATE_TABLE" \
            --state-machine-arn "$STATE_MACHINE_ARN" \
            --combinations-file "$COMBINATIONS_FILE" \
            --region "$REGION" \
            --max-start "$MAX_START"
        ;;
        
    summary)
        info_message "Obteniendo resumen de procesamiento..."
        python3 "$(dirname "$0")/register_combinations.py" summary \
            --state-table "$STATE_TABLE" \
            --region "$REGION" | jq '.'
        ;;
        
    reset)
        info_message "Restableciendo combinaciones fallidas..."
        python3 "$(dirname "$0")/register_combinations.py" reset \
            --state-table "$STATE_TABLE" \
            --region "$REGION"
        ;;
        
    *)
        error_message "Operación desconocida: $OPERATION"
        exit 1
        ;;
esac

RESULT=$?

if [ $RESULT -eq 0 ]; then
    success_message "Operación completada exitosamente"
else
    error_message "Error al ejecutar la operación"
    exit 1
fi

exit 0