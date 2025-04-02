#!/bin/bash
# Script para ejecutar el preprocesador para las combinaciones pendientes
# Este script maneja el preprocesamiento y prepara para la ejecución paralela

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/bash_utils.sh"

# Verificar parámetros
if [ "$#" -lt 1 ]; then
    error_message "Uso: $0 <lambda_name> [region] [batch_size]"
    error_message "  lambda_name: Nombre de la función Lambda del preprocesador (obligatorio)"
    error_message "  region: Región AWS (opcional, por defecto: eu-west-1)"
    error_message "  batch_size: Número de combinaciones a procesar en paralelo (opcional, por defecto: 5)"
    exit 1
fi

LAMBDA_NAME="$1"
REGION="${2:-eu-west-1}"
BATCH_SIZE="${3:-5}"

section_header "PREPROCESAMIENTO DE DATOS GTFS"
info_message "Función Lambda: ${LAMBDA_NAME}"
info_message "Región: ${REGION}"
info_message "Tamaño de lote: ${BATCH_SIZE}"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Verificar la existencia de combinaciones pendientes
PENDING_COUNT=$(jq '.pending' ${STATUS_FILE})
if [ "$PENDING_COUNT" -eq 0 ]; then
    warning_message "No hay combinaciones pendientes para procesar"
    show_status_summary
    exit 0
fi

info_message "Hay ${PENDING_COUNT} combinaciones pendientes para procesar"

# Procesar una combinación específica
process_combination() {
    local combo="$1"
    local p_empresa=$(echo $combo | jq -r '.P_EMPRESA')
    local p_contr=$(echo $combo | jq -r '.P_CONTR')
    local p_version=$(echo $combo | jq -r '.P_VERSION')
    
    info_message "Procesando combinación: E=${p_empresa}, C=${p_contr}, V=${p_version}"
    
    # Actualizar estado a preprocessing
    update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "preprocessing"
    
    # Preparar payload para la función Lambda
    local payload=$(format_preprocessor_payload "${p_empresa}" "${p_contr}" "${p_version}")
    local log_file="${LOGS_DIR}/preprocess_${p_empresa}_${p_contr}_${p_version}.log"
    
    info_message "Invocando función Lambda ${LAMBDA_NAME}..."
    
    # Invocar la función Lambda
    aws lambda invoke \
        --function-name ${LAMBDA_NAME} \
        --region ${REGION} \
        --payload "${payload}" \
        --cli-binary-format raw-in-base64-out \
        "${log_file}" \
        > /dev/null 2>&1
    
    local lambda_result=$?
    
    if [ $lambda_result -ne 0 ]; then
        error_message "Error al invocar la función Lambda"
        update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "failed" "Error al invocar Lambda: código ${lambda_result}"
        return 1
    fi
    
    # Analizar la respuesta
    local status=$(jq -r '.status' "${log_file}")
    local execution_id=$(jq -r '.execution_id' "${log_file}")
    local temp_dir=$(jq -r '.temp_dir' "${log_file}")
    
    if [ "${status}" == "success" ]; then
        success_message "Preprocesamiento exitoso para E=${p_empresa}, C=${p_contr}, V=${p_version}"
        success_message "execution_id=${execution_id}"
        success_message "temp_dir=${temp_dir}"
        
        # Actualizar estado con los datos del preprocesador
        local update_data=".combinations |= map(
            if .P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\" then
                .status = \"preprocessed\" |
                .execution_id = \"${execution_id}\" |
                .temp_dir = \"${temp_dir}\" |
                .preprocessed_at = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
            else
                .
            end
        )"
        
        jq "${update_data}" ${STATUS_FILE} > ${STATUS_FILE}.tmp
        mv ${STATUS_FILE}.tmp ${STATUS_FILE}
        update_status
        
        return 0
    else
        error_message "Preprocesamiento fallido para E=${p_empresa}, C=${p_contr}, V=${p_version}"
        
        # Obtener mensaje de error si está disponible
        local error_message=$(jq -r '.message // "Error desconocido"' "${log_file}")
        update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "failed" "${error_message}"
        
        return 1
    fi
}

# Procesar combinaciones pendientes en lotes
PENDING_COMBINATIONS=$(get_pending_combinations)
TOTAL_PROCESSED=0
TOTAL_SUCCESS=0
TOTAL_FAILED=0

for combo in $PENDING_COMBINATIONS; do
    # Procesar esta combinación
    process_combination "$combo"
    
    if [ $? -eq 0 ]; then
        TOTAL_SUCCESS=$((TOTAL_SUCCESS+1))
    else
        TOTAL_FAILED=$((TOTAL_FAILED+1))
    fi
    
    TOTAL_PROCESSED=$((TOTAL_PROCESSED+1))
    
    # Mostrar progreso
    progress_bar $TOTAL_PROCESSED $PENDING_COUNT "Preprocesamiento"
    
    # Salir si hemos alcanzado el tamaño del lote
    if [ $TOTAL_PROCESSED -ge $BATCH_SIZE ]; then
        info_message "Se ha alcanzado el tamaño máximo de lote (${BATCH_SIZE})"
        break
    fi
done

section_header "RESUMEN DE PREPROCESAMIENTO"
success_message "Total procesados: ${TOTAL_PROCESSED}"
success_message "Exitosos: ${TOTAL_SUCCESS}"
if [ $TOTAL_FAILED -gt 0 ]; then
    error_message "Fallidos: ${TOTAL_FAILED}"
fi

show_status_summary

exit 0