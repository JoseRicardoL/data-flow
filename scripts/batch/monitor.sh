#!/bin/bash
# Script para monitorear el estado de los jobs en ejecución
# y actualizar el estado de las combinaciones

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/batch_utils.sh"

# Verificar parámetros
if [ "$#" -lt 1 ]; then
    error_message "Uso: $0 <region> [max_check]"
    error_message "  region: Región AWS (obligatorio)"
    error_message "  max_check: Número máximo de combinaciones a verificar por ejecución (opcional, por defecto: 20)"
    exit 1
fi

REGION="$1"
MAX_CHECK="${2:-20}"

section_header "MONITOREO DE JOBS GLUE"
info_message "Región: ${REGION}"
info_message "Máximo a verificar: ${MAX_CHECK}"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Verificar la existencia de combinaciones en procesamiento
PROCESSING_COUNT=$(jq '.combinations | map(select(.status == "processing")) | length' ${STATUS_FILE})
if [ "$PROCESSING_COUNT" -eq 0 ]; then
    info_message "No hay jobs en ejecución para monitorear"
    show_status_summary
    exit 0
fi

info_message "Hay ${PROCESSING_COUNT} combinaciones en procesamiento"

# Función para verificar el estado de los jobs de una combinación
check_combination_jobs() {
    local combo="$1"
    local p_empresa=$(echo $combo | jq -r '.P_EMPRESA')
    local p_contr=$(echo $combo | jq -r '.P_CONTR')
    local p_version=$(echo $combo | jq -r '.P_VERSION')
    local macro_job_id=$(echo $combo | jq -r '.macro_job_id')
    local macro_stops_job_id=$(echo $combo | jq -r '.macro_stops_job_id')
    local macro_job_name=$(echo $combo | jq -r '.macro_job_name // "MacroGenerator"')
    local macro_stops_job_name=$(echo $combo | jq -r '.macro_stops_job_name // "MacroStopsGenerator"')
    
    info_message "Verificando jobs para combinación: E=${p_empresa}, C=${p_contr}, V=${p_version}"
    
    # Verificar estado de job de macro
    local macro_status="UNKNOWN"
    if [ -n "${macro_job_id}" ]; then
        macro_status=$(aws glue get-job-run \
            --job-name "${macro_job_name}" \
            --run-id "${macro_job_id}" \
            --region "${REGION}" \
            --query "JobRun.JobRunState" \
            --output text 2>/dev/null || echo "ERROR")
        
        info_message "Estado de job macro: ${macro_status}"
    fi
    
    # Verificar estado de job de macro_stops
    local macro_stops_status="UNKNOWN"
    if [ -n "${macro_stops_job_id}" ]; then
        macro_stops_status=$(aws glue get-job-run \
            --job-name "${macro_stops_job_name}" \
            --run-id "${macro_stops_job_id}" \
            --region "${REGION}" \
            --query "JobRun.JobRunState" \
            --output text 2>/dev/null || echo "ERROR")
        
        info_message "Estado de job macro_stops: ${macro_stops_status}"
    fi
    
    # Determinar si ambos jobs han terminado
    local macro_completed=0
    local macro_stops_completed=0
    
    if [[ "${macro_status}" == "SUCCEEDED" || "${macro_status}" == "FAILED" || 
          "${macro_status}" == "TIMEOUT" || "${macro_status}" == "STOPPED" || 
          "${macro_status}" == "ERROR" ]]; then
        macro_completed=1
    fi
    
    if [[ "${macro_stops_status}" == "SUCCEEDED" || "${macro_stops_status}" == "FAILED" || 
          "${macro_stops_status}" == "TIMEOUT" || "${macro_stops_status}" == "STOPPED" || 
          "${macro_stops_status}" == "ERROR" ]]; then
        macro_stops_completed=1
    fi
    
    # Actualizar estado de la combinación
    if [ $macro_completed -eq 1 ] && [ $macro_stops_completed -eq 1 ]; then
        # Ambos jobs han terminado
        local new_status="completed"
        local status_message=""
        
        # Verificar si alguno falló
        if [[ "${macro_status}" != "SUCCEEDED" || "${macro_stops_status}" != "SUCCEEDED" ]]; then
            new_status="failed"
            status_message="Macro: ${macro_status}, MacroStops: ${macro_stops_status}"
        fi
        
        # Actualizar estado
        update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "${new_status}" "${status_message}"
        
        # Actualizar información detallada
        local update_data=".combinations |= map(
            if .P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\" then
                .macro_status = \"${macro_status}\" |
                .macro_stops_status = \"${macro_stops_status}\" |
                .jobs_completed_at = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
            else
                .
            end
        )"
        
        jq "${update_data}" ${STATUS_FILE} > ${STATUS_FILE}.tmp
        mv ${STATUS_FILE}.tmp ${STATUS_FILE}
        
        if [ "${new_status}" == "completed" ]; then
            success_message "Jobs completados exitosamente para E=${p_empresa}, C=${p_contr}, V=${p_version}"
        else
            error_message "Jobs fallidos para E=${p_empresa}, C=${p_contr}, V=${p_version}: ${status_message}"
        fi
        
        return 0
    else
        # Al menos un job sigue en ejecución
        info_message "Jobs todavía en ejecución para E=${p_empresa}, C=${p_contr}, V=${p_version}"
        
        # Actualizar información de estado actual
        local update_data=".combinations |= map(
            if .P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\" then
                .macro_status = \"${macro_status}\" |
                .macro_stops_status = \"${macro_stops_status}\" |
                .last_checked = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
            else
                .
            end
        )"
        
        jq "${update_data}" ${STATUS_FILE} > ${STATUS_FILE}.tmp
        mv ${STATUS_FILE}.tmp ${STATUS_FILE}
        
        return 1
    fi
}

# Verificar jobs en ejecución
PROCESSING_COMBINATIONS=$(jq -c '.combinations | map(select(.status == "processing")) | .[]' ${STATUS_FILE})
TOTAL_CHECKED=0
TOTAL_COMPLETED=0
TOTAL_STILL_RUNNING=0

for combo in $PROCESSING_COMBINATIONS; do
    # Verificar esta combinación
    check_combination_jobs "$combo"
    
    if [ $? -eq 0 ]; then
        TOTAL_COMPLETED=$((TOTAL_COMPLETED+1))
    else
        TOTAL_STILL_RUNNING=$((TOTAL_STILL_RUNNING+1))
    fi
    
    TOTAL_CHECKED=$((TOTAL_CHECKED+1))
    
    # Actualizar contadores
    update_status
    
    # Salir si hemos alcanzado el máximo a verificar
    if [ $TOTAL_CHECKED -ge $MAX_CHECK ]; then
        info_message "Se ha alcanzado el máximo de verificaciones (${MAX_CHECK})"
        break
    fi
done

# Verificar time-outs (jobs en ejecución por más de 4 horas)
info_message "Verificando jobs con posible time-out..."

# Obtener combinaciones que llevan más de 4 horas en ejecución
TIMEOUT_CANDIDATES=$(jq -c '.combinations | map(select(
    .status == "processing" and 
    (.jobs_started_at | fromdateiso8601) < (now - 14400)
)) | .[]' ${STATUS_FILE})

TIMEOUT_COUNT=0

for combo in $TIMEOUT_CANDIDATES; do
    local p_empresa=$(echo $combo | jq -r '.P_EMPRESA')
    local p_contr=$(echo $combo | jq -r '.P_CONTR')
    local p_version=$(echo $combo | jq -r '.P_VERSION')
    
    warning_message "Posible timeout para combinación: E=${p_empresa}, C=${p_contr}, V=${p_version}"
    
    # Marcar como fallido por timeout
    update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "failed" "Timeout después de 4 horas de ejecución"
    
    TIMEOUT_COUNT=$((TIMEOUT_COUNT+1))
done

update_status

section_header "RESUMEN DE MONITOREO"
info_message "Total de combinaciones verificadas: ${TOTAL_CHECKED}"
success_message "Completadas en esta verificación: ${TOTAL_COMPLETED}"
warning_message "Todavía en ejecución: ${TOTAL_STILL_RUNNING}"

if [ $TIMEOUT_COUNT -gt 0 ]; then
    error_message "Marcadas como timeout: ${TIMEOUT_COUNT}"
fi

show_status_summary

exit 0