#!/bin/bash
# Script para ejecutar los jobs de macro y macro_stops en paralelo
# para combinaciones preprocesadas

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/batch_utils.sh"

# Verificar parámetros
if [ "$#" -lt 3 ]; then
    error_message "Uso: $0 <macro_job> <macro_stops_job> <bucket> [region] [batch_size]"
    error_message "  macro_job: Nombre del job Glue para macro (obligatorio)"
    error_message "  macro_stops_job: Nombre del job Glue para macro_stops (obligatorio)"
    error_message "  bucket: Nombre del bucket S3 (obligatorio)"
    error_message "  region: Región AWS (opcional, por defecto: eu-west-1)"
    error_message "  batch_size: Número de combinaciones a procesar en paralelo (opcional, por defecto: 5)"
    exit 1
fi

MACRO_JOB="$1"
MACRO_STOPS_JOB="$2"
BUCKET="$3"
REGION="${4:-eu-west-1}"
BATCH_SIZE="${5:-5}"

section_header "EJECUCIÓN DE JOBS GLUE EN PARALELO"
info_message "Job Macro: ${MACRO_JOB}"
info_message "Job MacroStops: ${MACRO_STOPS_JOB}"
info_message "Bucket: ${BUCKET}"
info_message "Región: ${REGION}"
info_message "Tamaño de lote: ${BATCH_SIZE}"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Verificar la existencia de combinaciones preprocesadas
PREPROCESSED_COUNT=$(jq '.combinations | map(select(.status == "preprocessed")) | length' ${STATUS_FILE})
if [ "$PREPROCESSED_COUNT" -eq 0 ]; then
    warning_message "No hay combinaciones preprocesadas para ejecutar"
    show_status_summary
    exit 0
fi

info_message "Hay ${PREPROCESSED_COUNT} combinaciones preprocesadas para ejecutar"

# Función para iniciar un job de Glue
start_glue_job() {
    local job_name="$1"
    local args="$2"
    
    aws glue start-job-run \
        --job-name "${job_name}" \
        --region "${REGION}" \
        --arguments "${args}" \
        --query "JobRunId" \
        --output text
}

# Función para verificar el estado de un job de Glue
check_glue_job_status() {
    local job_name="$1"
    local job_run_id="$2"
    
    aws glue get-job-run \
        --job-name "${job_name}" \
        --run-id "${job_run_id}" \
        --region "${REGION}" \
        --query "JobRun.JobRunState" \
        --output text
}

# Función para ejecutar jobs de macro y macro_stops para una combinación
execute_jobs() {
    local combo="$1"
    local p_empresa=$(echo $combo | jq -r '.P_EMPRESA')
    local p_contr=$(echo $combo | jq -r '.P_CONTR')
    local p_version=$(echo $combo | jq -r '.P_VERSION')
    local execution_id=$(echo $combo | jq -r '.execution_id')
    local temp_dir=$(echo $combo | jq -r '.temp_dir')
    
    info_message "Ejecutando jobs para combinación: E=${p_empresa}, C=${p_contr}, V=${p_version}"
    
    # Actualizar estado a processing
    update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "processing" "Iniciando jobs"
    
    # Crear JSON para información de entrada
    local json_input="[{\"statusCode\": 200, \"body\": \"{\\\"P_EMPRESA\\\": \\\"${p_empresa}\\\", \\\"P_VERSION\\\": \\\"${p_version}\\\", \\\"P_CONTR\\\": \\\"${p_contr}\\\"}\"}]"
    
    # Preparar argumentos comunes
    local common_args="{
        \"--P_EMPRESA\":\"${p_empresa}\",
        \"--P_VERSION\":\"${p_version}\",
        \"--P_CONTR\":\"${p_contr}\",
        \"--temp_dir\":\"${temp_dir}\",
        \"--execution_id\":\"${execution_id}\",
        \"--bronze_bucket\":\"${BUCKET}\",
        \"--S3_BUCKET\":\"${BUCKET}\",
        \"--json_input\":\"${json_input}\"
    }"
    
    # Iniciar job de macro
    info_message "Iniciando job de macro..."
    local macro_job_id=$(start_glue_job "${MACRO_JOB}" "${common_args}")
    
    if [ -z "${macro_job_id}" ]; then
        error_message "Error al iniciar job de macro"
        update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "failed" "Error al iniciar job de macro"
        return 1
    fi
    success_message "Job de macro iniciado con ID: ${macro_job_id}"
    
    # Iniciar job de macro_stops
    info_message "Iniciando job de macro_stops..."
    local macro_stops_job_id=$(start_glue_job "${MACRO_STOPS_JOB}" "${common_args}")
    
    if [ -z "${macro_stops_job_id}" ]; then
        error_message "Error al iniciar job de macro_stops"
        update_combination_status "${p_empresa}" "${p_contr}" "${p_version}" "failed" "Error al iniciar job de macro_stops"
        return 1
    fi
    success_message "Job de macro_stops iniciado con ID: ${macro_stops_job_id}"
    
    # Actualizar estado con los IDs de los jobs
    local update_data=".combinations |= map(
        if .P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\" then
            .macro_job_id = \"${macro_job_id}\" |
            .macro_stops_job_id = \"${macro_stops_job_id}\" |
            .jobs_started_at = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
        else
            .
        end
    )"
    
    jq "${update_data}" ${STATUS_FILE} > ${STATUS_FILE}.tmp
    mv ${STATUS_FILE}.tmp ${STATUS_FILE}
    update_status
    
    return 0
}

# Procesar combinaciones preprocesadas en lotes
PREPROCESSED_COMBINATIONS=$(jq -c '.combinations | map(select(.status == "preprocessed")) | .[]' ${STATUS_FILE})
TOTAL_PROCESSED=0
TOTAL_SUCCESS=0
TOTAL_FAILED=0

for combo in $PREPROCESSED_COMBINATIONS; do
    # Ejecutar jobs para esta combinación
    execute_jobs "$combo"
    
    if [ $? -eq 0 ]; then
        TOTAL_SUCCESS=$((TOTAL_SUCCESS+1))
    else
        TOTAL_FAILED=$((TOTAL_FAILED+1))
    fi
    
    TOTAL_PROCESSED=$((TOTAL_PROCESSED+1))
    
    # Mostrar progreso
    progress_bar $TOTAL_PROCESSED $PREPROCESSED_COUNT "Ejecución de jobs"
    
    # Salir si hemos alcanzado el tamaño del lote
    if [ $TOTAL_PROCESSED -ge $BATCH_SIZE ]; then
        info_message "Se ha alcanzado el tamaño máximo de lote (${BATCH_SIZE})"
        break
    fi
done

section_header "RESUMEN DE EJECUCIÓN"
success_message "Total procesados: ${TOTAL_PROCESSED}"
success_message "Exitosos: ${TOTAL_SUCCESS}"
if [ $TOTAL_FAILED -gt 0 ]; then
    error_message "Fallidos: ${TOTAL_FAILED}"
fi

show_status_summary

exit 0