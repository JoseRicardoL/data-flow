#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

STACK_NAME=$1
REGION=$2
ENV=$3

# Validar argumentos requeridos
validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "REGION" "$REGION" || exit 1
validate_required_var "ENV" "$ENV" || exit 1

# Mostrar información del script
show_script_info "PRUEBA DE JOB GLUE" "Ejecutar y monitorear trabajo Glue" \
    "Stack" "$STACK_NAME" \
    "Región" "$REGION" \
    "Ambiente" "$ENV"

section_header "CONFIGURACIÓN DEL JOB"
info_message "Obteniendo configuración del job..."
JOB_NAME=$(aws cloudformation describe-stacks \
    --stack-name ${STACK_NAME} \
    --region ${REGION} \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueJobName`].OutputValue' \
    --output text)

show_result $? "Job encontrado: ${JOB_NAME}" "Error al obtener el nombre del job"

section_header "EJECUCIÓN DEL JOB"
# Verificar si existe el archivo de argumentos
if [ -f "test/input/args.json" ]; then
    info_message "Encontrado archivo de argumentos, se ejecutará con estos argumentos..."
    JOB_RUN_ID=$(aws glue start-job-run \
        --job-name ${JOB_NAME} \
        --region ${REGION} \
        --arguments file://test/input/args.json \
        --output text \
        --query 'JobRunId')
    success_message "Run ID: ${JOB_RUN_ID}"
    echo "$JOB_RUN_ID" >test/logs/last_job_run_id.txt
    info_message "ID guardado en test/logs/last_job_run_id.txt"
else
    # Iniciar el job de Glue (original)
    info_message "Iniciando ejecución del job sin argumentos personalizados..."
    JOB_RUN_ID=$(aws glue start-job-run \
        --job-name ${JOB_NAME} \
        --region ${REGION} \
        --output text \
        --query 'JobRunId')

    success_message "Run ID: ${JOB_RUN_ID}"
fi

section_header "MONITOREO DE EJECUCIÓN"
info_message "Monitoreando ejecución del job..."

# Monitorear el estado del job
previous_status=""
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name ${JOB_NAME} \
        --run-id ${JOB_RUN_ID} \
        --region ${REGION} \
        --query 'JobRun.JobRunState' \
        --output text)

    # Solo mostrar el estado si ha cambiado
    if [ "$STATUS" != "$previous_status" ]; then
        TIMESTAMP=$(date "+%H:%M:%S")
        case $STATUS in
        "RUNNING")
            warning_message "[$TIMESTAMP] Job en ejecución..."
            ;;
        "SUCCEEDED")
            success_message "[$TIMESTAMP] Job completado exitosamente"
            ;;
        "FAILED")
            error_message "[$TIMESTAMP] Job falló"
            ;;
        "TIMEOUT")
            error_message "[$TIMESTAMP] Job timeout"
            ;;
        "STOPPED")
            error_message "[$TIMESTAMP] Job detenido"
            ;;
        *)
            info_message "[$TIMESTAMP] Estado: $STATUS"
            ;;
        esac
        previous_status=$STATUS
    fi

    if [ "$STATUS" = "SUCCEEDED" ]; then
        success_message "Job completado exitosamente"
        # Guardar detalles de la ejecución exitosa
        aws glue get-job-run \
            --job-name ${JOB_NAME} \
            --run-id ${JOB_RUN_ID} \
            --region ${REGION} \
            --output json >test/logs/last_successful_run.json
        info_message "Detalles guardados en test/logs/last_successful_run.json"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "TIMEOUT" ] || [ "$STATUS" = "STOPPED" ]; then
        error_message "Job falló con estado: ${STATUS}"

        # Obtener el error message
        ERROR=$(aws glue get-job-run \
            --job-name ${JOB_NAME} \
            --run-id ${JOB_RUN_ID} \
            --region ${REGION} \
            --query 'JobRun.ErrorMessage' \
            --output text)

        error_message "Error: ${ERROR}"
        echo "$ERROR" >test/logs/last_error.txt
        aws glue get-job-run \
            --job-name ${JOB_NAME} \
            --run-id ${JOB_RUN_ID} \
            --region ${REGION} \
            --output json >test/logs/last_failed_run.json
        info_message "Detalles guardados en test/logs/last_failed_run.json"
        finalize_script 1 "" "EJECUCIÓN FALLIDA"
        exit 1
    fi

    sleep 5
done

section_header "RESULTADOS DE EJECUCIÓN"
# Mostrar tiempo de ejecución y métricas finales si el job fue exitoso
if [ "$STATUS" = "SUCCEEDED" ]; then
    METRICS=$(aws glue get-job-run \
        --job-name ${JOB_NAME} \
        --run-id ${JOB_RUN_ID} \
        --region ${REGION} \
        --query 'JobRun.{ExecutionTime:ExecutionTime,MaxCapacity:MaxCapacity,WorkerType:WorkerType,NumberOfWorkers:NumberOfWorkers}' \
        --output json)

    highlight_message "Métricas de ejecución:"
    echo "$METRICS" | jq -r '. | to_entries | .[] | "\(.key): \(.value)"' | while read line; do
        success_message "$line"
    done
fi

# Finalizar el script
finalize_script 0 "PRUEBA COMPLETADA EXITOSAMENTE" ""
exit 0
