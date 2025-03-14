#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

STACK_NAME=$1
REGION=$2

# Validar argumentos requeridos
validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "REGION" "$REGION" || exit 1

# Mostrar información del script
show_script_info "OBTENCIÓN DE LOGS" "Recuperar logs de CloudWatch del último job ejecutado" \
    "Stack" "$STACK_NAME" \
    "Región" "$REGION"

# Verificar si existe el archivo con el último job ID
if [ ! -f "test/logs/last_job_run_id.txt" ]; then
    error_message "No existe registro del último job ejecutado"
    error_message "Ejecute primero 'make test' para generar un job ID"
    finalize_script 1 "" "OBTENCIÓN DE LOGS FALLIDA"
    exit 1
fi

section_header "RECUPERANDO ID DEL JOB"
info_message "Leyendo ID del último job ejecutado..."
JOB_RUN_ID=$(cat test/logs/last_job_run_id.txt)
success_message "Job Run ID: ${JOB_RUN_ID}"

section_header "OBTENIENDO NOMBRE DEL JOB"
info_message "Consultando información del stack..."
JOB_NAME=$(aws cloudformation describe-stacks \
    --stack-name ${STACK_NAME} \
    --region ${REGION} \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueJobName`].OutputValue' \
    --output text)

show_result $? "Job encontrado: ${JOB_NAME}" "Error al obtener el nombre del job"

section_header "RECUPERANDO CONFIGURACIÓN DEL JOB"
info_message "Obteniendo detalles de configuración del job..."

# Obtener detalles del job para verificar la configuración de logs
JOB_DETAILS=$(aws glue get-job \
    --job-name ${JOB_NAME} \
    --region ${REGION} \
    --query 'Job.DefaultArguments' \
    --output json 2>/dev/null || echo "{}")

# Extraer el grupo de logs de la configuración
LOG_GROUP_ARG=$(echo "$JOB_DETAILS" | jq -r '."--continuous-log-logGroup" // empty')
LOG_FILTER_ARG=$(echo "$JOB_DETAILS" | jq -r '."--continuous-log-logStreamPrefix" // empty')

# Si no hay configuración explícita, usar los valores por defecto
if [ -z "$LOG_GROUP_ARG" ]; then
    LOG_GROUP_ARG="/aws-glue/jobs/output"
    warning_message "No se encontró configuración de grupo de logs, usando valor por defecto: $LOG_GROUP_ARG"
else
    success_message "Grupo de logs configurado: $LOG_GROUP_ARG"
fi

section_header "RECUPERANDO LOGS"
info_message "Obteniendo logs de CloudWatch..."

# Crear directorio principal para logs si no existe
mkdir -p test/logs/cloudwatch

# Crear carpeta específica para este job usando el JOB_RUN_ID
JOB_LOG_DIR="test/logs/cloudwatch/${JOB_RUN_ID}"
mkdir -p "$JOB_LOG_DIR"

# Definir archivos consolidados para logs: all_logs, error y output
ALL_LOG_FILE="${JOB_LOG_DIR}/all_logs.log"
ERROR_LOG_FILE="${JOB_LOG_DIR}/error.log"
OUTPUT_LOG_FILE="${JOB_LOG_DIR}/output.log"

# Crear encabezados en cada archivo
echo "========== LOGS CONSOLIDADOS DEL JOB ${JOB_NAME} (${JOB_RUN_ID}) ==========" >"$ALL_LOG_FILE"
echo "Fecha de ejecución: $(date)" >>"$ALL_LOG_FILE"
echo "=========================================================================" >>"$ALL_LOG_FILE"
echo "" >>"$ALL_LOG_FILE"

echo "========== LOGS DE ERROR DEL JOB ${JOB_NAME} (${JOB_RUN_ID}) ==========" >"$ERROR_LOG_FILE"
echo "Fecha de ejecución: $(date)" >>"$ERROR_LOG_FILE"
echo "=========================================================================" >>"$ERROR_LOG_FILE"
echo "" >>"$ERROR_LOG_FILE"

echo "========== LOGS DE OUTPUT DEL JOB ${JOB_NAME} (${JOB_RUN_ID}) ==========" >"$OUTPUT_LOG_FILE"
echo "Fecha de ejecución: $(date)" >>"$OUTPUT_LOG_FILE"
echo "=========================================================================" >>"$OUTPUT_LOG_FILE"
echo "" >>"$OUTPUT_LOG_FILE"

# Lista de posibles grupos de logs para buscar
LOG_GROUPS=(
    "$LOG_GROUP_ARG"             # Grupo configurado o por defecto
    "/aws-glue/jobs/output"      # Grupo estándar de salida
    "/aws-glue/jobs/logs-${ENV}" # Grupo basado en ambiente
    "/aws-glue/jobs/logs-dev"    # Grupo específico para dev
    "/aws-glue/jobs/error"       # Grupo de errores
    "/aws-glue/jobs/${JOB_NAME}" # Grupo basado en nombre del job
    "/aws-glue/jobs"             # Grupo general de Glue
)

# Identificadores de flujo de log a buscar
STREAM_IDENTIFIERS=(
    "${JOB_NAME}/${JOB_RUN_ID}" # Formato estándar nombre/id
    "${JOB_RUN_ID}"             # Solo ID
    "${JOB_NAME}-${JOB_RUN_ID}" # Formato con guión
    "${JOB_NAME}_${JOB_RUN_ID}" # Formato con guión bajo
    "${JOB_NAME}"               # Solo nombre del job
)

# Variable para seguir si se encontraron logs
FOUND_LOGS=false

# Función para clasificar y mover logs "extra"
move_extra_log() {
    local stream_file="$1"
    local log_group="$2"
    local stream_name="$3"

    # Definir tipo según palabra clave en el nombre del stream o en el log group
    local EXTRA_TYPE="general"
    if [[ "$stream_name" == *"driver"* ]]; then
        EXTRA_TYPE="driver"
    elif [[ "$stream_name" == *"progress-bar"* ]]; then
        EXTRA_TYPE="progress-bar"
    elif [[ "$log_group" == *"/aws-glue/jobs/logs-dev"* ]]; then
        EXTRA_TYPE="dev"
    fi

    local EXTRA_DIR="${JOB_LOG_DIR}/extra/${EXTRA_TYPE}"
    mkdir -p "$EXTRA_DIR"

    # Generar un nombre más amigable
    # Ejemplo: <JOB_RUN_ID>_driver_stream_abcdef.log
    local NEW_NAME="${JOB_RUN_ID}_${EXTRA_TYPE}_${stream_name}.log"
    local EXTRA_FILE="${EXTRA_DIR}/${NEW_NAME}"

    mv "$stream_file" "$EXTRA_FILE"
    info_message "Extra log movido a: $EXTRA_FILE"
}

# Buscar en todos los grupos de logs posibles
for LOG_GROUP in "${LOG_GROUPS[@]}"; do
    info_message "Buscando en grupo de logs: $LOG_GROUP"

    # Verificar si el grupo de logs existe
    GROUP_EXISTS=$(aws logs describe-log-groups \
        --log-group-name-prefix "$LOG_GROUP" \
        --region ${REGION} \
        --query "logGroups[?logGroupName=='$LOG_GROUP'].logGroupName" \
        --output text)

    if [ -z "$GROUP_EXISTS" ]; then
        warning_message "El grupo de logs $LOG_GROUP no existe"
        continue
    fi

    # Buscar en todos los posibles formatos de flujo
    for STREAM_PREFIX in "${STREAM_IDENTIFIERS[@]}"; do
        info_message "Buscando streams con prefijo: $STREAM_PREFIX"

        # Obtener todos los streams que coincidan con el prefijo
        LOG_STREAMS=$(aws logs describe-log-streams \
            --log-group-name "$LOG_GROUP" \
            --log-stream-name-prefix "$STREAM_PREFIX" \
            --region ${REGION} \
            --query 'logStreams[*].logStreamName' \
            --output text)

        if [ -z "$LOG_STREAMS" ]; then
            warning_message "No se encontraron streams con prefijo $STREAM_PREFIX en $LOG_GROUP"
            continue
        fi

        success_message "Encontrados $(echo "$LOG_STREAMS" | wc -w) streams en $LOG_GROUP"
        FOUND_LOGS=true

        # Contador para los logs procesados
        STREAM_COUNT=0
        TOTAL_STREAMS=$(echo "$LOG_STREAMS" | wc -w)

        # Para cada stream, obtener los logs y guardarlos
        for STREAM in $LOG_STREAMS; do
            STREAM_COUNT=$((STREAM_COUNT + 1))
            STREAM_SHORT=$(echo "$STREAM" | sed 's/.*\///')
            # Archivo temporal para este stream
            STREAM_FILE="${JOB_LOG_DIR}/stream_${STREAM_SHORT}.log"

            info_message "Recuperando logs del stream $STREAM_COUNT de $TOTAL_STREAMS: $STREAM"

            # Obtener los logs del stream
            aws logs get-log-events \
                --log-group-name "$LOG_GROUP" \
                --log-stream-name "$STREAM" \
                --region ${REGION} \
                --output json >"${JOB_LOG_DIR}/temp_log.json"

            # Verificar si hay eventos de log
            LOG_EVENTS_COUNT=$(jq '.events | length' "${JOB_LOG_DIR}/temp_log.json")

            if [ "$LOG_EVENTS_COUNT" -eq 0 ]; then
                warning_message "No hay eventos de log en este stream"
                continue
            fi

            # Extraer los mensajes y timestamps
            jq -r '.events[] | (.timestamp|todate) + " [" + (.timestamp|tostring) + "] " + .message' \
                "${JOB_LOG_DIR}/temp_log.json" >"$STREAM_FILE"

            # Añadir al archivo de todos los logs
            echo "========== GRUPO: $LOG_GROUP - STREAM: $STREAM ==========" >>"$ALL_LOG_FILE"
            cat "$STREAM_FILE" >>"$ALL_LOG_FILE"
            echo "" >>"$ALL_LOG_FILE"
            echo "" >>"$ALL_LOG_FILE"

            # Según el grupo, también guardar en error u output
            if [[ "$LOG_GROUP" == *"/aws-glue/jobs/error"* ]]; then
                echo "========== GRUPO: $LOG_GROUP - STREAM: $STREAM ==========" >>"$ERROR_LOG_FILE"
                cat "$STREAM_FILE" >>"$ERROR_LOG_FILE"
                echo "" >>"$ERROR_LOG_FILE"
                echo "" >>"$ERROR_LOG_FILE"
            elif [[ "$LOG_GROUP" == *"/aws-glue/jobs/output"* ]]; then
                echo "========== GRUPO: $LOG_GROUP - STREAM: $STREAM ==========" >>"$OUTPUT_LOG_FILE"
                cat "$STREAM_FILE" >>"$OUTPUT_LOG_FILE"
                echo "" >>"$OUTPUT_LOG_FILE"
                echo "" >>"$OUTPUT_LOG_FILE"
            fi

            success_message "Log stream guardado en $STREAM_FILE ($LOG_EVENTS_COUNT eventos)"

            # Mover a subcarpeta "extra" con un nombre más amigable
            move_extra_log "$STREAM_FILE" "$LOG_GROUP" "$STREAM_SHORT"
        done
    done
done

# Limpiar archivos temporales
rm -f "${JOB_LOG_DIR}/temp_log.json"

# Verificar si se encontraron logs
if [ "$FOUND_LOGS" = false ]; then
    # No se encontraron logs, intentar obtener detalles de ejecución del API de Glue
    section_header "OBTENCIÓN DE DETALLES DE EJECUCIÓN"
    info_message "No se encontraron logs en CloudWatch, obteniendo detalles de la ejecución desde Glue API..."

    aws glue get-job-run \
        --job-name ${JOB_NAME} \
        --run-id ${JOB_RUN_ID} \
        --region ${REGION} \
        --output json >"${JOB_LOG_DIR}/job_${JOB_RUN_ID}_details.json"

    # Extraer información relevante a un archivo de texto
    echo "========== DETALLES DE EJECUCIÓN DEL JOB ${JOB_NAME} (${JOB_RUN_ID}) ==========" >"$ALL_LOG_FILE"
    echo "Fecha: $(date)" >>"$ALL_LOG_FILE"
    echo "=========================================================================" >>"$ALL_LOG_FILE"
    echo "" >>"$ALL_LOG_FILE"

    # Extraer y formatear los detalles más importantes
    JOB_DETAILS=$(cat "${JOB_LOG_DIR}/job_${JOB_RUN_ID}_details.json")

    echo "Estado: $(echo "$JOB_DETAILS" | jq -r '.JobRun.JobRunState')" >>"$ALL_LOG_FILE"
    echo "Hora de inicio: $(echo "$JOB_DETAILS" | jq -r '.JobRun.StartedOn')" >>"$ALL_LOG_FILE"
    echo "Hora de finalización: $(echo "$JOB_DETAILS" | jq -r '.JobRun.CompletedOn')" >>"$ALL_LOG_FILE"
    echo "Tiempo de ejecución: $(echo "$JOB_DETAILS" | jq -r '.JobRun.ExecutionTime') segundos" >>"$ALL_LOG_FILE"
    echo "" >>"$ALL_LOG_FILE"

    # Añadir mensaje de error si existe
    ERROR_MESSAGE=$(echo "$JOB_DETAILS" | jq -r '.JobRun.ErrorMessage // "No hay mensaje de error"')
    if [ "$ERROR_MESSAGE" != "No hay mensaje de error" ]; then
        echo "ERROR: $ERROR_MESSAGE" >>"$ALL_LOG_FILE"
        echo "" >>"$ALL_LOG_FILE"
    fi

    # Añadir argumentos utilizados
    echo "Argumentos:" >>"$ALL_LOG_FILE"
    echo "$JOB_DETAILS" | jq -r '.JobRun.Arguments | to_entries[] | "  \(.key): \(.value)"' >>"$ALL_LOG_FILE"
    echo "" >>"$ALL_LOG_FILE"

    success_message "Detalles de ejecución guardados en ${JOB_LOG_DIR}/job_${JOB_RUN_ID}_details.json"
    warning_message "No se encontraron logs en CloudWatch, pero se han obtenido los detalles de ejecución"

    # Mostrar información sobre la configuración de logs
    echo "" >>"$ALL_LOG_FILE"
    echo "========== CONFIGURACIÓN DE LOGS ==========" >>"$ALL_LOG_FILE"
    echo "No se encontraron logs para este trabajo en CloudWatch." >>"$ALL_LOG_FILE"
    echo "Verificar la configuración de CloudWatch Logs en el trabajo de Glue:" >>"$ALL_LOG_FILE"
    echo "1. Asegúrese de que '--enable-continuous-cloudwatch-log': 'true' esté configurado en los argumentos del trabajo" >>"$ALL_LOG_FILE"
    echo "2. Verifique que '--continuous-log-logGroup' esté configurado correctamente" >>"$ALL_LOG_FILE"
    echo "" >>"$ALL_LOG_FILE"

    # Añadir información sobre dónde se buscaron los logs
    echo "Se buscaron logs en los siguientes grupos:" >>"$ALL_LOG_FILE"
    for LOG_GROUP in "${LOG_GROUPS[@]}"; do
        echo "- $LOG_GROUP" >>"$ALL_LOG_FILE"
    done
    echo "" >>"$ALL_LOG_FILE"

    echo "Con los siguientes identificadores de flujo:" >>"$ALL_LOG_FILE"
    for STREAM_PREFIX in "${STREAM_IDENTIFIERS[@]}"; do
        echo "- $STREAM_PREFIX" >>"$ALL_LOG_FILE"
    done

    echo -e "${YELLOW}${ARROW} Se han obtenido los detalles de ejecución, pero no se encontraron logs en CloudWatch${NC}"
    echo -e "${YELLOW}${ARROW} Revise ${ALL_LOG_FILE} para ver los detalles y recomendaciones${NC}"
    finalize_script 0 "DETALLES DE EJECUCIÓN OBTENIDOS (SIN LOGS)" ""
else
    success_message "Logs consolidados guardados en ${ALL_LOG_FILE}"
    finalize_script 0 "LOGS RECUPERADOS EXITOSAMENTE" ""
    success_message "Logs disponibles en el directorio ${JOB_LOG_DIR}/"
fi

exit 0
