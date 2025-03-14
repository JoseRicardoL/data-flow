#!/bin/bash

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

# Validar argumentos requeridos
if [ "$#" -ne 4 ]; then
    echo "Uso: $0 STACK_NAME REGION ENV S3_BUCKET"
    exit 1
fi

STACK_NAME=$1
REGION=$2
ENV=$3
S3_BUCKET=$4

validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "REGION" "$REGION" || exit 1
validate_required_var "ENV" "$ENV" || exit 1
validate_required_var "S3_BUCKET" "$S3_BUCKET" || exit 1

show_script_info "DESCARGA DE ARCHIVOS" "Descarga automática de archivos generados desde S3" \
    "Stack" "$STACK_NAME" \
    "Región" "$REGION" \
    "Ambiente" "$ENV" \
    "Bucket S3" "$S3_BUCKET"

# Verificar dependencias
for cmd in jq aws grep; do
    if ! command -v $cmd &>/dev/null; then
        error_message "El comando $cmd no está instalado. Instálalo para continuar."
        exit 1
    fi
done

# Obtener el Job Run ID
JOB_RUN_ID_FILE="test/logs/last_job_run_id.txt"
if [ ! -f "$JOB_RUN_ID_FILE" ]; then
    error_message "No se encontró el archivo ${JOB_RUN_ID_FILE}."
    exit 1
fi

JOB_RUN_ID=$(cat "$JOB_RUN_ID_FILE")
if [ -z "$JOB_RUN_ID" ]; then
    error_message "El archivo ${JOB_RUN_ID_FILE} está vacío."
    exit 1
fi

section_header "OBTENIENDO DETALLES DEL JOB RUN"
info_message "Obteniendo detalles del job run ID: $JOB_RUN_ID ..."

# Obtener detalles del job
if ! aws glue get-job-run --job-name "$STACK_NAME" --run-id "$JOB_RUN_ID" --region "$REGION" >/tmp/job_details.json; then
    error_message "Error al obtener los detalles del job run."
    [ -f "/tmp/job_details.json" ] && rm "/tmp/job_details.json"
    exit 1
fi

# Extraer parámetros del job
JOB_DETAILS=$(cat /tmp/job_details.json)
JSON_INPUT=$(echo "$JOB_DETAILS" | jq -r '.JobRun.Arguments["--json_input"]')
if [ "$JSON_INPUT" == "null" ] || [ -z "$JSON_INPUT" ]; then
    error_message "No se encontró el argumento --json_input en los detalles del job run."
    rm -f "/tmp/job_details.json"
    exit 1
fi

BODY=$(echo "$JSON_INPUT" | jq -r '.[0].body')
if [ "$BODY" == "null" ] || [ -z "$BODY" ]; then
    error_message "No se pudo extraer el body del primer elemento del argumento --json_input."
    rm -f "/tmp/job_details.json"
    exit 1
fi

P_EMPRESA=$(echo "$BODY" | jq -r '.P_EMPRESA')
P_CONTR=$(echo "$BODY" | jq -r '.P_CONTR')
P_VERSION=$(echo "$BODY" | jq -r '.P_VERSION')

if [ -z "$P_EMPRESA" ] || [ -z "$P_CONTR" ] || [ -z "$P_VERSION" ]; then
    error_message "No se pudieron extraer P_EMPRESA, P_CONTR o P_VERSION del body."
    rm -f "/tmp/job_details.json"
    exit 1
fi

success_message "Parámetros extraídos: P_EMPRESA=$P_EMPRESA, P_CONTR=$P_CONTR, P_VERSION=$P_VERSION"
rm -f "/tmp/job_details.json"

# Definir directorio de salida y crear estructura
OUTPUT_DIR="test/output/S3"
mkdir -p "${OUTPUT_DIR}"

# Diagnóstico de credenciales AWS
section_header "DIAGNÓSTICO DE CREDENCIALES AWS"
info_message "Verificando configuración de AWS..."

# Verificar que AWS CLI está configurado correctamente
if ! aws sts get-caller-identity >/tmp/aws_identity.json 2>/tmp/aws_error.log; then
    error_message "Error de autenticación AWS. Detalles:"
    cat /tmp/aws_error.log
    rm -f /tmp/aws_identity.json /tmp/aws_error.log
    finalize_script 1 "" "No se pudieron verificar las credenciales de AWS. Ejecuta 'aws configure' o verifica tus variables de entorno AWS."
    exit 1
fi

# Mostrar información del usuario/rol
AWS_IDENTITY=$(cat /tmp/aws_identity.json)
AWS_ACCOUNT=$(echo "$AWS_IDENTITY" | jq -r '.Account')
AWS_USER=$(echo "$AWS_IDENTITY" | jq -r '.Arn')
success_message "AWS configurado correctamente como: ${AWS_USER} (Cuenta: ${AWS_ACCOUNT})"
rm -f /tmp/aws_identity.json

# Prueba de acceso a S3
section_header "PRUEBA DE ACCESO A S3"
info_message "Verificando acceso a servicios S3..."

# Probar si podemos listar buckets en general
if ! aws s3 ls >/tmp/s3_list.log 2>/tmp/s3_error.log; then
    error_message "Error al acceder al servicio S3. Detalles:"
    cat /tmp/s3_error.log
    rm -f /tmp/s3_list.log /tmp/s3_error.log
    finalize_script 1 "" "No se puede acceder al servicio S3. Verifica tu conexión de red y permisos."
    exit 1
fi
success_message "Acceso a S3 confirmado."
rm -f /tmp/s3_list.log

# Verificar si el bucket específico existe
info_message "Verificando existencia del bucket: ${S3_BUCKET}"
if ! aws s3api head-bucket --bucket "${S3_BUCKET}" --region "${REGION}" 2>/tmp/bucket_error.log; then
    error_code=$(cat /tmp/bucket_error.log)
    if [[ "$error_code" == *"404"* ]]; then
        error_message "El bucket ${S3_BUCKET} no existe."
    elif [[ "$error_code" == *"403"* ]]; then
        error_message "No tienes permisos para acceder al bucket ${S3_BUCKET}."
    else
        error_message "Error al acceder al bucket ${S3_BUCKET}. Detalles:"
        cat /tmp/bucket_error.log
    fi
    rm -f /tmp/bucket_error.log
    finalize_script 1 "" "No se puede acceder al bucket S3. Verifica que el nombre sea correcto y que tengas los permisos necesarios."
    exit 1
fi
success_message "Bucket ${S3_BUCKET} encontrado y accesible."

# Definir carpetas base a explorar y white lists
declare -A folder_whitelist
folders=("SAE" "GTFS")

folder_whitelist["GTFS"]="AGENCY FEED_INFO CALENDAR_DATES TRIPS STOPS ROUTES CALENDAR PRE_STOP_TIMES STOP_TIMES SHAPES"

folder_whitelist["SAE"]=""

section_header "DESCARGA DE ARCHIVOS"

# Inicializar contadores
total_discovered=0
total_downloaded=0
total_errors=0

# Para cada carpeta base
for folder in "${folders[@]}"; do
    section_header "PROCESANDO CARPETA: ${folder}"

    # Obtener la white list para esta carpeta
    whitelist="${folder_whitelist[${folder}]}"

    # Mostrar configuración
    if [ -z "$whitelist" ]; then
        highlight_message "Configuración: Descargar TODOS los archivos de ${folder}"
    else
        highlight_message "Configuración: Descargar sólo subcarpetas específicas de ${folder}: ${whitelist}"
    fi

    # Definimos el patrón para filtrar
    pattern="${folder}/"

    info_message "Listando objetos en s3://${S3_BUCKET}/${pattern}..."

    # Obtener lista de archivos (capturar errores)
    if ! aws s3 ls "s3://${S3_BUCKET}/${pattern}" --recursive --region "${REGION}" >"/tmp/${folder}_files.txt" 2>"/tmp/${folder}_error.txt"; then
        error_message "Error al listar archivos en ${folder}:"
        cat "/tmp/${folder}_error.txt"
        rm -f "/tmp/${folder}_files.txt" "/tmp/${folder}_error.txt"
        continue
    fi

    # Filtrar archivos que contienen los parámetros y terminan en .txt
    grep_pattern="explotation=${P_EMPRESA}/contract=${P_CONTR}/version=${P_VERSION}/"
    info_message "Filtrando archivos con patrón: ${grep_pattern}"

    if ! grep -E "${grep_pattern}" "/tmp/${folder}_files.txt" | grep "\.txt$" >"/tmp/${folder}_filtered.txt"; then
        warning_message "No se encontraron archivos con los parámetros especificados en ${folder}."
        rm -f "/tmp/${folder}_files.txt" "/tmp/${folder}_filtered.txt"
        continue
    fi

    # Filtrar por white list si está definida
    if [ -n "$whitelist" ]; then
        info_message "Aplicando filtro de white list..."
        >"/tmp/${folder}_whitelist.txt"

        for item in $whitelist; do
            # Buscar el patrón correspondiente en los archivos filtrados
            grep -E "${folder}/${item}/" "/tmp/${folder}_filtered.txt" >>"/tmp/${folder}_whitelist.txt" || true
        done

        # Reemplazar el archivo filtrado con el resultado de la white list
        if [ -s "/tmp/${folder}_whitelist.txt" ]; then
            mv "/tmp/${folder}_whitelist.txt" "/tmp/${folder}_filtered.txt"
        else
            warning_message "Ningún archivo coincide con la white list. No se descargarán archivos de ${folder}."
            rm -f "/tmp/${folder}_files.txt" "/tmp/${folder}_filtered.txt" "/tmp/${folder}_whitelist.txt"
            continue
        fi
    fi

    # Contar y mostrar archivos encontrados
    file_count=$(wc -l <"/tmp/${folder}_filtered.txt")
    success_message "Se encontraron ${file_count} archivos en ${folder} que coinciden con los criterios."

    # Mostrar los archivos encontrados
    echo "Archivos encontrados en ${folder}:"
    while read -r line; do
        s3_path=$(echo "$line" | awk '{print $4}')
        if [ -n "$s3_path" ]; then
            echo "  - $s3_path"

            # Crear directorio de destino manteniendo la estructura
            local_path="${OUTPUT_DIR}/${s3_path}"
            local_dir=$(dirname "${local_path}")
            mkdir -p "${local_dir}"

            # Descargar el archivo preservando la estructura
            info_message "Descargando ${s3_path} a ${local_path}..."
            if aws s3 cp "s3://${S3_BUCKET}/${s3_path}" "${local_path}" --region "${REGION}" >/dev/null 2>&1; then
                success_message "Archivo ${s3_path} descargado correctamente."
                ((total_downloaded++))
            else
                error_message "Error al descargar ${s3_path}."
                ((total_errors++))
            fi
        fi
    done <"/tmp/${folder}_filtered.txt"

    # Limpiar archivos temporales
    rm -f "/tmp/${folder}_files.txt" "/tmp/${folder}_filtered.txt" "/tmp/${folder}_error.txt"
done

section_header "VERIFICACIÓN FINAL"

# Contar archivos descargados
total_files=$(find "${OUTPUT_DIR}" -type f | wc -l)
if [ $total_files -gt 0 ]; then
    success_message "Proceso completado. Se descargaron ${total_files} archivos en total."
    info_message "Desglose de archivos por estructura de directorios:"
    find "${OUTPUT_DIR}" -type f | sed "s|^${OUTPUT_DIR}/||" | sort | while read -r file; do
        echo "  - $file"
    done

    finalize_script 0 "Proceso completado. Se descargaron ${total_files} archivos manteniendo la estructura original en ${OUTPUT_DIR}." ""
else
    finalize_script 1 "" "No se encontraron archivos para descargar con los parámetros especificados."
fi

exit $?
