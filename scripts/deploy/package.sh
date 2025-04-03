#!/bin/bash
# Script unificado para empaquetar funciones Lambda y sus capas
# Versión corregida para compatibilidad con versiones recientes de Python

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

# Verificar parámetros
if [ "$#" -lt 2 ]; then
    error_message "Uso: $0 <bucket_name> <region>"
    error_message "  bucket_name: Nombre del bucket S3 (obligatorio)"
    error_message "  region: Región AWS (obligatorio)"
    exit 1
fi

BUCKET="$1"
REGION="$2"
ENV="${3:-dev}" # Ambiente para la identificación de las capas

section_header "EMPAQUETADO DE FUNCIONES LAMBDA"
info_message "Bucket: ${BUCKET}"
info_message "Región: ${REGION}"
info_message "Ambiente: ${ENV}"

# Obtener directorio raíz del proyecto (2 niveles arriba del script)
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
info_message "Directorio raíz del proyecto: ${PROJECT_ROOT}"

# Definir directorios de las funciones Lambda (agregamos state_machine_updater)
LAMBDA_DIRS=(
    "${PROJECT_ROOT}/scripts/lambda/pre_processor"
    "${PROJECT_ROOT}/scripts/lambda/check_capacity"
    "${PROJECT_ROOT}/scripts/lambda/release_capacity"
    "${PROJECT_ROOT}/scripts/lambda/trigger_next"
    "${PROJECT_ROOT}/scripts/lambda/state_machine_updater"
)

# Definir nombres de archivos ZIP (mantener la misma estructura que las carpetas)
ZIP_NAMES=(
    "pre_processor.zip"
    "check_capacity.zip"
    "release_capacity.zip"
    "trigger_next.zip"
    "state_machine_updater.zip"
)

# Crear directorio para archivos ZIP
LAMBDA_OUTPUT_DIR="${PROJECT_ROOT}/lambda"
mkdir -p "${LAMBDA_OUTPUT_DIR}"
info_message "Directorio de salida para archivos ZIP: ${LAMBDA_OUTPUT_DIR}"

# Crear directorio para guardar ARNs de las capas
mkdir -p "${PROJECT_ROOT}/layer_arns"
LAYER_ARNS_FILE="${PROJECT_ROOT}/layer_arns/${ENV}_layer_arns.env"
> "$LAYER_ARNS_FILE" # Crear/vaciar archivo

# Asegurar que el número de elementos sea el mismo
if [ ${#LAMBDA_DIRS[@]} -ne ${#ZIP_NAMES[@]} ]; then
    error_message "Error de configuración: el número de directorios y archivos ZIP no coincide"
    exit 1
fi

# Procesar cada función Lambda
for i in "${!LAMBDA_DIRS[@]}"; do
    LAMBDA_DIR="${LAMBDA_DIRS[$i]}"
    ZIP_NAME="${ZIP_NAMES[$i]}"
    OUTPUT_PATH="${LAMBDA_OUTPUT_DIR}/${ZIP_NAME}"
    FUNCTION_NAME=$(basename "${LAMBDA_DIR}")

    section_header "EMPAQUETANDO ${FUNCTION_NAME}"
    info_message "Función: ${LAMBDA_DIR}"
    info_message "Archivo ZIP: ${OUTPUT_PATH}"

    # Verificar que el directorio existe
    if [ ! -d "${LAMBDA_DIR}" ]; then
        error_message "El directorio ${LAMBDA_DIR} no existe"
        continue
    fi

    # Verificar que el archivo lambda_function.py existe
    if [ ! -f "${LAMBDA_DIR}/lambda_function.py" ]; then
        error_message "No se encontró el archivo lambda_function.py en ${LAMBDA_DIR}"
        continue
    fi

    # 1. CREAR CAPA LAMBDA PARA LA FUNCIÓN (si aplica)
    section_header "CREANDO CAPA PARA ${FUNCTION_NAME}"

    LAYER_NAME="${FUNCTION_NAME}-layer-${ENV}"
    LAYER_ZIP="${LAMBDA_OUTPUT_DIR}/${FUNCTION_NAME}_layer.zip"

    # Determinar dependencias según la función
    DEPENDENCIES=""
    case "${FUNCTION_NAME}" in
        pre_processor)
            # En pre_processor se requieren dependencias (por ejemplo, psutil)
            DEPENDENCIES="psutil"
            ;;
        *)
            DEPENDENCIES=""
            ;;
    esac

    if [ -n "$DEPENDENCIES" ]; then
        # Crear directorio temporal para la capa
        LAYER_TEMP_DIR=$(mktemp -d)
        mkdir -p "${LAYER_TEMP_DIR}/python"

        # Instalar dependencias en la capa
        info_message "Instalando dependencias para capa: ${DEPENDENCIES}"
        pip install ${DEPENDENCIES} -t "${LAYER_TEMP_DIR}/python" --prefer-binary
        INSTALL_RESULT=$?

        if [ $INSTALL_RESULT -eq 0 ]; then
            # Comprimir capa
            info_message "Creando archivo ZIP para capa..."
            (cd "${LAYER_TEMP_DIR}" && zip -r "${LAYER_ZIP}" .)
            LAYER_ZIP_RESULT=$?

            if [ $LAYER_ZIP_RESULT -eq 0 ]; then
                # Subir capa a S3
                info_message "Subiendo capa a S3..."
                S3_LAYER_KEY="lambda_layers/${ENV}/${FUNCTION_NAME}_layer.zip"
                aws s3 cp "${LAYER_ZIP}" "s3://${BUCKET}/${S3_LAYER_KEY}" --region "${REGION}"

                # Publicar capa Lambda
                info_message "Publicando capa Lambda..."
                LAYER_ARN=$(aws lambda publish-layer-version \
                    --layer-name "${LAYER_NAME}" \
                    --description "Dependencias para ${FUNCTION_NAME}" \
                    --compatible-runtimes python3.11 \
                    --content S3Bucket="${BUCKET}",S3Key="${S3_LAYER_KEY}" \
                    --region "${REGION}" \
                    --query 'LayerVersionArn' \
                    --output text)

                # Guardar ARN en archivo
                echo "${FUNCTION_NAME^^}_LAYER_ARN=${LAYER_ARN}" >> "$LAYER_ARNS_FILE"

                # Para pre_processor, agregar también el ARN fijo de pandas (si es necesario)
                if [ "${FUNCTION_NAME}" == "pre_processor" ]; then
                    echo "PANDAS_LAYER_ARN=arn:aws:lambda:eu-west-1:336392948345:layer:AWSLambda-Python311-SciPy1x:1" >> "$LAYER_ARNS_FILE"
                fi

                success_message "Capa para ${FUNCTION_NAME} publicada: ${LAYER_ARN}"
            else
                error_message "Error al crear archivo ZIP para capa"
            fi
        else
            error_message "Error al instalar dependencias para la capa"
        fi

        # Limpiar directorio temporal de la capa
        rm -rf "${LAYER_TEMP_DIR}"
    else
        info_message "No hay dependencias para ${FUNCTION_NAME}. Omitiendo creación de capa."
    fi

    # 2. EMPAQUETAR FUNCIÓN LAMBDA (código sin dependencias)
    section_header "EMPAQUETANDO FUNCIÓN ${FUNCTION_NAME}"
    info_message "Creando archivo ZIP ligero..."
    (cd "${LAMBDA_DIR}" && zip -r "${OUTPUT_PATH}" .)
    ZIP_RESULT=$?

    if [ $ZIP_RESULT -eq 0 ]; then
        # Subir archivo ZIP a S3
        info_message "Subiendo archivo ZIP a S3..."
        aws s3 cp "${OUTPUT_PATH}" "s3://${BUCKET}/lambda/${ZIP_NAME}" --region "${REGION}"
        S3_RESULT=$?

        show_result $S3_RESULT "Archivo ZIP subido a s3://${BUCKET}/lambda/${ZIP_NAME}" "Error al subir archivo ZIP"
    else
        error_message "Error al crear archivo ZIP"
    fi

    info_message "Limpieza completa para ${FUNCTION_NAME}"
done

success_message "Empaquetado completado. ARNs de capas guardados en: ${LAYER_ARNS_FILE}"
exit 0
