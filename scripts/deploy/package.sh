#!/bin/bash
# Script unificado para empaquetar todas las funciones Lambda del sistema

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

section_header "EMPAQUETADO DE FUNCIONES LAMBDA"
info_message "Bucket: ${BUCKET}"
info_message "Región: ${REGION}"

# Obtener directorio raíz del proyecto (2 niveles arriba del script)
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
info_message "Directorio raíz del proyecto: ${PROJECT_ROOT}"

# Definir directorios de las funciones Lambda
LAMBDA_DIRS=(
    "${PROJECT_ROOT}/scripts/lambda/pre_processor"
    "${PROJECT_ROOT}/scripts/lambda/check_capacity"
    "${PROJECT_ROOT}/scripts/lambda/release_capacity"
    "${PROJECT_ROOT}/scripts/lambda/trigger_next"
)

# Definir nombres de archivos ZIP (mantener la misma estructura que las carpetas)
ZIP_NAMES=(
    "pre_processor.zip"
    "check_capacity.zip" 
    "release_capacity.zip"
    "trigger_next.zip"
)

# Crear directorio para archivos ZIP
LAMBDA_OUTPUT_DIR="${PROJECT_ROOT}/lambda"
mkdir -p "${LAMBDA_OUTPUT_DIR}"
info_message "Directorio de salida para archivos ZIP: ${LAMBDA_OUTPUT_DIR}"

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
    
    section_header "EMPAQUETANDO ${LAMBDA_DIR}"
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
    
    # Crear directorio temporal
    TEMP_DIR=$(mktemp -d)
    info_message "Directorio temporal: ${TEMP_DIR}"
    
    # Copiar archivos al directorio temporal
    cp -r ${LAMBDA_DIR}/* ${TEMP_DIR}/
    success_message "Archivos copiados al directorio temporal"
    
    # Determinar dependencias según la función
    DEPENDENCIES="boto3"
    case "${LAMBDA_DIR}" in
        *pre_processor*)
            DEPENDENCIES="boto3 pandas==1.5.3 psutil"
            ;;
        *)
            DEPENDENCIES="boto3"
            ;;
    esac
    
    # Instalar dependencias en el directorio temporal
    info_message "Instalando dependencias: ${DEPENDENCIES}"
    pip install -t ${TEMP_DIR} ${DEPENDENCIES} --upgrade --no-deps
    show_result $? "Dependencias instaladas" "Error al instalar dependencias"
    
    # Crear archivo ZIP (usando ruta absoluta para evitar problemas)
    info_message "Creando archivo ZIP..."
    (cd ${TEMP_DIR} && zip -r "${OUTPUT_PATH}" .)
    ZIP_RESULT=$?
    show_result $ZIP_RESULT "Archivo ZIP creado: ${OUTPUT_PATH}" "Error al crear archivo ZIP"
    
    if [ $ZIP_RESULT -eq 0 ]; then
        # Subir archivo ZIP a S3
        info_message "Subiendo archivo ZIP a S3..."
        aws s3 cp "${OUTPUT_PATH}" "s3://${BUCKET}/lambda/${ZIP_NAME}" --region ${REGION}
        show_result $? "Archivo ZIP subido a s3://${BUCKET}/lambda/${ZIP_NAME}" "Error al subir archivo ZIP"
    fi
    
    # Limpiar directorio temporal
    rm -rf ${TEMP_DIR}
    success_message "Directorio temporal eliminado"
done

finalize_script 0 "EMPAQUETADO DE FUNCIONES LAMBDA COMPLETADO" ""
exit 0