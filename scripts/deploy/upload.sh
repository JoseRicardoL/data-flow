#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

LOCAL_PATH=$1
S3_BUCKET=$2
S3_PATH=$3

# Validar argumentos requeridos
validate_required_var "LOCAL_PATH" "$LOCAL_PATH" || exit 1
validate_required_var "S3_BUCKET" "$S3_BUCKET" || exit 1
validate_required_var "S3_PATH" "$S3_PATH" || exit 1

# Mostrar información del script
show_script_info "CARGA DE SCRIPT A S3" "Subir script a bucket S3" \
    "Archivo local" "$LOCAL_PATH" \
    "Bucket S3" "$S3_BUCKET" \
    "Ruta destino" "$S3_PATH"

section_header "VALIDACIÓN DE ARCHIVO"
# Verificar que el archivo existe
if [ ! -f "$LOCAL_PATH" ]; then
    warning_message "El archivo $LOCAL_PATH no existe"
    
    # Extraer directorio y nombre de archivo
    LOCAL_DIR=$(dirname "$LOCAL_PATH")
    FILE_NAME=$(basename "$LOCAL_PATH")
    
    # Crear directorio si no existe
    mkdir -p "$LOCAL_DIR"
    
    # Determinar tipo de script basado en el path
    if [[ "$LOCAL_PATH" == *"macro_generator"* ]]; then
        warning_message "Creando script de macro_generator..."
        cp "scripts/glue/macro_generator/glue_script.py" "$LOCAL_PATH"
    elif [[ "$LOCAL_PATH" == *"macro_stops_generator"* ]]; then
        warning_message "Creando script de macro_stops_generator..."
        cp "scripts/glue/macro_stops_generator/glue_script.py" "$LOCAL_PATH"
    else
        # Crear un script de placeholder que indica al usuario qué hacer
        echo '"""
Este es un script placeholder. Reemplaza este contenido con tu código real.
Para el pipeline GTFS, este script debería realizar la extracción inicial de datos
que alimenta al preprocesador Lambda y a los jobs de Glue.
"""

import sys
from awsglue.utils import getResolvedOptions

def main():
    # Obtener argumentos
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    print(f"Ejecutando {args[\"JOB_NAME\"]}")
    print("ATENCIÓN: Este es un script placeholder. Reemplaza con tu implementación real.")

if __name__ == "__main__":
    main()
' > "$LOCAL_PATH"
        warning_message "Creado script placeholder en $LOCAL_PATH"
    fi
fi

# Obtener el nombre del script
SCRIPT_NAME=$(basename $S3_PATH)

section_header "CARGA A S3"
info_message "Subiendo archivo a S3..."

# Verificar si la ruta S3 incluye un subdirectorio
S3_DIR=$(dirname "$S3_PATH")

# Subir a S3 construyendo la ruta completa
aws s3 cp "$LOCAL_PATH" "s3://${S3_BUCKET}/${S3_PATH}"
show_result $? "Archivo cargado correctamente: s3://${S3_BUCKET}/${S3_PATH}" "Error al cargar el archivo"

# Finalizar el script
finalize_script 0 "CARGA COMPLETADA EXITOSAMENTE" ""
exit 0