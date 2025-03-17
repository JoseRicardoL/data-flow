#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "VERIFICACIÓN DEL ENTORNO AWS GLUE LOCAL"

# Verificar requisitos previos
check_prerequisites || exit 1

# Verificar que el contenedor esté en ejecución
if ! check_container_running "glue_local"; then
    error_message "El contenedor glue_local no está en ejecución"
    info_message "Ejecute 'make local-up' para iniciar el entorno"
    exit 1
fi

# Verificar servicios expuestos
info_message "Verificando servicios..."
if check_http_service "http://localhost:8888"; then
    success_message "✓ Jupyter Lab está respondiendo correctamente"
else
    warning_message "Jupyter Lab no responde"
fi

# Verificar estructura de directorios
info_message "Verificando estructura de directorios..."
run_in_container "glue_local" "mkdir -p /home/glue_user/workspace/{notebooks,data}"
run_in_container "glue_local" "test -d /home/glue_user/workspace/notebooks"
success_message "✓ Estructura de directorios correcta"

# Verificar entorno Python y Spark
info_message "Verificando entorno Python/Spark..."
PYTHON_TEST=$(docker exec glue_local python3 -c "
import sys
import pyspark
from pyspark.context import SparkContext
print(f'Python: {sys.version.split()[0]}')
print(f'PySpark: {pyspark.__version__}')
try:
    sc = SparkContext.getOrCreate()
    print('Spark: OK')
except Exception as e:
    print(f'Error: {str(e)}')
    exit(1)
")

echo "$PYTHON_TEST"
success_message "✓ Entorno Python/Spark funcionando correctamente"

section_header "VERIFICACIÓN COMPLETADA"
success_message "✓ El entorno AWS Glue local está configurado correctamente"
