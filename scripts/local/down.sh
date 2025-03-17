#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "DETENIENDO ENTORNO LOCAL"

# Verificar requisitos previos
check_prerequisites || exit 1

# Verificar si hay contenedores en ejecución
if ! check_container_running "glue_local"; then
    warning_message "No hay contenedores en ejecución"
    exit 0
fi

# Detener contenedores
info_message "Deteniendo servicios Docker..."
run_docker_compose down || {
    error_message "Error al detener los servicios Docker"
    info_message "Intentando forzar la detención..."
    docker rm -f glue_local >/dev/null 2>&1 || true
}

# Verificación final
if check_container_running "glue_local"; then
    error_message "No se pudo detener completamente el contenedor"
    exit 1
else
    success_message "Entorno detenido correctamente"
fi
