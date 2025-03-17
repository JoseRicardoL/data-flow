#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "INICIANDO ENTORNO LOCAL"

# Verificar requisitos previos
check_prerequisites || exit 1

# Verificar puertos disponibles
info_message "Verificando disponibilidad de puertos..."
for port in 8888 4040; do
    if command -v lsof >/dev/null 2>&1 && lsof -i:$port -sTCP:LISTEN >/dev/null 2>&1; then
        warning_message "El puerto $port ya está en uso. Podría haber conflictos."
    fi
done

# Detener contenedores existentes
if check_container_running "glue_local"; then
    info_message "Deteniendo contenedores existentes..."
    run_docker_compose down
fi

# Iniciar servicios
info_message "Iniciando servicios Docker..."
run_docker_compose up --build -d || {
    error_message "Error al iniciar los servicios Docker"
    exit 1
}

# Esperar inicialización
info_message "Esperando inicialización del contenedor..."
attempt=0
max_attempts=15
while [ $attempt -lt $max_attempts ]; do
    if check_container_running "glue_local"; then
        if docker logs glue_local 2>&1 | grep -q "Jupyter.*running" ||
            docker logs glue_local 2>&1 | grep -q "http://127.0.0.1:8888"; then
            break
        fi
    fi

    echo -n "."
    sleep 2
    attempt=$((attempt + 1))
done
echo ""

# Verificación final
if check_container_running "glue_local"; then
    success_message "✓ Entorno Glue iniciado correctamente"

    # Verificar acceso a Jupyter
    if check_http_service "http://localhost:8888" 3; then
        success_message "✓ Jupyter Lab está accesible en http://localhost:8888"
    else
        warning_message "Jupyter Lab podría no estar accesible aún"
    fi

    # Crear directorios necesarios
    info_message "Configurando directorios de trabajo..."
    run_in_container "glue_local" "mkdir -p /home/glue_user/workspace/{notebooks,data}"

    info_message "Spark UI estará disponible en http://localhost:4040 cuando se ejecute un job"
else
    error_message "El contenedor se detuvo inesperadamente"
    docker logs glue_local | tail -30
    exit 1
fi
