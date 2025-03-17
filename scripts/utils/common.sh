#!/bin/bash

# Importar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

# Verificar Docker y dependencias
check_prerequisites() {
    # Verificar Docker
    if ! command -v docker >/dev/null 2>&1; then
        error_message "Docker no está instalado"
        return 1
    fi

    if ! docker info >/dev/null 2>&1; then
        error_message "El servicio Docker no está en ejecución"
        return 1
    fi

    # Verificar Docker Compose
    if command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        error_message "Docker Compose no está instalado"
        return 1
    fi

    export DOCKER_COMPOSE_CMD
    return 0
}

# Verificar estado de contenedor
check_container_running() {
    local container="$1"
    docker ps | grep -q "$container"
    return $?
}

# Verificar acceso a servicio HTTP
check_http_service() {
    local url="$1"
    local max_retries="${2:-1}"
    local retry=0

    while [ $retry -lt $max_retries ]; do
        if command -v curl >/dev/null 2>&1; then
            if curl -s --head --request GET "$url" | grep -E "200 OK|302 Found" >/dev/null; then
                return 0
            fi
        elif command -v wget >/dev/null 2>&1; then
            if wget -q --spider "$url"; then
                return 0
            fi
        else
            warning_message "Ni curl ni wget están disponibles para verificar servicios HTTP"
            return 0 # Asumimos que está funcionando
        fi

        retry=$((retry + 1))
        [ $retry -lt $max_retries ] && sleep 2
    done

    return 1
}

# Abrir URL en navegador
open_url_in_browser() {
    local url="$1"

    for cmd in "xdg-open" "open" "start" "python3 -m webbrowser" "python -m webbrowser"; do
        if command -v ${cmd%% *} >/dev/null 2>&1; then
            if $cmd "$url" >/dev/null 2>&1; then
                return 0
            fi
        fi
    done

    info_message "No se pudo abrir automáticamente. URL: $url"
    return 1
}

# Ejecutar docker compose
run_docker_compose() {
    local action="$1"
    shift

    pushd local >/dev/null 2>&1 || {
        error_message "No se pudo acceder al directorio local"
        return 1
    }

    $DOCKER_COMPOSE_CMD $action "$@"
    local result=$?

    popd >/dev/null 2>&1 || true
    return $result
}

# Ejecutar comando dentro del contenedor
run_in_container() {
    local container="$1"
    local cmd="$2"

    if ! check_container_running "$container"; then
        error_message "El contenedor $container no está en ejecución"
        return 1
    fi

    docker exec -i "$container" bash -c "$cmd"
    return $?
}

# Asegurar que el contenedor está en ejecución
ensure_container_running() {
    local container="$1"
    local start_script="$2"

    if ! check_container_running "$container"; then
        warning_message "El contenedor $container no está en ejecución"
        if [ -n "$start_script" ] && [ -f "$start_script" ]; then
            info_message "Iniciando contenedor..."
            bash "$start_script"
            return $?
        else
            error_message "No se pudo iniciar el contenedor automáticamente"
            return 1
        fi
    fi

    return 0
}

# Validar archivo existe
validate_file_exists() {
    local file="$1"
    local description="${2:-archivo}"

    if [ ! -f "$file" ]; then
        error_message "El $description no existe: $file"
        return 1
    fi

    return 0
}
