#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "ACCESO A JUPYTER LAB"

# Asegurar que el contenedor está en ejecución
ensure_container_running "glue_local" "$(dirname "$0")/up.sh" || exit 1

# Verificar que Jupyter esté funcionando
JUPYTER_URL="http://localhost:8888"
info_message "Verificando acceso a Jupyter Lab..."

if ! check_http_service "$JUPYTER_URL" 3; then
    error_message "Jupyter Lab no está respondiendo"
    docker logs glue_local | tail -20
    exit 1
fi

# Abrir navegador
info_message "Abriendo navegador en $JUPYTER_URL..."
open_url_in_browser "$JUPYTER_URL"

success_message "✓ Jupyter Lab disponible en $JUPYTER_URL"
