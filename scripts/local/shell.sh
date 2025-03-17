#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "ACCESO AL SHELL DEL CONTENEDOR"

# Asegurar que el contenedor está en ejecución
ensure_container_running "glue_local" "$(dirname "$0")/up.sh" || exit 1

# Acceder al shell
info_message "Accediendo al shell del contenedor Glue..."
docker exec -it glue_local bash -c "cd /home/glue_user/workspace && bash"

success_message "✓ Sesión de shell finalizada"
