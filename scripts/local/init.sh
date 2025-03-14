#!/bin/bash
set -e

# Verificar que el archivo de utilidades de formato exista
FORMAT_SCRIPT="$(dirname "$0")/../utils/format.sh"
if [ ! -f "$FORMAT_SCRIPT" ]; then
    echo "Error: No se encontró el archivo de utilidades de formato en $FORMAT_SCRIPT"
    exit 1
fi
source "$FORMAT_SCRIPT"

section_header "INICIALIZACIÓN DEL ENTORNO LOCAL AWS GLUE"

# 1. Verificar que Docker esté instalado
info_message "Verificando que Docker esté instalado..."
if ! command -v docker >/dev/null 2>&1; then
    error_message "Docker no está instalado o no está en el PATH. Instálalo y vuelve a intentar."
    exit 1
fi
success_message "Docker está instalado."

# 2. Verificar que Docker Compose esté instalado
info_message "Verificando que Docker Compose esté instalado..."
if ! command -v docker-compose >/dev/null 2>&1 && ! docker compose version >/dev/null 2>&1; then
    warning_message "Docker Compose no se encontró; asegúrate de que está instalado o utiliza 'docker compose'."
else
    success_message "Docker Compose está disponible."
fi

# 3. Verificar y crear directorios necesarios
info_message "Verificando directorios necesarios..."
mkdir -p local/.devcontainer
mkdir -p local/notebooks
success_message "Directorios verificados."

# 4. Asegurar permisos de ejecución en los scripts locales
info_message "Asegurando permisos de ejecución en scripts locales..."
SCRIPTS=("up.sh" "down.sh" "shell.sh" "jupyter.sh" "run.sh" "doctor.sh")
for script in "${SCRIPTS[@]}"; do
    SCRIPT_PATH="scripts/local/${script}"
    if [ -f "$SCRIPT_PATH" ]; then
        chmod +x "$SCRIPT_PATH"
        info_message "Permisos establecidos para $SCRIPT_PATH"
    else
        warning_message "No se encontró el script $SCRIPT_PATH"
    fi
done

# 5. Verificar la existencia de un archivo .env
if [ ! -f .env ]; then
    warning_message "No se encontró el archivo .env en la raíz. Verifica si es necesario para tu configuración."
else
    success_message "Archivo .env encontrado."
fi

section_header "VERIFICACIÓN COMPLETADA"
success_message "Entorno de desarrollo local verificado correctamente"
info_message "Para iniciar el entorno, ejecute: make local-up"
