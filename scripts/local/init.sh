#!/bin/bash
set -eo pipefail

# Cargar funciones comunes
source "$(dirname "$0")/../utils/common.sh"

section_header "INICIALIZACIÓN DEL ENTORNO LOCAL AWS GLUE"

# Verificar requisitos previos
check_prerequisites || exit 1

# Crear directorios de trabajo
info_message "Creando directorios de trabajo..."
mkdir -p local/workspace/{notebooks,data}
success_message "✓ Estructura de directorios creada correctamente"

# Configurar permisos de ejecución para todos los scripts
info_message "Configurando permisos de scripts..."
find scripts -name "*.sh" -exec chmod +x {} \;
success_message "✓ Permisos de scripts configurados"

# Verificar archivo .env
info_message "Verificando archivo de variables de entorno..."
if [ ! -f .env ]; then
    if [ -f .env.template ]; then
        cp .env.template .env
        success_message "✓ Archivo .env creado desde plantilla"
    else
        warning_message "No se encontró .env ni .env.template"
        echo "AWS_ACCESS_KEY_ID=" >.env
        echo "AWS_SECRET_ACCESS_KEY=" >>.env
        echo "AWS_SESSION_TOKEN=" >>.env
        info_message "Se ha creado un archivo .env vacío"
    fi
else
    success_message "✓ Archivo .env ya existe"
fi

section_header "INICIALIZACIÓN COMPLETADA"
success_message "✓ Entorno preparado correctamente"
info_message "Para iniciar el entorno, ejecute: make local-up"
