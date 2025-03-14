#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

section_header "ACCESO A JUPYTER LAB"
info_message "Abriendo navegador en Jupyter Lab..."

python -m webbrowser http://localhost:8888

success_message "Navegador abierto con Jupyter Lab"
