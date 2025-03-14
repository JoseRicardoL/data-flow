#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

section_header "DETENIENDO ENTORNO LOCAL"
info_message "Deteniendo entorno AWS Glue..."

cd local && docker compose down

success_message "Entorno detenido correctamente"
