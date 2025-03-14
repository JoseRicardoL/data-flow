#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

section_header "INICIANDO ENTORNO LOCAL"

info_message "→ Instalando dependencias con Pipenv..."
cd local && docker compose up dependencies

info_message "→ Iniciando entorno AWS Glue para desarrollo local..."
docker compose up -d glue

info_message "→ Esperando a que se inicialice..."
sleep 5

if docker ps | grep -q glue_local; then
    success_message "✓ Entorno iniciado correctamente"
    info_message "Jupyter Lab: http://localhost:8888"
    info_message "Spark UI: http://localhost:4040"
else
    error_message "✗ El contenedor se detuvo inesperadamente"
    docker logs glue_local
    exit 1
fi