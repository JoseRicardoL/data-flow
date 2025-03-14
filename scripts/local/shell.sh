#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

section_header "CONECTANDO AL SHELL"
info_message "Conectando al shell del contenedor AWS Glue..."

docker exec -it glue_local bash

success_message "Sesión de shell finalizada"
