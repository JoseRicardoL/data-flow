#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

REGION=$1

# Validar argumentos requeridos
validate_required_var "REGION" "$REGION" || exit 1

# Mostrar información del script
show_script_info "VALIDACIÓN DE TEMPLATE" "Validar sintaxis del template CloudFormation" \
    "Región" "$REGION"

section_header "VALIDANDO TEMPLATE"
info_message "Validando template CloudFormation..."

aws cloudformation validate-template \
    --template-body file://cloudformation/glue-job.yaml \
    --region $REGION

show_result $? "Template validado correctamente" "Error en la validación del template"

# Finalizar el script
finalize_script 0 "VALIDACIÓN COMPLETADA EXITOSAMENTE" ""
exit 0
