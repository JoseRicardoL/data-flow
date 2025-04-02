#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

ARTIFACTORY_BUCKET=$1
REGION=$2

# Validar argumentos requeridos
validate_required_var "ARTIFACTORY_BUCKET" "$ARTIFACTORY_BUCKET" || exit 1
validate_required_var "REGION" "$REGION" || exit 1

# Mostrar información del script
show_script_info "CARGA DE NESTED STACKS A S3" "Subir templates de nested stacks a bucket S3" \
    "Bucket Artifactory" "$ARTIFACTORY_BUCKET" \
    "Región" "$REGION"

section_header "CARGA DE TEMPLATES"

# Lista de templates a subir
TEMPLATES=(
    "cloudformation/nested-stacks/template-dynamo.yaml"
    "cloudformation/nested-stacks/template-glue.yaml"
    "cloudformation/nested-stacks/template-lambda.yaml"
    "cloudformation/nested-stacks/template-param-store.yaml"
    "cloudformation/nested-stacks/template-step-functions.yaml"
)

for template in "${TEMPLATES[@]}"; do
    if [ ! -f "$template" ]; then
        error_message "El archivo $template no existe"
        continue
    fi
    
    template_name=$(basename "$template")
    destination="cloudformation/nested-stacks/$template_name"
    
    info_message "Subiendo $template a s3://${ARTIFACTORY_BUCKET}/${destination}..."
    
    aws s3 cp "$template" "s3://${ARTIFACTORY_BUCKET}/${destination}" --region "$REGION"
    show_result $? "Template $template_name cargado correctamente" "Error al cargar el template $template_name"
done

# Finalizar el script
finalize_script 0 "CARGA DE NESTED STACKS COMPLETADA EXITOSAMENTE" ""
exit 0