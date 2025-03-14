#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

STACK_NAME=$1
REGION=$2
ENV=$3
S3_BUCKET=$4
GLUE_SCRIPTS_PATH=$5

# Validar argumentos requeridos
validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "REGION" "$REGION" || exit 1
validate_required_var "ENV" "$ENV" || exit 1
validate_required_var "S3_BUCKET" "$S3_BUCKET" || exit 1
validate_required_var "GLUE_SCRIPTS_PATH" "$GLUE_SCRIPTS_PATH" || exit 1

# Mostrar información del script
show_script_info "DESPLIEGUE DE INFRAESTRUCTURA" "Despliegue de stack CloudFormation" \
    "Stack" "$STACK_NAME" \
    "Región" "$REGION" \
    "Ambiente" "$ENV" \
    "Bucket S3" "$S3_BUCKET" \
    "Ruta Scripts" "$GLUE_SCRIPTS_PATH"

export STACK_NAME
export ENV
export S3_BUCKET
export GLUE_SCRIPTS_PATH

# Generar parámetros desde template
section_header "PREPARANDO PARÁMETROS"
info_message "Generando archivo de parámetros..."
envsubst <cloudformation/parameters/${ENV}.json.template >cloudformation/parameters/${ENV}.json
show_result $? "Parámetros generados correctamente" "Error al generar parámetros"

# Verificar el estado actual del stack
section_header "VERIFICANDO ESTADO DEL STACK"
info_message "Consultando estado actual..."
STATUS=$(check_stack_status ${STACK_NAME} ${REGION})
echo -e "${CYAN}Estado actual: ${BOLD}${STATUS}${NC}"

# Esperar si el stack está en proceso
if [[ "$STATUS" == *"IN_PROGRESS"* ]]; then
    warning_message "El stack está en proceso. Esperando a que finalice..."
    spinner=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
    counter=0

    while [[ "$STATUS" == *"IN_PROGRESS"* ]]; do
        idx=$(($counter % ${#spinner[@]}))
        echo -ne "\r${YELLOW}${spinner[$idx]} Esperando... Estado: ${STATUS} ($(date +%H:%M:%S))${NC}"
        sleep 5
        STATUS=$(check_stack_status ${STACK_NAME} ${REGION})
        counter=$((counter + 1))
    done
    echo -e "\r${GREEN}${CHECK} Estado final de la operación previa: ${STATUS}                   ${NC}"
fi

# Crear la ruta para el template empaquetado
PACKAGED_TEMPLATE="packaged-${ENV}.yaml"

# Empaquetar el template
section_header "EMPAQUETANDO TEMPLATE"
info_message "Empaquetando recursos en S3..."
aws cloudformation package \
    --template-file cloudformation/glue-job.yaml \
    --s3-bucket ${S3_BUCKET} \
    --s3-prefix ${GLUE_SCRIPTS_PATH} \
    --output-template-file ${PACKAGED_TEMPLATE} >/dev/null

show_result $? "Template empaquetado correctamente: ${PACKAGED_TEMPLATE}" "Error al empaquetar template"

# Desplegar el stack
section_header "DESPLEGANDO STACK"
info_message "Iniciando despliegue CloudFormation..."
warning_message "Este proceso puede tomar varios minutos. Por favor, espere...\n"

aws cloudformation deploy \
    --template-file ${PACKAGED_TEMPLATE} \
    --stack-name ${STACK_NAME} \
    --parameter-overrides file://cloudformation/parameters/${ENV}.json \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --region ${REGION} \
    --no-fail-on-empty-changeset

DEPLOY_RESULT=$?

# Limpiar archivos temporales
section_header "LIMPIEZA"
info_message "Eliminando archivos temporales..."
cleanup_temp_files "${PACKAGED_TEMPLATE}" "cloudformation/parameters/${ENV}.json"

# Mostrar outputs del stack si el despliegue fue exitoso
if [ $DEPLOY_RESULT -eq 0 ]; then
    show_stack_outputs "$STACK_NAME" "$REGION"
fi

# Finalizar el script
finalize_script $DEPLOY_RESULT "¡DESPLIEGUE COMPLETADO CON ÉXITO!" "ERROR EN EL DESPLIEGUE"
exit $DEPLOY_RESULT
