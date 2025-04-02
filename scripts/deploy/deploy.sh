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

# Verificar si existe el template
if [ ! -f "cloudformation/parameters/${ENV}.json.template" ]; then
    error_message "No se encontró el archivo template: cloudformation/parameters/${ENV}.json.template"
    info_message "Creando template basado en el archivo ${ENV}.json existente..."
    
    if [ -f "cloudformation/parameters/${ENV}.json" ]; then
        # Crear un template básico a partir del archivo existente
        cat > "cloudformation/parameters/${ENV}.json.template" << EOF
{
  "environment": "${ENV}",
  "region": "${REGION}",
  "bucket": "${S3_BUCKET}",
  "artifactBucket": "${ARTIFACTORY_BUCKET:-${S3_BUCKET}}",
  "glueJobConfig": {
    "workerType": "G.1X",
    "numberOfWorkers": 2,
    "timeout": 2880
  },
  "vpcConfig": {
    "vpcId": "${VPC_ID:-}",
    "subnets": "${VPC_SUBNETS:-}",
    "securityGroupId": "${SECURITY_GROUP_ID:-}",
    "routeTableId": "${ROUTE_TABLE_ID:-}"
  },
  "layerArns": {
    "preProcessor": "${PRE_PROCESSOR_LAYER_ARN:-}",
    "checkCapacity": "${CHECK_CAPACITY_LAYER_ARN:-}",
    "releaseCapacity": "${RELEASE_CAPACITY_LAYER_ARN:-}",
    "triggerNext": "${TRIGGER_NEXT_LAYER_ARN:-}"
  },
  "concurrency": {
    "maxRuns": 25,
    "maxStateMachines": 5
  }
}
EOF
        success_message "Template creado: cloudformation/parameters/${ENV}.json.template"
    else
        error_message "No se encontró el archivo de parámetros: cloudformation/parameters/${ENV}.json"
        finalize_script 1 "" "DESPLIEGUE FALLIDO"
        exit 1
    fi
fi

# Aplicar sustitución de variables de entorno
envsubst < "cloudformation/parameters/${ENV}.json.template" > "cloudformation/parameters/${ENV}.json"
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
    --template-file cloudformation/template.yaml \
    --s3-bucket ${S3_BUCKET} \
    --s3-prefix ${GLUE_SCRIPTS_PATH} \
    --output-template-file ${PACKAGED_TEMPLATE} >/dev/null

show_result $? "Template empaquetado correctamente: ${PACKAGED_TEMPLATE}" "Error al empaquetar template"

# Cargar los ARNs de las capas Lambda si existen
LAYER_ARNS_FILE="layer_arns/${ENV}_layer_arns.env"
if [ -f "$LAYER_ARNS_FILE" ]; then
    info_message "Cargando ARNs de las capas Lambda..."
    source "$LAYER_ARNS_FILE"
    
    # Verificar que las variables necesarias estén definidas
    if [ -z "$PRE_PROCESSOR_LAYER_ARN" ] || [ -z "$CHECK_CAPACITY_LAYER_ARN" ] || [ -z "$RELEASE_CAPACITY_LAYER_ARN" ] || [ -z "$TRIGGER_NEXT_LAYER_ARN" ]; then
        warning_message "Algunas variables de ARNs de capas no están definidas en $LAYER_ARNS_FILE"
        info_message "Esto puede provocar que el despliegue falle si las capas son requeridas"
    fi
else
    warning_message "No se encontró el archivo de ARNs de capas: $LAYER_ARNS_FILE"
    info_message "Si las capas son requeridas, el despliegue podría fallar"
fi

# Extraer parámetros del archivo JSON generado
section_header "PREPARANDO PARÁMETROS PARA CLOUDFORMATION"
info_message "Extrayendo parámetros para CloudFormation..."

# Leer el archivo JSON de parámetros
if [ ! -f "cloudformation/parameters/${ENV}.json" ]; then
    error_message "No se encontró el archivo de parámetros: cloudformation/parameters/${ENV}.json"
    finalize_script 1 "" "DESPLIEGUE FALLIDO"
    exit 1
fi

# Construir parámetros para CloudFormation
PARAMS_FILE="cloudformation/parameters/${ENV}.json"
CF_PARAMS=""

if command -v jq >/dev/null 2>&1; then
    # Método con jq (más robusto)
    info_message "Usando jq para procesar parámetros..."
    
    # Extraer parámetros básicos - Sin intentar caer de vuelta a variables de entorno dentro de jq
    ENV_VALUE=$(jq -r '.environment' $PARAMS_FILE)
    [ -z "$ENV_VALUE" ] && ENV_VALUE="$ENV"
    
    BUCKET_VALUE=$(jq -r '.bucket' $PARAMS_FILE)
    [ -z "$BUCKET_VALUE" ] && BUCKET_VALUE="$S3_BUCKET"
    
    ARTIFACT_BUCKET=$(jq -r '.artifactBucket' $PARAMS_FILE)
    [ -z "$ARTIFACT_BUCKET" ] && ARTIFACT_BUCKET="${ARTIFACTORY_BUCKET:-$S3_BUCKET}"
    
    # Extraer configuración Glue
    WORKER_TYPE=$(jq -r '.glueJobConfig.workerType' $PARAMS_FILE)
    [ -z "$WORKER_TYPE" ] && WORKER_TYPE="G.1X"
    
    WORKERS=$(jq -r '.glueJobConfig.numberOfWorkers' $PARAMS_FILE)
    [ -z "$WORKERS" ] && WORKERS="2"
    
    # Extraer configuración de concurrencia
    MAX_RUNS=$(jq -r '.concurrency.maxRuns' $PARAMS_FILE)
    [ -z "$MAX_RUNS" ] && MAX_RUNS="25"
    
    MAX_SM=$(jq -r '.concurrency.maxStateMachines' $PARAMS_FILE)
    [ -z "$MAX_SM" ] && MAX_SM="5"
    
    # Capas Lambda - usar variables de entorno si están definidas
    PRE_PROCESSOR_LAYER_ARN=$(jq -r '.layerArns.preProcessor' $PARAMS_FILE)
    [ -z "$PRE_PROCESSOR_LAYER_ARN" ] && PRE_PROCESSOR_LAYER_ARN="${PRE_PROCESSOR_LAYER_ARN:-}"
    
    CHECK_CAPACITY_LAYER_ARN=$(jq -r '.layerArns.checkCapacity' $PARAMS_FILE)
    [ -z "$CHECK_CAPACITY_LAYER_ARN" ] && CHECK_CAPACITY_LAYER_ARN="${CHECK_CAPACITY_LAYER_ARN:-}"
    
    RELEASE_CAPACITY_LAYER_ARN=$(jq -r '.layerArns.releaseCapacity' $PARAMS_FILE)
    [ -z "$RELEASE_CAPACITY_LAYER_ARN" ] && RELEASE_CAPACITY_LAYER_ARN="${RELEASE_CAPACITY_LAYER_ARN:-}"
    
    TRIGGER_NEXT_LAYER_ARN=$(jq -r '.layerArns.triggerNext' $PARAMS_FILE)
    [ -z "$TRIGGER_NEXT_LAYER_ARN" ] && TRIGGER_NEXT_LAYER_ARN="${TRIGGER_NEXT_LAYER_ARN:-}"
    
    # Construir string de parámetros
    CF_PARAMS="S3Bucket=${BUCKET_VALUE} \
S3BUCKETArtifactory=${ARTIFACT_BUCKET} \
GlueJobName=${STACK_NAME} \
Environment=${ENV_VALUE} \
WorkerType=${WORKER_TYPE} \
NumberOfWorkers=${WORKERS} \
MaxConcurrentRuns=${MAX_RUNS} \
MaxConcurrentStateMachines=${MAX_SM} \
LambdaS3KeyPrefix=lambda \
PreProcessorLayerArn=${PRE_PROCESSOR_LAYER_ARN} \
CheckCapacityLayerArn=${CHECK_CAPACITY_LAYER_ARN} \
ReleaseCapacityLayerArn=${RELEASE_CAPACITY_LAYER_ARN} \
TriggerNextLayerArn=${TRIGGER_NEXT_LAYER_ARN}"
else
    # Método alternativo sin jq
    warning_message "jq no está instalado, usando método alternativo para parámetros..."
    
    # Si no está instalado jq, usar valores de variables de entorno
    CF_PARAMS="S3Bucket=${S3_BUCKET} \
S3BUCKETArtifactory=${ARTIFACTORY_BUCKET:-$S3_BUCKET} \
GlueJobName=${STACK_NAME} \
Environment=${ENV} \
WorkerType=G.1X \
NumberOfWorkers=2 \
MaxConcurrentRuns=25 \
MaxConcurrentStateMachines=5 \
LambdaS3KeyPrefix=lambda \
PreProcessorLayerArn=${PRE_PROCESSOR_LAYER_ARN:-} \
CheckCapacityLayerArn=${CHECK_CAPACITY_LAYER_ARN:-} \
ReleaseCapacityLayerArn=${RELEASE_CAPACITY_LAYER_ARN:-} \
TriggerNextLayerArn=${TRIGGER_NEXT_LAYER_ARN:-}"
fi

# Desplegar el stack
section_header "DESPLEGANDO STACK"
info_message "Iniciando despliegue CloudFormation..."
warning_message "Este proceso puede tomar varios minutos. Por favor, espere...\n"

# Mostrar detalles de parámetros
info_message "Parámetros a utilizar:"
echo -e "${CYAN}${CF_PARAMS}${NC}"

aws cloudformation deploy \
    --template-file ${PACKAGED_TEMPLATE} \
    --stack-name ${STACK_NAME} \
    --parameter-overrides ${CF_PARAMS} \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --region ${REGION} \
    --no-fail-on-empty-changeset

DEPLOY_RESULT=$?

# Limpiar archivos temporales
section_header "LIMPIEZA"
info_message "Eliminando archivos temporales..."
cleanup_temp_files "${PACKAGED_TEMPLATE}"

# Mostrar outputs del stack si el despliegue fue exitoso
if [ $DEPLOY_RESULT -eq 0 ]; then
    show_stack_outputs "$STACK_NAME" "$REGION"
fi

# Finalizar el script
finalize_script $DEPLOY_RESULT "¡DESPLIEGUE COMPLETADO CON ÉXITO!" "ERROR EN EL DESPLIEGUE"
exit $DEPLOY_RESULT