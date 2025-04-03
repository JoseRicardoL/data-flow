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

# Cargar y exportar variables de ARNs de capas desde layer_arns/<env>_layer_arns.env
LAYER_ARNS_FILE="layer_arns/${ENV}_layer_arns.env"
if [ -f "$LAYER_ARNS_FILE" ]; then
    info_message "Cargando ARNs de las capas Lambda desde $LAYER_ARNS_FILE..."
    export PRE_PROCESSOR_LAYER_ARN=$(grep PRE_PROCESSOR_LAYER_ARN "$LAYER_ARNS_FILE" | cut -d'=' -f2)
    export PANDAS_LAYER_ARN=$(grep PANDAS_LAYER_ARN "$LAYER_ARNS_FILE" | cut -d'=' -f2)
else
    warning_message "No se encontró el archivo de ARNs de capas: $LAYER_ARNS_FILE"
    export PRE_PROCESSOR_LAYER_ARN=""
    export PANDAS_LAYER_ARN=""
fi

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
  "artifactBucket": "${ARTIFACTORY_BUCKET}",
  "glueJobConfig": {
    "workerType": "G.1X",
    "numberOfWorkers": 2,
    "timeout": 2880
  },
  "vpcConfig": {
    "vpcId": "${VPC_ID}",
    "subnets": "${VPC_SUBNETS}",
    "securityGroupId": "${SECURITY_GROUP_ID}",
    "routeTableId": "${ROUTE_TABLE_ID}"
  },
  "layerArns": {
    "preProcessor": "${PRE_PROCESSOR_LAYER_ARN}",
    "pandasLayer": "${PANDAS_LAYER_ARN}"
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

# Extraer parámetros del archivo JSON generado
section_header "PREPARANDO PARÁMETROS PARA CLOUDFORMATION"
info_message "Extrayendo parámetros para CloudFormation..."

if [ ! -f "cloudformation/parameters/${ENV}.json" ]; then
    error_message "No se encontró el archivo de parámetros: cloudformation/parameters/${ENV}.json"
    finalize_script 1 "" "DESPLIEGUE FALLIDO"
    exit 1
fi

PARAMS_FILE="cloudformation/parameters/${ENV}.json"
CF_PARAMS=""

if command -v jq >/dev/null 2>&1; then
    info_message "Usando jq para procesar parámetros..."
    ENV_VALUE=$(jq -r ".environment // \"$ENV\"" $PARAMS_FILE)
    BUCKET_VALUE=$(jq -r ".bucket // \"$S3_BUCKET\"" $PARAMS_FILE)
    ARTIFACT_BUCKET=$(jq -r ".artifactBucket // \"${ARTIFACTORY_BUCKET:-$S3_BUCKET}\"" $PARAMS_FILE)
    WORKER_TYPE=$(jq -r '.glueJobConfig.workerType // "G.1X"' $PARAMS_FILE)
    WORKERS=$(jq -r '.glueJobConfig.numberOfWorkers // 2' $PARAMS_FILE)
    MAX_RUNS=$(jq -r '.concurrency.maxRuns // 25' $PARAMS_FILE)
    MAX_SM=$(jq -r '.concurrency.maxStateMachines // 5' $PARAMS_FILE)
    PRE_PROCESSOR_LAYER_ARN=$(jq -r '.layerArns.preProcessor // env.PRE_PROCESSOR_LAYER_ARN // ""' $PARAMS_FILE)
    PANDAS_LAYER_ARN=$(jq -r '.layerArns.pandasLayer // env.PANDAS_LAYER_ARN // ""' $PARAMS_FILE)
    
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
PandasLayerArn=${PANDAS_LAYER_ARN}"
else
    warning_message "jq no está instalado, usando método alternativo para parámetros..."
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
PandasLayerArn=${PANDAS_LAYER_ARN:-}"
fi

section_header "DESPLEGANDO STACK"
info_message "Iniciando despliegue CloudFormation..."
warning_message "Este proceso puede tomar varios minutos. Por favor, espere...\n"
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

section_header "LIMPIEZA"
info_message "Eliminando archivos temporales..."
cleanup_temp_files "${PACKAGED_TEMPLATE}"

if [ $DEPLOY_RESULT -eq 0 ]; then
    show_stack_outputs "$STACK_NAME" "$REGION"
fi

finalize_script $DEPLOY_RESULT "¡DESPLIEGUE COMPLETADO CON ÉXITO!" "ERROR EN EL DESPLIEGUE"
exit $DEPLOY_RESULT
