#!/bin/bash
set -e

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

STACK_NAME=$1
REGION=$2
ENV=$3

# Validar argumentos requeridos
validate_required_var "STACK_NAME" "$STACK_NAME" || exit 1
validate_required_var "REGION" "$REGION" || exit 1
validate_required_var "ENV" "$ENV" || exit 1

# Mostrar información del script
show_script_info "ELIMINACIÓN DE STACK" "Eliminar recursos de CloudFormation" \
    "Stack" "$STACK_NAME" \
    "Región" "$REGION" \
    "Ambiente" "$ENV"

section_header "ELIMINANDO STACK"
info_message "Iniciando eliminación del stack: ${STACK_NAME} en la región ${REGION}..."

# Iniciar la eliminación del stack
warning_message "Solicitando eliminación del stack..."
aws cloudformation delete-stack \
    --stack-name ${STACK_NAME} \
    --region $REGION

success_message "Solicitud de eliminación enviada correctamente"
info_message "Esperando a que el stack sea eliminado completamente..."

# Mostrar una barra de progreso mientras se espera la eliminación
counter=0
spinner=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
start_time=$(date +%s)

while true; do
    STATUS=$(check_stack_status ${STACK_NAME} ${REGION})

    if [ "$STATUS" = "STACK_NOT_FOUND" ]; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo -e "\r${GREEN}${CHECK} Stack eliminado completamente                                   ${NC}"
        success_message "Tiempo total de eliminación: ${duration} segundos"
        break
    elif [[ "$STATUS" == *"DELETE_IN_PROGRESS"* ]]; then
        idx=$(($counter % ${#spinner[@]}))
        echo -ne "\r${YELLOW}${spinner[$idx]} Eliminando stack... Estado: ${STATUS} ($(date +%H:%M:%S))${NC}"
        counter=$((counter + 1))
        sleep 2
    elif [[ "$STATUS" == *"DELETE_FAILED"* ]]; then
        echo -e "\r${RED}${CROSS} Error al eliminar el stack. Estado final: ${STATUS}            ${NC}"
        error_message "Revise el panel de CloudFormation para más detalles"
        finalize_script 1 "" "ELIMINACIÓN FALLIDA"
        exit 1
    else
        echo -ne "\r${YELLOW}${spinner[$idx]} Estado del stack: ${STATUS} ($(date +%H:%M:%S))${NC}"
        counter=$((counter + 1))
        sleep 2
    fi
done

# Verificar si existen archivos temporales en la raíz del proyecto y eliminarlos
section_header "LIMPIEZA DE ARCHIVOS TEMPORALES"
info_message "Limpiando archivos temporales..."
TEMP_FILES=(
    "packaged-${ENV}.yaml"
    "cloudformation/parameters/${ENV}.json"
)

cleanup_temp_files "${TEMP_FILES[@]}"

# Finalizar el script
finalize_script 0 "LIMPIEZA COMPLETADA EXITOSAMENTE" ""
echo -e "${GREEN}${CHECK} El stack ${STACK_NAME} ha sido eliminado de la región ${REGION}${NC}"
exit 0
