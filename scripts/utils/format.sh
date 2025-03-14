#!/bin/bash

# Colores y símbolos
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export RED='\033[0;31m'
export CYAN='\033[0;36m'
export BOLD='\033[1m'
export NC='\033[0m'
export CHECK="✓"
export CROSS="✗"
export ARROW="→"
export STAR="★"

# Función para mostrar cabecera de sección
function section_header() {
    echo -e "\n${BOLD}${BLUE}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}   $1${NC}"
    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════════════════${NC}\n"
}

# Función para mostrar resultado de una acción
function show_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}${CHECK} $2${NC}"
    else
        echo -e "${RED}${CROSS} $3${NC}"
        return $1
    fi
}

# Función para mostrar un mensaje de información
function info_message() {
    echo -e "${BLUE}${ARROW} $1${NC}"
}

# Función para mostrar un mensaje de éxito
function success_message() {
    echo -e "${GREEN}${CHECK} $1${NC}"
}

# Función para mostrar un mensaje de error
function error_message() {
    echo -e "${RED}${CROSS} $1${NC}"
}

# Función para mostrar un mensaje de advertencia
function warning_message() {
    echo -e "${YELLOW}${ARROW} $1${NC}"
}

# Función para mostrar un mensaje destacado
function highlight_message() {
    echo -e "${CYAN}${STAR} ${BOLD}$1${NC}"
}

# Función para mostrar un spinner durante procesos largos
function show_spinner() {
    local pid=$1
    local message=$2
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    local delay=0.1

    while ps -p $pid >/dev/null; do
        for i in $(seq 0 9); do
            echo -ne "\r${YELLOW}${spinstr:$i:1}${NC} $message"
            sleep $delay
        done
    done
    echo -ne "\r${YELLOW}   ${NC} $message"
    echo
}

# Función para formatear la salida JSON
function format_json_output() {
    local json_data=$1
    if [ ! -z "$json_data" ] && [ "$json_data" != "null" ]; then
        echo $json_data | jq -r '.[] | "   - \(.OutputKey): \(.OutputValue)"' 2>/dev/null || echo $json_data
    fi
}

# Función para mostrar un contador de espera con progreso
function wait_with_progress() {
    local check_command=$1
    local success_status=$2
    local message=$3

    spinner=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
    counter=0
    status=$($check_command)

    while [[ "$status" != "$success_status" && "$status" != *"FAILED"* && "$status" != *"ROLLBACK"* ]]; do
        idx=$(($counter % ${#spinner[@]}))
        echo -ne "\r${YELLOW}${spinner[$idx]} $message Status: ${status} ($(date +%H:%M:%S))${NC}"
        sleep 5
        status=$($check_command)
        counter=$((counter + 1))
    done

    if [[ "$status" == "$success_status" ]]; then
        echo -e "\r${GREEN}${CHECK} Operación completada: ${status}                   ${NC}"
        return 0
    else
        echo -e "\r${RED}${CROSS} Operación fallida: ${status}                   ${NC}"
        return 1
    fi
}

# Función para verificar el estado de un stack de CloudFormation
function check_stack_status() {
    aws cloudformation describe-stacks \
        --stack-name $1 \
        --region $2 \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "STACK_NOT_FOUND"
}

# Función para limpiar archivos temporales
function cleanup_temp_files() {
    for file in "$@"; do
        if [ -f "$file" ]; then
            rm "$file"
            success_message "Eliminado: $file"
        fi
    done
}

# Función para mostrar los outputs de un stack
function show_stack_outputs() {
    local stack_name=$1
    local region=$2

    info_message "Obteniendo detalles del stack..."

    # Obteniendo outputs del stack
    local outputs=$(aws cloudformation describe-stacks \
        --stack-name ${stack_name} \
        --region ${region} \
        --query 'Stacks[0].Outputs' \
        --output json 2>/dev/null)

    if [ ! -z "$outputs" ] && [ "$outputs" != "null" ]; then
        echo -e "\n${CYAN}${STAR} ${BOLD}Outputs del Stack:${NC}"
        format_json_output "$outputs"
    fi

    echo -e "\n${BLUE}${ARROW} Para verificar el stack en la consola AWS:${NC}"
    echo -e "${CYAN}   https://${region}.console.aws.amazon.com/cloudformation/home?region=${region}#/stacks/stackinfo?stackId=${stack_name}${NC}\n"
}

# Función para mostrar una barra de progreso
function progress_bar() {
    local current=$1
    local total=$2
    local title=$3
    local width=50
    local percentage=$((current * 100 / total))
    local completed=$((width * current / total))
    local remaining=$((width - completed))

    # Construir la barra
    local bar="["
    for ((i = 0; i < completed; i++)); do
        bar+="="
    done

    if [ $current -lt $total ]; then
        bar+=">"
        remaining=$((remaining - 1))
    fi

    for ((i = 0; i < remaining; i++)); do
        bar+=" "
    done
    bar+="]"

    echo -ne "${BLUE}${title}: ${YELLOW}${bar} ${percentage}%\r${NC}"

    if [ $current -eq $total ]; then
        echo -e "\n${GREEN}${CHECK} ${title}: Completado${NC}"
    fi
}

# Función para validar que una variable esté definida
function validate_required_var() {
    local var_name=$1
    local var_value=$2

    if [ -z "$var_value" ]; then
        error_message "Error: Variable $var_name no está definida"
        return 1
    fi
    return 0
}

# Función para mostrar información inicial del script
function show_script_info() {
    local script_name=$1
    local description=$2
    shift 2

    section_header "INICIANDO OPERACIÓN: $script_name"
    echo -e "${CYAN}${STAR} Descripción: ${BOLD}${description}${NC}"

    # Mostrar argumentos proporcionados
    local i=1
    while [ "$#" -gt 0 ]; do
        local arg_name=$1
        local arg_value=$2

        if [ ! -z "$arg_name" ] && [ ! -z "$arg_value" ]; then
            echo -e "${CYAN}${STAR} ${arg_name}: ${BOLD}${arg_value}${NC}"
        fi

        shift 2
        i=$((i + 1))
    done
    echo
}

# Función para finalizar el script con un mensaje de éxito o error
function finalize_script() {
    local status=$1
    local success_message=$2
    local error_message=$3

    section_header "RESULTADO FINAL"

    if [ $status -eq 0 ]; then
        echo -e "${GREEN}${CHECK} ${BOLD}${success_message}${NC}\n"
    else
        echo -e "${RED}${CROSS} ${BOLD}${error_message}${NC}"
        echo -e "${RED}Por favor revise los errores anteriores para obtener más detalles.${NC}\n"
    fi

    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════════════════${NC}"

    return $status
}
