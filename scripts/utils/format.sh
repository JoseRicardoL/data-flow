#!/bin/bash
# Utilidades de formato para scripts de shell
# Proporciona funciones para formatear la salida en consola con colores y estilos

# =====================================
# CONFIGURACIÓN
# =====================================

# Habilitar/deshabilitar colores (0=desactivado, 1=activado)
USE_COLORS=${USE_COLORS:-1}

# Ancho de terminal (autodetectado si es posible)
if command -v tput &>/dev/null && [ -t 1 ]; then
    TERM_WIDTH=$(tput cols 2>/dev/null || echo 80)
else
    TERM_WIDTH=80
fi

# Color automático según tipo de terminal
if [ $USE_COLORS -eq 1 ] && [ -t 1 ]; then
    # Colores básicos
    export BLACK='\033[0;30m'
    export RED='\033[0;31m'
    export GREEN='\033[0;32m'
    export YELLOW='\033[1;33m'
    export BLUE='\033[0;34m'
    export MAGENTA='\033[0;35m'
    export CYAN='\033[0;36m'
    export WHITE='\033[0;37m'
    
    # Estilos
    export BOLD='\033[1m'
    export DIM='\033[2m'
    export UNDERLINE='\033[4m'
    export BLINK='\033[5m'
    export REVERSE='\033[7m'
    export HIDDEN='\033[8m'
    
    # Reset
    export NC='\033[0m'
else
    # Sin colores
    export BLACK=''
    export RED=''
    export GREEN=''
    export YELLOW=''
    export BLUE=''
    export MAGENTA=''
    export CYAN=''
    export WHITE=''
    export BOLD=''
    export DIM=''
    export UNDERLINE=''
    export BLINK=''
    export REVERSE=''
    export HIDDEN=''
    export NC=''
fi

# Símbolos
export CHECK="✓"
export CROSS="✗"
export ARROW="→"
export STAR="★"

# =====================================
# FUNCIONES BÁSICAS DE MENSAJE
# =====================================

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

# Función para mostrar resultado de una acción
function show_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}${CHECK} $2${NC}"
    else
        echo -e "${RED}${CROSS} $3${NC}"
        return $1
    fi
}

# =====================================
# FUNCIONES DE SECCIÓN Y FORMATO
# =====================================

# Función para generar una línea de caracteres repetidos
function repeat_char() {
    local char="$1"
    local width="$2"
    local result=""
    
    for ((i=0; i<width; i++)); do
        result="${result}${char}"
    done
    
    echo "$result"
}

# Función para mostrar cabecera de sección
function section_header() {
    local title="$1"
    local width=${2:-$TERM_WIDTH}
    local char=${3:-"="}
    
    local line=$(repeat_char "$char" $width)
    
    echo -e "\n${BOLD}${BLUE}${line}${NC}"
    echo -e "${BOLD}${BLUE}   $title${NC}"
    echo -e "${BOLD}${BLUE}${line}${NC}\n"
}

# Función para mostrar una subsección
function subsection_header() {
    local title="$1"
    local width=${2:-$TERM_WIDTH}
    local char=${3:-"-"}
    
    local line=$(repeat_char "$char" $((width/4)))
    
    echo -e "${CYAN}${line} ${BOLD}${title}${NC} ${CYAN}${line}${NC}"
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

    local line=$(repeat_char "=" $TERM_WIDTH)
    echo -e "${BOLD}${BLUE}${line}${NC}"

    return $status
}

# =====================================
# BARRAS DE PROGRESO Y MONITOREO
# =====================================

# Función para mostrar una barra de progreso
function progress_bar() {
    local current=$1
    local total=$2
    local title=$3
    local width=${4:-50}
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

# Spinner para procesos largos
function show_spinner() {
    local pid=$1
    local message=$2
    local spinchars='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    local i=0
    
    while kill -0 $pid 2>/dev/null; do
        i=$(( (i+1) % ${#spinchars} ))
        printf "\r${YELLOW}%s${NC} %s" "${spinchars:$i:1}" "$message"
        sleep 0.1
    done
    printf "\r${YELLOW}  ${NC} %s\n" "$message"
}

# =====================================
# UTILIDADES AWS
# =====================================

# Función para verificar el estado de un stack de CloudFormation
function check_stack_status() {
    aws cloudformation describe-stacks \
        --stack-name $1 \
        --region $2 \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "STACK_NOT_FOUND"
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

# =====================================
# UTILIDADES VARIAS
# =====================================

# Función para formatear la salida JSON
function format_json_output() {
    local json_data=$1
    if [ ! -z "$json_data" ] && [ "$json_data" != "null" ]; then
        echo $json_data | jq -r '.[] | "   - \(.OutputKey): \(.OutputValue)"' 2>/dev/null || echo $json_data
    fi
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

# Función para comprobar si un comando existe
function check_command() {
    local cmd=$1
    local msg=${2:-"El comando $cmd no está instalado. Por favor instálalo para continuar."}
    
    if ! command -v $cmd &>/dev/null; then
        error_message "$msg"
        return 1
    fi
    return 0
}

# Función para comparar versiones correctamente
function version_compare() {
    # Extraer los números principales y menores para comparar correctamente las versiones
    local ver1_major=$(echo "$1" | cut -d. -f1)
    local ver1_minor=$(echo "$1" | cut -d. -f2)
    local ver2_major=$(echo "$2" | cut -d. -f1)
    local ver2_minor=$(echo "$2" | cut -d. -f2)
    
    # Comparar versión mayor primero
    if [ "$ver1_major" -lt "$ver2_major" ]; then
        return 0  # Verdadero, es menor
    elif [ "$ver1_major" -gt "$ver2_major" ]; then
        return 1  # Falso, no es menor
    else
        # Si las versiones mayores son iguales, comparar las menores
        if [ "$ver1_minor" -lt "$ver2_minor" ]; then
            return 0  # Verdadero, es menor
        else
            return 1  # Falso, no es menor
        fi
    fi
}