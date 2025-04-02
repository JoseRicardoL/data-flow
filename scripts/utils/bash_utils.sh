#!/bin/bash
# Utilidades para el procesamiento por lotes de GTFS
# Este archivo contiene funciones compartidas por los scripts de procesamiento

# Importar utilidades de formato
source "$(dirname "$0")/format.sh"

# Directorio para archivos de estado y progreso
BATCH_DIR="batch_processing"
COMBINATIONS_FILE="${BATCH_DIR}/combinations.json"
STATUS_FILE="${BATCH_DIR}/status.json"
LOGS_DIR="${BATCH_DIR}/logs"

# Función para inicializar el entorno de procesamiento
init_batch_environment() {
    mkdir -p ${BATCH_DIR}
    mkdir -p ${LOGS_DIR}

    # Registrar metadatos iniciales si el archivo de estado no existe
    if [ ! -f "${STATUS_FILE}" ]; then
        echo '{
            "started_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
            "total": 0,
            "pending": 0,
            "preprocessing": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "combinations": []
        }' > ${STATUS_FILE}
    fi
}

# Función para actualizar el archivo de estado
update_status() {
    local total=$(jq '.combinations | length' ${STATUS_FILE})
    local pending=$(jq '.combinations | map(select(.status == "pending")) | length' ${STATUS_FILE})
    local preprocessing=$(jq '.combinations | map(select(.status == "preprocessing")) | length' ${STATUS_FILE})
    local processing=$(jq '.combinations | map(select(.status == "processing")) | length' ${STATUS_FILE})
    local completed=$(jq '.combinations | map(select(.status == "completed")) | length' ${STATUS_FILE})
    local failed=$(jq '.combinations | map(select(.status == "failed")) | length' ${STATUS_FILE})
    
    # Actualizar contadores
    jq ".total = ${total} | 
        .pending = ${pending} | 
        .preprocessing = ${preprocessing} | 
        .processing = ${processing} | 
        .completed = ${completed} | 
        .failed = ${failed} |
        .last_updated = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"" ${STATUS_FILE} > ${STATUS_FILE}.tmp
    
    mv ${STATUS_FILE}.tmp ${STATUS_FILE}
}

# Función para actualizar el estado de una combinación específica
update_combination_status() {
    local p_empresa=$1
    local p_contr=$2
    local p_version=$3
    local status=$4
    local message="${5:-}"
    
    # Crear un filtro para buscar la combinación específica
    local filter=".combinations |= map(
        if .P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\" then
            .status = \"${status}\" |
            .last_updated = \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\" |
            if \"${message}\" != \"\" then .message = \"${message}\" else . end
        else
            .
        end
    )"
    
    # Aplicar el filtro y guardar
    jq "${filter}" ${STATUS_FILE} > ${STATUS_FILE}.tmp
    mv ${STATUS_FILE}.tmp ${STATUS_FILE}
    
    # Actualizar contadores
    update_status
}

# Función para registrar una nueva combinación
register_combination() {
    local p_empresa=$1
    local p_contr=$2
    local p_version=$3
    
    # Verificar si ya existe
    local exists=$(jq ".combinations | map(select(.P_EMPRESA == \"${p_empresa}\" and .P_CONTR == \"${p_contr}\" and .P_VERSION == \"${p_version}\")) | length" ${STATUS_FILE})
    
    if [ "${exists}" -eq "0" ]; then
        # Crear nuevo objeto de combinación
        local new_combination="{
            \"P_EMPRESA\": \"${p_empresa}\",
            \"P_CONTR\": \"${p_contr}\",
            \"P_VERSION\": \"${p_version}\",
            \"status\": \"pending\",
            \"registered_at\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
        }"
        
        # Añadir a la lista de combinaciones
        jq ".combinations += [${new_combination}]" ${STATUS_FILE} > ${STATUS_FILE}.tmp
        mv ${STATUS_FILE}.tmp ${STATUS_FILE}
        
        # Actualizar contadores
        update_status
        return 0
    else
        # Ya existe, no hacer nada
        return 1
    fi
}

# Función para obtener las combinaciones pendientes
get_pending_combinations() {
    jq -c '.combinations | map(select(.status == "pending")) | .[]' ${STATUS_FILE}
}

# Función para obtener las combinaciones fallidas
get_failed_combinations() {
    jq -c '.combinations | map(select(.status == "failed")) | .[]' ${STATUS_FILE}
}

# Función para reiniciar las combinaciones fallidas
reset_failed_combinations() {
    # Cambiar el estado de las combinaciones fallidas a pendiente
    jq '.combinations |= map(
        if .status == "failed" then
            .status = "pending" |
            .retries = (if .retries then .retries + 1 else 1 end) |
            .last_updated = "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'" |
            del(.message) |
            del(.error)
        else
            .
        end
    )' ${STATUS_FILE} > ${STATUS_FILE}.tmp
    
    mv ${STATUS_FILE}.tmp ${STATUS_FILE}
    
    # Actualizar contadores
    update_status
    
    # Devolver el número de combinaciones restablecidas
    jq '.combinations | map(select(.retries > 0)) | length' ${STATUS_FILE}
}

# Función para formatear el payload para el preprocesador
format_preprocessor_payload() {
    local p_empresa=$1
    local p_contr=$2
    local p_version=$3
    
    echo "{
        \"statusCode\": 200,
        \"body\": \"{\\\"P_EMPRESA\\\": \\\"${p_empresa}\\\", \\\"P_VERSION\\\": \\\"${p_version}\\\", \\\"P_CONTR\\\": \\\"${p_contr}\\\"}\"
    }"
}

# Función para mostrar el resumen del estado actual
show_status_summary() {
    local total=$(jq '.total' ${STATUS_FILE})
    local pending=$(jq '.pending' ${STATUS_FILE})
    local preprocessing=$(jq '.preprocessing' ${STATUS_FILE})
    local processing=$(jq '.processing' ${STATUS_FILE})
    local completed=$(jq '.completed' ${STATUS_FILE})
    local failed=$(jq '.failed' ${STATUS_FILE})
    local started_at=$(jq -r '.started_at' ${STATUS_FILE})
    local last_updated=$(jq -r '.last_updated' ${STATUS_FILE})
    
    section_header "RESUMEN DE PROCESAMIENTO BATCH"
    
    echo -e "${CYAN}${BOLD}Estado al $(date)${NC}"
    echo -e "${BLUE}Inicio: ${started_at}${NC}"
    echo -e "${BLUE}Última actualización: ${last_updated}${NC}"
    echo ""
    echo -e "${CYAN}${BOLD}Progreso:${NC}"
    echo -e "${CYAN}Total combinaciones: ${total}${NC}"
    echo -e "${YELLOW}Pendientes: ${pending}${NC}"
    echo -e "${YELLOW}En preprocesamiento: ${preprocessing}${NC}"
    echo -e "${YELLOW}En procesamiento: ${processing}${NC}"
    echo -e "${GREEN}Completadas: ${completed}${NC}"
    echo -e "${RED}Fallidas: ${failed}${NC}"
    
    # Calcular progreso porcentual
    if [ "${total}" -gt "0" ]; then
        local progress=$(( (completed * 100) / total ))
        echo -e "${CYAN}${BOLD}Completado: ${progress}%${NC}"
    fi
    
    # Mostrar últimas 5 combinaciones fallidas si hay alguna
    if [ "${failed}" -gt "0" ]; then
        echo ""
        echo -e "${RED}${BOLD}Últimas combinaciones fallidas:${NC}"
        jq -r '.combinations | map(select(.status == "failed")) | .[:5] | .[] | "  - E=\(.P_EMPRESA), C=\(.P_CONTR), V=\(.P_VERSION): \(.message // "Sin mensaje")"' ${STATUS_FILE}
        
        if [ "${failed}" -gt "5" ]; then
            echo -e "${RED}  ... y $(( failed - 5 )) más${NC}"
        fi
    fi
}

# Función para verificar dependencias
check_dependencies() {
    # Verificar jq
    if ! command -v jq &> /dev/null; then
        error_message "jq no está instalado. Por favor instálelo con: apt-get install jq o brew install jq"
        return 1
    fi
    
    # Verificar aws-cli
    if ! command -v aws &> /dev/null; then
        error_message "AWS CLI no está instalado. Por favor instálelo siguiendo las instrucciones en: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        return 1
    fi
    
    # Verificar que se puede conectar a AWS
    if ! aws sts get-caller-identity &> /dev/null; then
        error_message "No se puede conectar a AWS. Verifique sus credenciales y configuración."
        return 1
    fi
    
    return 0
}