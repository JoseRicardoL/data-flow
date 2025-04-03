#!/bin/bash
# Script para procesar combinaciones GTFS usando la máquina de estados
# Versión compatible con las funciones básicas de format.sh

# Cargar utilidades de formato
source "$(dirname "$0")/../utils/format.sh"

# Función para mostrar ayuda
show_help() {
    section_header "AYUDA: PROCESAMIENTO CON MÁQUINA DE ESTADOS"
    
    echo "Uso: $0 <operation> <state_table> <state_machine_arn> [opciones]"
    echo ""
    subsection_header "OPERACIONES"
    echo "  register      Registrar combinaciones en DynamoDB"
    echo "  start         Iniciar procesamiento con la máquina de estados"
    echo "  summary       Obtener resumen de procesamiento"
    echo "  reset         Restablecer combinaciones fallidas"
    echo ""
    subsection_header "OPCIONES"
    echo "  --bucket <bucket>          : Bucket S3 (requerido para register y start)"
    echo "  --combinations-file <file> : Archivo JSON de combinaciones (default: batch_processing/combinations.json)"
    echo "  --region <region>          : Región AWS (default: eu-west-1)"
    echo "  --max-start <num>          : Máximo de ejecuciones a iniciar (default: 1)"
    echo "  --format <json|table>      : Formato de salida para summary (default: json)"
    echo ""
    subsection_header "EJEMPLOS"
    info_message "Registrar combinaciones:"
    echo "  $0 register MyTable MyStateMachine --bucket my-bucket"
    info_message "Iniciar procesamiento:"
    echo "  $0 start MyTable MyStateMachine --bucket my-bucket --max-start 5"
    info_message "Ver resumen con formato de tabla:"
    echo "  $0 summary MyTable --format table"
    info_message "Restablecer combinaciones fallidas:"
    echo "  $0 reset MyTable"
    
    exit 1
}

# Función para validar los requisitos
validate_requirements() {
    local missing=0
    
    # Verificar comandos requeridos
    for cmd in python3 aws jq; do
        if ! check_command $cmd; then
            missing=1
        fi
    done
    
    # Verificar Python 3.6+
    if command -v python3 &> /dev/null; then
        py_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        if version_compare "$py_version" "3.6"; then
            error_message "Se requiere Python 3.6 o superior. Versión actual: $py_version"
            missing=1
        fi
    fi
    
    # Verificar credenciales AWS
    if ! aws sts get-caller-identity &>/dev/null; then
        error_message "No se puede acceder a AWS. Verifica tus credenciales."
        missing=1
    fi
    
    return $missing
}

# Verificar parámetros mínimos
if [ "$#" -lt 2 ]; then
    show_help
fi

# Obtener operación y argumentos principales
OPERATION="$1"
STATE_TABLE="$2"
STATE_MACHINE_ARN="$3"
shift 3

# Valores por defecto
BUCKET=""
COMBINATIONS_FILE="batch_processing/combinations.json"
REGION="eu-west-1"
MAX_START=1
FORMAT="json"

# Procesar opciones adicionales
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --combinations-file)
            COMBINATIONS_FILE="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --max-start)
            MAX_START="$2"
            shift 2
            ;;
        --format)
            FORMAT="$2"
            shift 2
            ;;
        --help)
            show_help
            ;;
        *)
            error_message "Opción desconocida: $1"
            show_help
            ;;
    esac
done

section_header "PROCESAMIENTO CON MÁQUINA DE ESTADOS"
info_message "Operación: ${OPERATION}"
info_message "Tabla DynamoDB: ${STATE_TABLE}"
info_message "Máquina de estados ARN: ${STATE_MACHINE_ARN}"
[ -n "$BUCKET" ] && info_message "Bucket S3: ${BUCKET}"
info_message "Archivo de combinaciones: ${COMBINATIONS_FILE}"
info_message "Región: ${REGION}"
[ "$OPERATION" = "start" ] && info_message "Máximo de ejecuciones a iniciar: ${MAX_START}"

# Validar requisitos
validate_requirements
if [ $? -ne 0 ]; then
    highlight_message "Faltan requisitos para continuar con la operación"
    exit 1
fi

# Verificar si las combinaciones existen antes de ejecutar
if [[ "$OPERATION" = "register" || "$OPERATION" = "start" ]]; then
    if [ -z "$BUCKET" ]; then
        error_message "El parámetro --bucket es requerido para la operación ${OPERATION}"
        exit 1
    fi
    
    if [ -z "$STATE_MACHINE_ARN" ]; then
        error_message "Se requiere especificar el ARN de la máquina de estados para ${OPERATION}"
        exit 1
    fi
    
    if [ ! -f "$COMBINATIONS_FILE" ]; then
        error_message "El archivo de combinaciones $COMBINATIONS_FILE no existe."
        info_message "Ejecuta primero 'make discover-gtfs' para generar combinaciones."
        exit 1
    fi
    
    # Verificar si hay combinaciones para procesar
    section_header "VERIFICANDO COMBINACIONES"
    info_message "Analizando archivo $COMBINATIONS_FILE..."
    
    # Extraer conteo usando jq 
    (jq '.combinations | length' "$COMBINATIONS_FILE" > /tmp/combo_count.tmp) &
    COUNTING_PID=$!
    show_spinner $COUNTING_PID "Analizando combinaciones disponibles..."
    
    # Obtener el resultado
    COMBINATIONS_COUNT=$(cat /tmp/combo_count.tmp 2>/dev/null || echo 0)
    rm -f /tmp/combo_count.tmp
    
    if [ "$COMBINATIONS_COUNT" -eq 0 ]; then
        warning_message "No hay combinaciones en el archivo $COMBINATIONS_FILE."
        info_message "Ejecuta 'make discover-gtfs' para descubrir nuevas combinaciones."
        exit 1
    else
        success_message "Se encontraron $COMBINATIONS_COUNT combinaciones para procesar."
    fi
fi

# Ejecutar el script Python según la operación
case "$OPERATION" in
    register)
        section_header "REGISTRANDO COMBINACIONES"
        info_message "Registrando combinaciones en la tabla DynamoDB..."
        
        # Ejecutar en background para mostrar spinner
        (python3 "$(dirname "$0")/register_combinations.py" register \
            --bucket "$BUCKET" \
            --state-table "$STATE_TABLE" \
            --state-machine-arn "$STATE_MACHINE_ARN" \
            --combinations-file "$COMBINATIONS_FILE" \
            --region "$REGION" > /tmp/register_output.log 2>&1) &
            
        PID=$!
        show_spinner $PID "Registrando combinaciones en DynamoDB..."
        
        # Verificar resultado
        wait $PID
        RESULT=$?
        
        if [ $RESULT -eq 0 ]; then
            # Mostrar resultados del registro
            subsection_header "RESULTADOS DEL REGISTRO"
            cat /tmp/register_output.log
            success_message "Registro completado exitosamente."
        else
            error_message "Error en el registro de combinaciones. Consulta el log para más detalles."
            cat /tmp/register_output.log
        fi
        
        # Limpiar archivos temporales
        rm -f /tmp/register_output.log
        ;;
        
    start)
        section_header "INICIANDO PROCESAMIENTO"
        info_message "Iniciando procesamiento de combinaciones..."
        info_message "Se iniciarán hasta $MAX_START ejecuciones"
        
        # Ejecutar en background para mostrar spinner
        (python3 "$(dirname "$0")/register_combinations.py" start \
            --bucket "$BUCKET" \
            --state-table "$STATE_TABLE" \
            --state-machine-arn "$STATE_MACHINE_ARN" \
            --combinations-file "$COMBINATIONS_FILE" \
            --region "$REGION" \
            --max-start "$MAX_START" > /tmp/start_output.log 2>&1) &
            
        PID=$!
        show_spinner $PID "Iniciando procesamiento de combinaciones..."
        
        # Verificar resultado
        wait $PID
        RESULT=$?
        
        if [ $RESULT -eq 0 ]; then
            # Contar ejecuciones iniciadas
            STARTED=$(grep -o "Se iniciaron [0-9]\+ ejecuciones" /tmp/start_output.log | awk '{print $3}')
            
            if [ -n "$STARTED" ] && [ "$STARTED" -gt 0 ]; then
                success_message "Se han iniciado $STARTED ejecuciones"
            else
                warning_message "No se iniciaron nuevas ejecuciones"
            fi
            
            subsection_header "SIGUIENTE PASO"
            info_message "Ejecuta 'make statemachine-summary' para monitorear el progreso."
        else
            error_message "Error al iniciar procesamiento. Consulta el log para más detalles."
            cat /tmp/start_output.log
        fi
        
        # Limpiar archivos temporales
        rm -f /tmp/start_output.log
        ;;
        
    summary)
        section_header "RESUMEN DE PROCESAMIENTO"
        info_message "Obteniendo resumen de procesamiento..."
        
        # Ejecutar en background para mostrar spinner
        (python3 "$(dirname "$0")/register_combinations.py" summary \
            --state-table "$STATE_TABLE" \
            --region "$REGION" > /tmp/summary_output.json 2>/tmp/summary_error.log) &
            
        PID=$!
        show_spinner $PID "Consultando estado de procesamiento..."
        
        # Verificar resultado
        wait $PID
        RESULT=$?
        
        if [ $RESULT -eq 0 ]; then
            if [ "$FORMAT" = "json" ]; then
                # Formato JSON con colores si jq está disponible
                if command -v jq &>/dev/null; then
                    jq -C '.' /tmp/summary_output.json
                else
                    # Sin colores si no hay jq
                    cat /tmp/summary_output.json
                fi
            else
                # Formato tabla usando estilo básico
                if [ -s "/tmp/summary_output.json" ]; then
                    SUMMARY_JSON=$(cat /tmp/summary_output.json)
                    
                    # Extraer información para la tabla
                    TOTAL=$(echo "$SUMMARY_JSON" | jq -r '.total // 0')
                    PENDING=$(echo "$SUMMARY_JSON" | jq -r '.by_status.pending // 0')
                    PROCESSING=$(echo "$SUMMARY_JSON" | jq -r '.by_status.processing // 0')
                    COMPLETED=$(echo "$SUMMARY_JSON" | jq -r '.by_status.completed // 0')
                    FAILED=$(echo "$SUMMARY_JSON" | jq -r '.by_status.failed // 0')
                    PERCENTAGE=$(echo "$SUMMARY_JSON" | jq -r '.completion_percentage // 0')
                    
                    # Mostrar resumen general
                    subsection_header "RESUMEN DE PROCESAMIENTO GTFS"
                    echo "Total de combinaciones: $TOTAL"
                    echo "Completadas: $COMPLETED"
                    echo "En procesamiento: $PROCESSING"
                    echo "Pendientes: $PENDING"
                    echo "Fallidas: $FAILED"
                    echo "Progreso: ${PERCENTAGE}%"
                    
                    # Mostrar barra de progreso visual
                    if [ $TOTAL -gt 0 ]; then
                        subsection_header "PROGRESO VISUAL"
                        progress_bar $COMPLETED $TOTAL "Procesamiento GTFS"
                        echo
                    fi
                    
                    # Mostrar últimas combinaciones fallidas si existen
                    if [ "$FAILED" -gt 0 ]; then
                        subsection_header "COMBINACIONES FALLIDAS"
                        FAILED_COMBOS=$(echo "$SUMMARY_JSON" | jq -r '.failed[] | "\(.id): \(.error // "Sin detalles del error")"')
                        
                        echo -e "${YELLOW}Últimas combinaciones fallidas:${NC}"
                        echo "$FAILED_COMBOS" | while read line; do
                            error_message "$line"
                        done
                        
                        subsection_header "RESOLUCIÓN"
                        info_message "Para reintentar combinaciones fallidas, ejecuta: make statemachine-reset"
                    fi
                    
                    # Mostrar estadísticas por empresa si hay datos
                    if [ $(echo "$SUMMARY_JSON" | jq -r '.by_enterprise | length') -gt 0 ]; then
                        subsection_header "ESTADÍSTICAS POR EMPRESA"
                        
                        # Procesar cada empresa
                        echo "$SUMMARY_JSON" | jq -r '.by_enterprise | to_entries[] | [.key, .value.total, .value.completed, .value.failed] | @tsv' |
                        while IFS=$'\t' read -r empresa total completadas fallidas; do
                            # Calcular progreso para cada empresa
                            if [ "$total" -gt 0 ]; then
                                progreso=$(( completadas * 100 / total ))
                                echo -e "${BOLD}Empresa $empresa${NC}: Total: $total, Completadas: $completadas, Fallidas: $fallidas, Progreso: ${progreso}%"
                            else
                                echo -e "${BOLD}Empresa $empresa${NC}: Total: $total, Completadas: $completadas, Fallidas: $fallidas, Progreso: 0%"
                            fi
                        done
                    fi
                else
                    error_message "No se pudo obtener el resumen"
                    if [ -s "/tmp/summary_error.log" ]; then
                        cat /tmp/summary_error.log
                    fi
                fi
            fi
        else
            error_message "Error al obtener el resumen"
            if [ -s "/tmp/summary_error.log" ]; then
                cat /tmp/summary_error.log
            fi
        fi
        
        # Limpiar archivos temporales
        rm -f /tmp/summary_output.json /tmp/summary_error.log
        ;;
        
    reset)
        section_header "RESETEO DE COMBINACIONES FALLIDAS"
        
        # Ejecutar en background para mostrar spinner
        (python3 "$(dirname "$0")/register_combinations.py" reset \
            --state-table "$STATE_TABLE" \
            --region "$REGION" > /tmp/reset_output.log 2>&1) &
            
        PID=$!
        show_spinner $PID "Restableciendo combinaciones fallidas..."
        
        # Verificar resultado
        wait $PID
        RESULT=$?
        
        if [ $RESULT -eq 0 ]; then
            # Extraer número de combinaciones restablecidas
            RESET_COUNT=$(grep -o "Se restablecieron [0-9]\+ combinaciones" /tmp/reset_output.log | awk '{print $3}')
            
            if [ -n "$RESET_COUNT" ] && [ "$RESET_COUNT" -gt 0 ]; then
                success_message "Se restablecieron ${RESET_COUNT} combinaciones fallidas a estado pendiente"
                
                subsection_header "SIGUIENTE PASO"
                info_message "Ejecuta 'make statemachine-start' para procesar las combinaciones restablecidas."
            else
                info_message "No había combinaciones fallidas para restablecer."
            fi
        else
            error_message "Error al restablecer combinaciones"
            cat /tmp/reset_output.log
        fi
        
        # Limpiar archivos temporales
        rm -f /tmp/reset_output.log
        ;;
        
    *)
        error_message "Operación desconocida: $OPERATION"
        show_help
        ;;
esac

# Mensaje final basado en el resultado
if [ ${RESULT:-0} -eq 0 ]; then
    finalize_script 0 "OPERACIÓN COMPLETADA EXITOSAMENTE" ""
else
    finalize_script 1 "" "ERROR AL EJECUTAR LA OPERACIÓN"
fi

exit ${RESULT:-0}