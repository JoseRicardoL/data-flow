#!/bin/bash
# Script para generar reportes del procesamiento por lotes

# Cargar utilidades
source "$(dirname "$0")/../utils/format.sh"
source "$(dirname "$0")/../utils/batch_utils.sh"

section_header "REPORTE DE PROCESAMIENTO BATCH"

# Verificar dependencias
check_dependencies
if [ $? -ne 0 ]; then
    exit 1
fi

# Inicializar entorno
init_batch_environment

# Verificar si hay datos de procesamiento
if [ ! -f "${STATUS_FILE}" ]; then
    error_message "No hay datos de procesamiento disponibles"
    exit 1
fi

# Generar informe resumido
generate_summary_report() {
    local output_file="${BATCH_DIR}/summary_report.txt"
    local started_at=$(jq -r '.started_at' ${STATUS_FILE})
    local last_updated=$(jq -r '.last_updated' ${STATUS_FILE})
    local total=$(jq '.total' ${STATUS_FILE})
    local pending=$(jq '.pending' ${STATUS_FILE})
    local preprocessing=$(jq '.preprocessing' ${STATUS_FILE})
    local processing=$(jq '.processing' ${STATUS_FILE})
    local completed=$(jq '.completed' ${STATUS_FILE})
    local failed=$(jq '.failed' ${STATUS_FILE})
    
    echo "====================================================" > ${output_file}
    echo "  INFORME DE PROCESAMIENTO BATCH GTFS" >> ${output_file}
    echo "====================================================" >> ${output_file}
    echo "" >> ${output_file}
    echo "Generado: $(date)" >> ${output_file}
    echo "Inicio del procesamiento: ${started_at}" >> ${output_file}
    echo "Última actualización: ${last_updated}" >> ${output_file}
    echo "" >> ${output_file}
    echo "RESUMEN:" >> ${output_file}
    echo "- Total de combinaciones: ${total}" >> ${output_file}
    echo "- Completadas exitosamente: ${completed}" >> ${output_file}
    echo "- Fallidas: ${failed}" >> ${output_file}
    echo "- En procesamiento: ${processing}" >> ${output_file}
    echo "- En preprocesamiento: ${preprocessing}" >> ${output_file}
    echo "- Pendientes: ${pending}" >> ${output_file}
    
    if [ "${total}" -gt 0 ]; then
        local progress=$(( (completed * 100) / total ))
        echo "- Progreso general: ${progress}%" >> ${output_file}
    fi
    
    echo "" >> ${output_file}
    echo "DETALLES POR EXPLOTACIÓN:" >> ${output_file}
    
    # Generar estadísticas por explotación
    local explotations=$(jq -r '.combinations | map(.P_EMPRESA) | unique | .[]' ${STATUS_FILE})
    
    for exp in ${explotations}; do
        local exp_total=$(jq ".combinations | map(select(.P_EMPRESA == \"${exp}\")) | length" ${STATUS_FILE})
        local exp_completed=$(jq ".combinations | map(select(.P_EMPRESA == \"${exp}\" and .status == \"completed\")) | length" ${STATUS_FILE})
        local exp_failed=$(jq ".combinations | map(select(.P_EMPRESA == \"${exp}\" and .status == \"failed\")) | length" ${STATUS_FILE})
        local exp_progress=0
        
        if [ "${exp_total}" -gt 0 ]; then
            exp_progress=$(( (exp_completed * 100) / exp_total ))
        fi
        
        echo "Explotación ${exp}:" >> ${output_file}
        echo "  - Total: ${exp_total}" >> ${output_file}
        echo "  - Completadas: ${exp_completed}" >> ${output_file}
        echo "  - Fallidas: ${exp_failed}" >> ${output_file}
        echo "  - Progreso: ${exp_progress}%" >> ${output_file}
        echo "" >> ${output_file}
    done
    
    echo "====================================================" >> ${output_file}
    echo "ÚLTIMAS 10 COMBINACIONES FALLIDAS:" >> ${output_file}
    echo "====================================================" >> ${output_file}
    
    jq -r '.combinations | map(select(.status == "failed")) | .[:10] | .[] | 
        "E=\(.P_EMPRESA), C=\(.P_CONTR), V=\(.P_VERSION)\n  Error: \(.message // .error // "Desconocido")"' \
        ${STATUS_FILE} >> ${output_file}
    
    success_message "Informe resumido generado: ${output_file}"
    return 0
}

# Generar informe detallado
generate_detailed_report() {
    local output_file="${BATCH_DIR}/detailed_report.json"
    
    # Copiar el archivo de estado pero añadiendo información adicional
    jq '{
        report_title: "Informe Detallado de Procesamiento GTFS",
        generated_at: "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
        started_at: .started_at,
        last_updated: .last_updated,
        total: .total,
        completed: .completed,
        failed: .failed,
        pending: .pending,
        preprocessing: .preprocessing,
        processing: .processing,
        progress_percentage: (if .total > 0 then (.completed * 100 / .total) else 0 end),
        combinations: .combinations | sort_by(.P_EMPRESA, .P_CONTR, .P_VERSION)
    }' ${STATUS_FILE} > ${output_file}
    
    success_message "Informe detallado generado: ${output_file}"
    return 0
}

# Generar informe de errores
generate_error_report() {
    local output_file="${BATCH_DIR}/error_report.txt"
    
    echo "====================================================" > ${output_file}
    echo "  INFORME DE ERRORES - PROCESAMIENTO BATCH GTFS" >> ${output_file}
    echo "====================================================" >> ${output_file}
    echo "" >> ${output_file}
    echo "Generado: $(date)" >> ${output_file}
    echo "" >> ${output_file}
    
    # Agrupar errores por tipo/mensaje
    echo "ERRORES AGRUPADOS POR TIPO:" >> ${output_file}
    
    # Extraer mensajes de error únicos
    local error_types=$(jq -r '.combinations | map(select(.status == "failed")) | map(.message // .error // "Desconocido") | unique | .[]' ${STATUS_FILE})
    
    for error_type in ${error_types}; do
        # Escapar el mensaje de error para usarlo en el filtro jq
        local escaped_error=$(echo "${error_type}" | sed 's/"/\\"/g')
        
        # Contar combinaciones con este error
        local count=$(jq ".combinations | map(select(.status == \"failed\" and (.message == \"${escaped_error}\" or .error == \"${escaped_error}\"))) | length" ${STATUS_FILE})
        
        echo "Tipo de error: ${error_type}" >> ${output_file}
        echo "  Ocurrencias: ${count}" >> ${output_file}
        echo "  Combinaciones afectadas:" >> ${output_file}
        
        # Listar las combinaciones afectadas
        jq -r ".combinations | map(select(.status == \"failed\" and (.message == \"${escaped_error}\" or .error == \"${escaped_error}\"))) | .[] | 
            \"    - E=\(.P_EMPRESA), C=\(.P_CONTR), V=\(.P_VERSION)\"" \
            ${STATUS_FILE} >> ${output_file}
        
        echo "" >> ${output_file}
    done
    
    success_message "Informe de errores generado: ${output_file}"
    return 0
}

# Generar todos los informes
generate_summary_report
generate_detailed_report
generate_error_report

# Mostrar resumen actual
show_status_summary

success_message "Todos los informes generados en el directorio ${BATCH_DIR}"
exit 0