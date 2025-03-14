#!/bin/bash
set -e

source "$(dirname "$0")/../utils/format.sh"

section_header "DIAGNÓSTICO INTEGRAL DEL ENTORNO LOCAL AWS GLUE"

# 1. Verificar que Docker esté corriendo
info_message "Verificando estado del daemon Docker..."
if ! docker info >/dev/null 2>&1; then
    error_message "El daemon de Docker no está corriendo. Inícialo y vuelve a ejecutar este diagnóstico."
    exit 1
else
    success_message "Docker está corriendo."
fi

# 2. Mostrar versiones de Docker y Docker Compose
info_message "Versiones instaladas:"
docker version --format "Cliente: {{.Client.Version}}  Servidor: {{.Server.Version}}"
if docker compose version >/dev/null 2>&1; then
    docker compose version
else
    warning_message "Docker Compose no se encontró o no está configurado."
fi

# 3. Verificar existencia y estado del contenedor 'glue_local'
info_message "Buscando el contenedor 'glue_local'..."
CONTAINER_EXISTS=$(docker ps -a --filter "name=glue_local" --format "{{.Names}}")
if [ -z "$CONTAINER_EXISTS" ]; then
    warning_message "El contenedor 'glue_local' no existe."
else
    info_message "El contenedor 'glue_local' existe."
    if docker ps --filter "name=glue_local" --format "{{.Names}}" | grep -q "glue_local"; then
        success_message "El contenedor 'glue_local' está en ejecución."
    else
        warning_message "El contenedor 'glue_local' existe pero no está en ejecución."
        info_message "Últimas líneas de log:"
        docker logs --tail 30 glue_local
    fi
fi

# 4. Prueba de ejecución de comando dentro del contenedor
if docker ps --filter "name=glue_local" --format "{{.Names}}" | grep -q "glue_local"; then
    info_message "Ejecutando prueba simple dentro del contenedor..."
    if docker exec glue_local bash -c "echo 'Comando ejecutado correctamente'" >/dev/null 2>&1; then
        success_message "El contenedor ejecuta comandos correctamente."
    else
        error_message "No se pudo ejecutar comandos dentro del contenedor."
    fi
fi

# 5. Verificar disponibilidad de puertos (8888 para Jupyter, 4040 para Spark UI)
info_message "Verificando puertos en el host:"
for port in 8888 4040; do
    if command -v lsof >/dev/null 2>&1; then
        if lsof -i :"$port" >/dev/null 2>&1; then
            warning_message "El puerto $port está en uso. Verifica si es correcto o hay conflictos."
        else
            success_message "El puerto $port está disponible."
        fi
    else
        info_message "No se encontró 'lsof'; verifica manualmente el puerto $port."
    fi
done

# 6. Verificar que el directorio de trabajo exista dentro del contenedor
if docker ps --filter "name=glue_local" --format "{{.Names}}" | grep -q "glue_local"; then
    info_message "Verificando directorio /home/glue_user/workspace en el contenedor..."
    if docker exec glue_local ls -la /home/glue_user/workspace >/dev/null 2>&1; then
        success_message "El directorio /home/glue_user/workspace existe y es accesible."
    else
        error_message "El directorio /home/glue_user/workspace no existe o no es accesible dentro del contenedor."
    fi
fi

# 7. Prueba de conexión a Jupyter Lab (HTTP)
if command -v curl >/dev/null 2>&1; then
    info_message "Verificando respuesta HTTP en http://localhost:8888 ..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8888 || echo "000")
    if [ "$HTTP_CODE" == "200" ] || [ "$HTTP_CODE" == "302" ]; then
        success_message "Jupyter Lab responde correctamente (HTTP $HTTP_CODE)."
    else
        warning_message "Jupyter Lab no responde correctamente (HTTP $HTTP_CODE). Verifica que el contenedor esté corriendo y el servicio iniciado."
    fi
else
    info_message "curl no está instalado; omitiendo comprobación HTTP."
fi

# 8. Revisar logs en busca de errores críticos
if docker ps -a --filter "name=glue_local" --format "{{.Names}}" | grep -q "glue_local"; then
    info_message "Buscando errores críticos en los logs del contenedor..."
    ERROR_LINES=$(docker logs glue_local 2>&1 | grep -i -E "error|failed|exception" | tail -n 10 || true)
    if [ -n "$ERROR_LINES" ]; then
        warning_message "Se encontraron mensajes de error en los logs:"
        echo "$ERROR_LINES"
    else
        success_message "No se detectaron errores críticos en los logs."
    fi
fi

section_header "DIAGNÓSTICO FINALIZADO"
success_message "Diagnóstico completado. Revisa los mensajes anteriores para identificar posibles problemas."
info_message "Si algún paso ha fallado, considera revisar la configuración, permisos o logs detallados del contenedor."
