include .env

.PHONY: validate deploy clean upload upload-nested-stacks prepare test logs download package discover-gtfs process-gtfs execute-gtfs monitor-gtfs report-gtfs batch-gtfs reset-failed help statemachine-register statemachine-start statemachine-summary statemachine-reset

validate:
	./scripts/deploy/validate.sh $(REGION)

upload:
	@echo "Subiendo scripts a S3..."
	./scripts/deploy/upload.sh $(LOCAL_SCRIPTS_PATH)/macro_generator/glue_script.py $(S3_BUCKET) scripts/glue/macro_generator/glue_script.py
	./scripts/deploy/upload.sh $(LOCAL_SCRIPTS_PATH)/macro_stops_generator/glue_script.py $(S3_BUCKET) scripts/glue/macro_stops_generator/glue_script.py

upload-nested-stacks:
	./scripts/deploy/upload_nested_stacks.sh $(ARTIFACTORY_BUCKET) $(REGION)

package:
	./scripts/deploy/package.sh $(S3_BUCKET) $(REGION) $(ENV)

deploy: upload package upload-nested-stacks
	./scripts/deploy/deploy.sh $(STACK_NAME) $(REGION) $(ENV) $(S3_BUCKET) $(GLUE_SCRIPTS_PATH)

clean:
	./scripts/deploy/clean.sh $(STACK_NAME) $(REGION) $(ENV)

prepare:
	./scripts/deploy/prepare.sh $(STACK_NAME) $(S3_BUCKET)

test: prepare
	./scripts/deploy/test.sh $(STACK_NAME) $(REGION) $(ENV)

logs:
	./scripts/deploy/logs.sh $(STACK_NAME) $(REGION)

download:
	./scripts/deploy/download.sh $(STACK_NAME) $(REGION) $(ENV) $(S3_BUCKET)

# Comandos para procesamiento GTFS por lotes
LAMBDA_NAME ?= $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query "Stacks[0].Outputs[?OutputKey=='GTFSPreprocessorFunction'].OutputValue" --output text 2>/dev/null || echo "GTFSPreprocessor-$(ENV)")
MACRO_JOB ?= $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query "Stacks[0].Outputs[?OutputKey=='MacroGeneratorJob'].OutputValue" --output text 2>/dev/null || echo "MacroGenerator-$(ENV)")
MACRO_STOPS_JOB ?= $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query "Stacks[0].Outputs[?OutputKey=='MacroStopsGeneratorJob'].OutputValue" --output text 2>/dev/null || echo "MacroStopsGenerator-$(ENV)")
STATE_TABLE ?= $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query "Stacks[0].Outputs[?OutputKey=='ProcessingStateTable'].OutputValue" --output text 2>/dev/null || echo "GTFSProcessingState-$(ENV)")
STATE_MACHINE_ARN ?= $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query "Stacks[0].Outputs[?OutputKey=='StateMachine'].OutputValue" --output text 2>/dev/null || echo "")
BATCH_SIZE ?= 5
MAX_START ?= 1
MAX_MONITOR_CHECKS ?= 20

# Descubrir combinaciones de datos GTFS
discover-gtfs:
	@echo "Descubriendo combinaciones de datos GTFS en $(S3_BUCKET)..."
	./scripts/batch/discover.sh $(S3_BUCKET) $(REGION) $(ENV)

# Preprocesar combinaciones pendientes
process-gtfs:
	@echo "Preprocesando combinaciones de datos GTFS..."
	./scripts/batch/process.sh $(LAMBDA_NAME) $(REGION) $(BATCH_SIZE)

# Ejecutar jobs de macro y macro_stops
execute-gtfs:
	@echo "Ejecutando jobs de macro y macro_stops..."
	./scripts/batch/execute.sh $(MACRO_JOB) $(MACRO_STOPS_JOB) $(S3_BUCKET) $(REGION) $(BATCH_SIZE)

# Monitorear jobs en ejecución
monitor-gtfs:
	@echo "Monitoreando jobs en ejecución..."
	./scripts/batch/monitor.sh $(REGION) $(MAX_MONITOR_CHECKS)

# Generar informes de procesamiento
report-gtfs:
	@echo "Generando informes de procesamiento..."
	./scripts/batch/report.sh

# Reiniciar combinaciones fallidas
reset-failed:
	@echo "Reiniciando combinaciones fallidas..."
	@jq '.combinations |= map(if .status == "failed" then .status = "pending" | del(.message) | del(.error) else . end)' batch_processing/status.json > batch_processing/status.json.tmp
	@mv batch_processing/status.json.tmp batch_processing/status.json
	@echo "Combinaciones fallidas restablecidas a pendientes"

# Ejecutar todo el proceso batch
batch-gtfs:
	@echo "Ejecutando todo el proceso batch de GTFS..."
	./scripts/batch/batch_all.sh $(S3_BUCKET) $(LAMBDA_NAME) $(MACRO_JOB) $(MACRO_STOPS_JOB) $(REGION) $(BATCH_SIZE) $(MAX_MONITOR_CHECKS)

# Comandos para procesamiento con máquina de estados
statemachine-register:
	@echo "Registrando combinaciones en DynamoDB..."
	./scripts/batch/statemachine_process.sh register $(STATE_TABLE) $(STATE_MACHINE_ARN) --bucket $(S3_BUCKET) --region $(REGION)

statemachine-start:
	@echo "Iniciando procesamiento con máquina de estados..."
	./scripts/batch/statemachine_process.sh start $(STATE_TABLE) $(STATE_MACHINE_ARN) --bucket $(S3_BUCKET) --region $(REGION) --max-start $(MAX_START)

statemachine-summary:
	@echo "Obteniendo resumen de procesamiento..."
	./scripts/batch/statemachine_process.sh summary $(STATE_TABLE) $(STATE_MACHINE_ARN) --region $(REGION)

statemachine-reset:
	@echo "Restableciendo combinaciones fallidas..."
	./scripts/batch/statemachine_process.sh reset $(STATE_TABLE) $(STATE_MACHINE_ARN) --region $(REGION)

# Ayuda sobre comandos disponibles
help:
	@echo "Comandos disponibles:"
	@echo "  make validate                - Validar template CloudFormation"
	@echo "  make deploy                  - Desplegar stack en AWS"
	@echo "  make test                    - Ejecutar job Glue"
	@echo "  make logs                    - Ver logs de ejecución"
	@echo "  make download                - Descargar resultados"
	@echo "  make clean                   - Limpiar recursos"
	@echo "  make package                 - Empaquetar y subir todas las funciones Lambda y sus capas"
	@echo "  make upload-nested-stacks    - Subir templates de nested stacks a S3"
	@echo ""
	@echo "Comandos para procesamiento GTFS por lotes:"
	@echo "  make discover-gtfs           - Descubrir combinaciones de datos GTFS"
	@echo "  make process-gtfs            - Preprocesar combinaciones pendientes"
	@echo "  make execute-gtfs            - Ejecutar jobs de macro y macro_stops"
	@echo "  make monitor-gtfs            - Monitorear jobs en ejecución"
	@echo "  make report-gtfs             - Generar informes de procesamiento"
	@echo "  make reset-failed            - Reiniciar combinaciones fallidas"
	@echo "  make batch-gtfs              - Ejecutar todo el proceso batch"
	@echo ""
	@echo "Comandos para procesamiento con máquina de estados:"
	@echo "  make statemachine-register   - Registrar combinaciones en DynamoDB"
	@echo "  make statemachine-start      - Iniciar procesamiento con máquina de estados"
	@echo "  make statemachine-summary    - Obtener resumen de procesamiento"
	@echo "  make statemachine-reset      - Restablecer combinaciones fallidas"
	@echo ""
	@echo "Variables configurables:"
	@echo "  BATCH_SIZE=10                - Cambiar tamaño de lote (default: 5)"
	@echo "  MAX_START=5                  - Máximo de ejecuciones a iniciar (default: 1)"
	@echo "  MAX_MONITOR_CHECKS=30        - Cambiar máximo de verificaciones (default: 20)"
	@echo "  STATE_TABLE=MiTabla          - Especificar nombre de tabla DynamoDB"
	@echo "  STATE_MACHINE_ARN=arn:aws... - Especificar ARN de máquina de estados"