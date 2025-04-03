#!/usr/bin/env python3
"""
Script para registrar combinaciones GTFS en DynamoDB y gestionar su procesamiento.
Versión robusta optimizada para ejecuciones múltiples con limpieza de datos inconsistentes.
"""

import boto3
import json
import os
import argparse
import logging
import time
from datetime import datetime, timedelta
import uuid
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("register_combinations.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Constantes
MAX_SCAN_ITEMS = 100  # Número máximo de elementos por operación de escaneo DynamoDB
CLEANUP_HOURS_THRESHOLD = 8  # Horas después de las cuales un estado intermedio se considera inconsistente


def validate_table(dynamodb, table_name):
    """Valida que la tabla DynamoDB existe y está activa."""
    try:
        table = dynamodb.Table(table_name)
        response = table.meta.client.describe_table(TableName=table_name)
        table_status = response["Table"]["TableStatus"]
        
        if table_status != "ACTIVE":
            logger.warning(f"La tabla {table_name} existe pero no está activa (estado: {table_status})")
            return False
            
        logger.info(f"Tabla {table_name} verificada: ACTIVE")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(f"La tabla {table_name} no existe")
        else:
            logger.error(f"Error al verificar tabla {table_name}: {str(e)}")
        return False


def validate_combination(combo):
    """
    Valida rigurosamente que una combinación tenga todos los campos requeridos y tipos correctos.
    
    Args:
        combo: Diccionario con los datos de la combinación
        
    Returns:
        tuple: (bool, str) - Indica si la combinación es válida y un mensaje de error si no lo es
    """
    required_fields = ["P_EMPRESA", "P_CONTR", "P_VERSION"]
    
    # Verificar que todos los campos requeridos existan
    for field in required_fields:
        if field not in combo:
            return False, f"Campo requerido {field} no encontrado"
        
        # Validar que no esté vacío
        if not combo[field]:
            return False, f"Campo {field} está vacío"
            
        # Si es necesario, verificar tipo (aquí asumimos que todos son strings)
        if not isinstance(combo[field], str):
            combo[field] = str(combo[field])  # Convertir a string si no lo es
    
    return True, ""


def clean_inconsistent_data(state_table):
    """
    Busca y limpia datos inconsistentes en la tabla de estado.
    
    Args:
        state_table: Tabla DynamoDB de estado
    
    Returns:
        dict: Estadísticas de limpieza {deleted: X, reset: Y}
    """
    stats = {"deleted": 0, "reset": 0}
    last_evaluated_key = None
    
    logger.info("Iniciando limpieza de datos inconsistentes...")
    
    # Calculamos el umbral de tiempo para estados intermedios
    threshold_time = datetime.now() - timedelta(hours=CLEANUP_HOURS_THRESHOLD)
    threshold_time_str = threshold_time.isoformat()
    
    # Escaneamos toda la tabla con paginación
    while True:
        scan_kwargs = {
            "Limit": MAX_SCAN_ITEMS
        }
        
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
            
        response = state_table.scan(**scan_kwargs)
        items = response.get("Items", [])
        
        for item in items:
            item_id = item.get("id", "unknown")
            is_inconsistent = False
            inconsistency_reason = ""
            
            # 1. Verificar campos requeridos
            required_fields = ["P_EMPRESA", "P_CONTR", "P_VERSION", "status"]
            for field in required_fields:
                if field not in item or not item[field]:
                    is_inconsistent = True
                    inconsistency_reason = f"Falta campo {field}"
                    break
            
            # 2. Verificar estados intermedios bloqueados
            if not is_inconsistent and item.get("status") in ["preprocessing", "processing"]:
                # Si tiene timestamp de inicio y ha estado en ese estado por más tiempo del umbral
                if "started_at" in item:
                    try:
                        started_at = datetime.fromisoformat(item["started_at"].replace('Z', '+00:00'))
                        if started_at < threshold_time:
                            is_inconsistent = True
                            inconsistency_reason = f"Estado {item['status']} por más de {CLEANUP_HOURS_THRESHOLD}h"
                    except (ValueError, TypeError):
                        # Si el formato de fecha no es válido
                        is_inconsistent = True
                        inconsistency_reason = "Timestamp started_at inválido"
                else:
                    # Si no tiene timestamp de inicio, también es inconsistente
                    is_inconsistent = True
                    inconsistency_reason = f"Estado {item['status']} sin timestamp"
            
            # 3. Verificar estados desconocidos
            if not is_inconsistent and item.get("status") not in ["pending", "preprocessing", "processing", "completed", "failed"]:
                is_inconsistent = True
                inconsistency_reason = f"Estado desconocido: {item.get('status')}"
            
            # Limpiar registros inconsistentes
            if is_inconsistent:
                try:
                    # Determinar si eliminar o resetear
                    # Para mantener el historial, preferimos resetear en lugar de eliminar
                    if item.get("status") in ["preprocessing", "processing"]:
                        # Resetear a pendiente
                        state_table.update_item(
                            Key={"id": item_id},
                            UpdateExpression="SET #s = :pending, reset_at = :now, reset_reason = :reason, retries = if_not_exists(retries, :zero) + :one",
                            ExpressionAttributeNames={"#s": "status"},
                            ExpressionAttributeValues={
                                ":pending": "pending",
                                ":now": datetime.now().isoformat(),
                                ":reason": inconsistency_reason,
                                ":zero": 0,
                                ":one": 1
                            },
                        )
                        stats["reset"] += 1
                        logger.info(f"Restablecido registro inconsistente: {item_id} - Razón: {inconsistency_reason}")
                    else:
                        # Para otras inconsistencias graves, eliminar
                        state_table.delete_item(Key={"id": item_id})
                        stats["deleted"] += 1
                        logger.info(f"Eliminado registro inconsistente: {item_id} - Razón: {inconsistency_reason}")
                        
                except Exception as e:
                    logger.error(f"Error al limpiar registro inconsistente {item_id}: {str(e)}")
        
        # Verificar si hay más elementos que escanear
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    
    logger.info(f"Limpieza completada: {stats['deleted']} eliminados, {stats['reset']} restablecidos")
    return stats


def register_combination_atomic(state_table, combo):
    """
    Registra una combinación de manera atómica, asegurando que no haya duplicados
    ni condiciones de carrera.
    
    Args:
        state_table: Tabla DynamoDB de estado
        combo: Diccionario con datos de la combinación
    
    Returns:
        tuple: (str, str) - Estado de registro y mensaje
    """
    combo_id = f"{combo['P_EMPRESA']}_{combo['P_CONTR']}_{combo['P_VERSION']}"
    
    try:
        # Verificar primero si ya existe
        response = state_table.get_item(Key={"id": combo_id})
        
        if "Item" in response:
            # Ya existe, verificar su estado
            item = response["Item"]
            current_status = item.get("status", "unknown")
            
            # Si está en estado fallido, restablecer
            if current_status == "failed":
                state_table.update_item(
                    Key={"id": combo_id},
                    UpdateExpression="SET #s = :pending, reset_at = :now, retries = if_not_exists(retries, :zero) + :one, error = :null",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":pending": "pending",
                        ":now": datetime.now().isoformat(),
                        ":zero": 0,
                        ":one": 1,
                        ":null": None
                    },
                )
                return "reset", "Restablecido de fallido a pendiente"
            
            # Si está en un estado inconsistente (diferente de pending, processing, completed), restablecer
            elif current_status not in ["pending", "processing", "completed"]:
                state_table.update_item(
                    Key={"id": combo_id},
                    UpdateExpression="SET #s = :pending, reset_at = :now, retries = if_not_exists(retries, :zero) + :one",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":pending": "pending",
                        ":now": datetime.now().isoformat(),
                        ":zero": 0,
                        ":one": 1
                    },
                )
                return "reset", f"Restablecido de estado {current_status} a pendiente"
            
            # De lo contrario, dejar como está (ya sea pending, processing o completed)
            return "skipped", f"Ya existe con estado: {current_status}"
        
        # No existe, crear nuevo registro con ConditionExpression para seguridad adicional
        state_table.put_item(
            Item={
                "id": combo_id,
                "P_EMPRESA": combo["P_EMPRESA"],
                "P_CONTR": combo["P_CONTR"],
                "P_VERSION": combo["P_VERSION"],
                "status": "pending",
                "registered_at": datetime.now().isoformat(),
                "retries": 0
            },
            ConditionExpression="attribute_not_exists(id)"
        )
        return "registered", "Registrado exitosamente"
    
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Condición de carrera: otro proceso creó el registro entre nuestra verificación y creación
            # Volver a intentar una vez más
            try:
                response = state_table.get_item(Key={"id": combo_id})
                if "Item" in response:
                    return "skipped", "Creado por otro proceso concurrente"
                return "error", "Error de condición, pero registro no encontrado"
            except Exception as inner_e:
                return "error", f"Error al verificar después de condición fallida: {str(inner_e)}"
        return "error", f"Error al registrar: {str(e)}"
    except Exception as e:
        return "error", f"Error general: {str(e)}"


def log_registration_execution(bucket, registration_summary, combinations_file):
    """
    Guarda un log de la ejecución actual en S3.
    
    Args:
        bucket: Nombre del bucket S3
        registration_summary: Resumen de la operación
        combinations_file: Archivo de combinaciones procesado
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "summary": registration_summary,
        "combinations_file": combinations_file
    }
    
    # Guardar el log en S3
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=bucket,
            Key=f"GTFS_LOGS/registrations/registration_log_{timestamp}.json",
            Body=json.dumps(log_data, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Log de ejecución guardado en S3: registration_log_{timestamp}.json")
    except Exception as e:
        logger.error(f"Error al guardar log de registro: {str(e)}")


def register_combinations(
    bucket_name,
    state_table_name,
    state_machine_arn,
    combinations_file="batch_processing/combinations.json",
    region="eu-west-1",
    start_processing=False,
    max_to_start=1,
    clean_inconsistent=False,
):
    """
    Registra combinaciones en DynamoDB y opcionalmente inicia su procesamiento.
    Optimizado para múltiples ejecuciones seguras.
    """
    # Inicializar clientes AWS
    dynamodb = boto3.resource("dynamodb", region_name=region)
    step_functions = boto3.client("stepfunctions", region_name=region)

    # Validar tabla
    if not validate_table(dynamodb, state_table_name):
        logger.error(f"La tabla {state_table_name} no es válida para el registro")
        return False

    # Obtener tabla de DynamoDB
    state_table = dynamodb.Table(state_table_name)
    
    # Limpiar datos inconsistentes antes de empezar (si se solicita)
    if clean_inconsistent:
        cleanup_stats = clean_inconsistent_data(state_table)
        logger.info(f"Limpieza completada: {cleanup_stats['deleted']} eliminados, {cleanup_stats['reset']} restablecidos")

    # Cargar combinaciones desde el archivo JSON
    try:
        with open(combinations_file, "r") as f:
            data = json.load(f)
            combinations = data.get("combinations", [])

        if not combinations:
            logger.error(f"No se encontraron combinaciones en {combinations_file}")
            return False

        logger.info(f"Se cargaron {len(combinations)} combinaciones desde {combinations_file}")

    except Exception as e:
        logger.error(f"Error al cargar combinaciones: {str(e)}")
        return False

    # Contadores para estadísticas
    stats = {
        "registered": 0,  # Nuevos registros
        "reset": 0,       # Restablecidos de fallido a pendiente
        "skipped": 0,     # Ya existentes y no modificados
        "errors": 0       # Errores durante el registro
    }
    processed_combinations = []

    # Función para procesar una combinación individual
    def process_combination(combo):
        # Validar la combinación
        is_valid, error_msg = validate_combination(combo)
        if not is_valid:
            logger.error(f"Combinación inválida: {error_msg}")
            return "error", error_msg
            
        # Registrar combinación de forma atómica
        result, message = register_combination_atomic(state_table, combo)
        combo_id = f"{combo['P_EMPRESA']}_{combo['P_CONTR']}_{combo['P_VERSION']}"
        
        if result == "registered":
            logger.info(f"Combinación registrada: {combo_id}")
        elif result == "reset":
            logger.info(f"Combinación restablecida: {combo_id} - {message}")
        elif result == "skipped":
            logger.info(f"Combinación omitida: {combo_id} - {message}")
        else:
            logger.error(f"Error registrando combinación {combo_id}: {message}")
            
        return result, message

    # Procesar combinaciones en paralelo para mayor eficiencia
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_combo = {executor.submit(process_combination, combo): combo for combo in combinations}
        
        for future in as_completed(future_to_combo):
            combo = future_to_combo[future]
            combo_id = f"{combo['P_EMPRESA']}_{combo['P_CONTR']}_{combo['P_VERSION']}"
            
            try:
                result, message = future.result()
                processed_combinations.append({
                    "id": combo_id,
                    "result": result,
                    "message": message
                })
                
                # Actualizar estadísticas
                if result in stats:
                    stats[result] += 1
                else:
                    stats["errors"] += 1
            except Exception as e:
                logger.error(f"Error procesando combinación {combo_id}: {str(e)}")
                stats["errors"] += 1
                processed_combinations.append({
                    "id": combo_id,
                    "result": "error",
                    "message": str(e)
                })

    # Guardar log de la ejecución
    if bucket_name:
        log_registration_execution(bucket_name, stats, combinations_file)

    # Mostrar resumen
    logger.info(f"Registro completado: {stats['registered']} nuevas, {stats['reset']} restablecidas, "
                f"{stats['skipped']} existentes, {stats['errors']} errores")
    
    # Si se solicita iniciar procesamiento
    if start_processing and (stats["registered"] + stats["reset"]) > 0:
        started = trigger_processing(state_table, state_machine_arn, max_to_start)
        if started > 0:
            logger.info(f"Se iniciaron {started} ejecuciones")
        else:
            logger.warning("No se iniciaron nuevas ejecuciones")

    return stats["registered"] > 0 or stats["reset"] > 0


def trigger_processing(state_table, state_machine_arn, max_to_start=1):
    """
    Inicia el procesamiento de combinaciones pendientes.
    
    Args:
        state_table: Tabla DynamoDB de estado
        state_machine_arn: ARN de la máquina de estados
        max_to_start: Número máximo de ejecuciones a iniciar
        
    Returns:
        int: Número de ejecuciones iniciadas
    """
    # Cliente Step Functions
    step_functions = boto3.client("stepfunctions")
    started = 0
    
    # Función para obtener todas las combinaciones pendientes con paginación
    def get_all_pending_combinations(limit=MAX_SCAN_ITEMS):
        pending_combinations = []
        last_evaluated_key = None
        
        while True and len(pending_combinations) < limit:
            scan_kwargs = {
                "FilterExpression": "#s = :pending",
                "ExpressionAttributeNames": {"#s": "status"},
                "ExpressionAttributeValues": {":pending": "pending"},
                "Limit": min(MAX_SCAN_ITEMS, limit - len(pending_combinations))
            }
            
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
                
            response = state_table.scan(**scan_kwargs)
            pending_combinations.extend(response.get("Items", []))
            
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key or len(pending_combinations) >= limit:
                break
                
        return pending_combinations

    try:
        # Obtener combinaciones pendientes
        pending_combinations = get_all_pending_combinations(max_to_start)
        
        if not pending_combinations:
            logger.info("No hay combinaciones pendientes para procesar")
            return 0

        logger.info(f"Se encontraron {len(pending_combinations)} combinaciones pendientes")

        # Iniciar ejecuciones hasta el límite especificado
        for combo in pending_combinations:
            combo_id = combo["id"]

            # Iniciar ejecución de la máquina de estados
            execution_name = f"GTFSProcess-{combo_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            try:
                # Verificar que sigue en estado pendiente antes de iniciar
                response = state_table.get_item(Key={"id": combo_id})
                if "Item" not in response or response["Item"].get("status") != "pending":
                    logger.warning(f"La combinación {combo_id} ya no está en estado pendiente")
                    continue

                # Iniciar ejecución
                execution = step_functions.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=execution_name,
                    input=json.dumps(combo),
                )

                # Actualizar estado en DynamoDB (con condición para seguridad)
                try:
                    state_table.update_item(
                        Key={"id": combo_id},
                        UpdateExpression="SET #s = :processing, execution_arn = :arn, started_at = :t",
                        ConditionExpression="#s = :pending",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={
                            ":processing": "processing",
                            ":pending": "pending",
                            ":arn": execution["executionArn"],
                            ":t": datetime.now().isoformat(),
                        },
                    )
                    
                    started += 1
                    logger.info(f"Ejecución iniciada para {combo_id}: {execution['executionArn']}")
                    
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        # El estado ya cambió mientras iniciábamos la ejecución
                        logger.warning(f"No se pudo actualizar estado para {combo_id}, condición fallida")
                    else:
                        raise

            except Exception as e:
                logger.error(f"Error al iniciar ejecución para {combo_id}: {str(e)}")

        return started

    except Exception as e:
        logger.error(f"Error al iniciar procesamiento: {str(e)}")
        return 0


def get_processing_summary(state_table_name, region="eu-west-1"):
    """
    Obtiene un resumen del estado actual de procesamiento.
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    
    # Validar tabla
    if not validate_table(dynamodb, state_table_name):
        return {
            "success": False,
            "error": "Tabla DynamoDB no válida",
            "timestamp": datetime.now().isoformat(),
        }
        
    state_table = dynamodb.Table(state_table_name)

    try:
        # Función para escanear toda la tabla con paginación
        def scan_all_items():
            all_items = []
            last_evaluated_key = None
            
            while True:
                scan_kwargs = {"Limit": MAX_SCAN_ITEMS}
                
                if last_evaluated_key:
                    scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
                    
                response = state_table.scan(**scan_kwargs)
                all_items.extend(response.get("Items", []))
                
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
                    
            return all_items
        
        # Escanear toda la tabla con paginación
        items = scan_all_items()
        
        if not items:
            return {
                "total": 0,
                "by_status": {},
                "timestamp": datetime.now().isoformat(),
            }

        # Contar combinaciones por estado
        total = len(items)
        by_status = {}
        by_enterprise = {}
        
        for item in items:
            # Contar por estado
            status = item.get("status", "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            
            # Contar por empresa
            enterprise = item.get("P_EMPRESA", "unknown")
            if enterprise not in by_enterprise:
                by_enterprise[enterprise] = {
                    "total": 0, 
                    "completed": 0, 
                    "failed": 0, 
                    "processing": 0, 
                    "pending": 0
                }
            
            by_enterprise[enterprise]["total"] += 1
            if status in by_enterprise[enterprise]:
                by_enterprise[enterprise][status] += 1

        # Obtener las últimas ejecutadas
        recent_items = sorted(
            [item for item in items if item.get("started_at")],
            key=lambda x: x.get("started_at", ""),
            reverse=True,
        )[:5]

        # Obtener las últimas fallidas
        failed_items = [item for item in items if item.get("status") == "failed"][:5]
        
        # Calcular porcentaje completado
        completed = by_status.get("completed", 0)
        completion_percentage = (completed / total * 100) if total > 0 else 0

        # Obtener información sobre reintentos
        retry_stats = {
            "total_retries": sum(int(item.get("retries", 0)) for item in items),
            "items_with_retries": len([item for item in items if int(item.get("retries", 0)) > 0]),
            "max_retries": max((int(item.get("retries", 0)) for item in items), default=0)
        }

        return {
            "total": total,
            "by_status": by_status,
            "completion_percentage": round(completion_percentage, 2),
            "by_enterprise": by_enterprise,
            "recent": recent_items,
            "failed": failed_items,
            "retry_stats": retry_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error al obtener resumen: {str(e)}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


def reset_failed_combinations(state_table_name, region="eu-west-1"):
    """
    Restablece las combinaciones fallidas a estado pendiente.
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    
    # Validar tabla
    if not validate_table(dynamodb, state_table_name):
        logger.error(f"La tabla {state_table_name} no es válida para el reset")
        return 0
        
    state_table = dynamodb.Table(state_table_name)

    try:
        # Función para obtener todas las combinaciones fallidas con paginación
        def get_all_failed_combinations():
            failed_combinations = []
            last_evaluated_key = None
            
            while True:
                scan_kwargs = {
                    "FilterExpression": "#s = :failed",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {":failed": "failed"},
                    "Limit": MAX_SCAN_ITEMS
                }
                
                if last_evaluated_key:
                    scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
                    
                response = state_table.scan(**scan_kwargs)
                failed_combinations.extend(response.get("Items", []))
                
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
                    
            return failed_combinations
            
        # Obtener combinaciones fallidas
        failed_items = get_all_failed_combinations()
        
        if not failed_items:
            logger.info("No hay combinaciones fallidas para restablecer")
            return 0

        # Restablecer cada combinación fallida de forma atómica
        reset_count = 0
        for item in failed_items:
            combo_id = item["id"]

            try:
                # Actualizar estado en DynamoDB con condición
                state_table.update_item(
                    Key={"id": combo_id},
                    UpdateExpression="SET #s = :pending, reset_at = :t, retries = if_not_exists(retries, :zero) + :one",
                    ConditionExpression="#s = :failed",  # Solo actualizar si sigue en estado fallido
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":pending": "pending",
                        ":failed": "failed",
                        ":t": datetime.now().isoformat(),
                        ":zero": 0,
                        ":one": 1
                    },
                )

                reset_count += 1
                logger.info(f"Combinación {combo_id} restablecida a pendiente")

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    logger.warning(f"La combinación {combo_id} ya no está en estado fallido")
                else:
                    logger.error(f"Error al restablecer combinación {combo_id}: {str(e)}")

        logger.info(f"Se restablecieron {reset_count} combinaciones fallidas")
        return reset_count

    except Exception as e:
        logger.error(f"Error al restablecer combinaciones fallidas: {str(e)}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Registrar combinaciones GTFS y gestionar su procesamiento"
    )
    parser.add_argument(
        "operation",
        choices=["register", "start", "summary", "reset"],
        help="Operación a realizar",
    )
    parser.add_argument("--bucket", help="Nombre del bucket S3")
    parser.add_argument(
        "--state-table", required=True, help="Nombre de la tabla DynamoDB de estado"
    )
    parser.add_argument("--state-machine-arn", help="ARN de la máquina de estados")
    parser.add_argument(
        "--combinations-file",
        default="batch_processing/combinations.json",
        help="Archivo JSON con las combinaciones",
    )
    parser.add_argument("--region", default="eu-west-1", help="Región AWS")
    parser.add_argument(
        "--max-start",
        type=int,
        default=1,
        help="Número máximo de ejecuciones a iniciar",
    )
    parser.add_argument(
        "--clean-inconsistent",
        action="store_true",
        help="Limpiar datos inconsistentes antes de registrar"
    )

    args = parser.parse_args()

    if args.operation == "register":
        if not args.bucket:
            parser.error("--bucket es requerido para la operación register")

        register_combinations(
            args.bucket,
            args.state_table,
            args.state_machine_arn,
            args.combinations_file,
            args.region,
            False,
            0,
            args.clean_inconsistent
        )

    elif args.operation == "start":
        if not args.bucket or not args.state_machine_arn:
            parser.error(
                "--bucket y --state-machine-arn son requeridos para la operación start"
            )

        register_combinations(
            args.bucket,
            args.state_table,
            args.state_machine_arn,
            args.combinations_file,
            args.region,
            True,
            args.max_start,
            args.clean_inconsistent
        )

    elif args.operation == "summary":
        summary = get_processing_summary(args.state_table, args.region)
        print(json.dumps(summary, indent=2))

    elif args.operation == "reset":
        reset_count = reset_failed_combinations(args.state_table, args.region)
        print(f"Se restablecieron {reset_count} combinaciones fallidas")


if __name__ == "__main__":
    main()