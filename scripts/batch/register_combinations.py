#!/usr/bin/env python3
"""
Script para registrar combinaciones GTFS en DynamoDB y iniciar su procesamiento.
"""

import boto3
import json
import os
import argparse
import logging
import time
from datetime import datetime
import uuid
from botocore.exceptions import ClientError

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


def register_combinations(
    bucket_name,
    state_table_name,
    stateMachineArn,
    combinations_file="batch_processing/combinations.json",
    region="eu-west-1",
    start_processing=False,
    max_to_start=1,
):
    """
    Registra combinaciones en DynamoDB y opcionalmente inicia su procesamiento.

    Args:
        bucket_name: Nombre del bucket S3
        state_table_name: Nombre de la tabla DynamoDB de estado
        stateMachineArn: ARN de la máquina de estados
        combinations_file: Archivo JSON con las combinaciones a registrar
        region: Región AWS
        start_processing: Si es True, inicia el procesamiento de las combinaciones
        max_to_start: Número máximo de ejecuciones a iniciar si start_processing es True
    """
    # Inicializar clientes AWS
    dynamodb = boto3.resource("dynamodb", region_name=region)
    step_functions = boto3.client("stepfunctions", region_name=region)

    # Obtener tabla de DynamoDB
    state_table = dynamodb.Table(state_table_name)

    # Cargar combinaciones desde el archivo JSON
    try:
        with open(combinations_file, "r") as f:
            data = json.load(f)
            combinations = data.get("combinations", [])

        if not combinations:
            logger.error(f"No se encontraron combinaciones en {combinations_file}")
            return False

        logger.info(
            f"Se cargaron {len(combinations)} combinaciones desde {combinations_file}"
        )

    except Exception as e:
        logger.error(f"Error al cargar combinaciones: {str(e)}")
        return False

    # Registrar combinaciones en DynamoDB
    registered = 0
    skipped = 0

    for combo in combinations:
        try:
            # Generar ID único para esta combinación (si no tiene uno)
            combo_id = (
                combo.get("id")
                or f"{combo['P_EMPRESA']}_{combo['P_CONTR']}_{combo['P_VERSION']}"
            )

            # Verificar si ya existe
            try:
                response = state_table.get_item(Key={"id": combo_id})

                if "Item" in response:
                    logger.info(
                        f"Combinación ya registrada: {combo_id}, estado: {response['Item'].get('status', 'desconocido')}"
                    )
                    skipped += 1
                    continue
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise

            # Crear registro en DynamoDB
            item = {
                "id": combo_id,
                "P_EMPRESA": combo["P_EMPRESA"],
                "P_CONTR": combo["P_CONTR"],
                "P_VERSION": combo["P_VERSION"],
                "status": "pending",
                "registered_at": datetime.now().isoformat(),
            }

            # Añadir campos adicionales si existen
            if "gtfs_type" in combo:
                item["gtfs_type"] = combo["gtfs_type"]

            state_table.put_item(Item=item)
            registered += 1
            logger.info(f"Combinación registrada: {combo_id}")

        except Exception as e:
            logger.error(f"Error al registrar combinación: {str(e)}")

    logger.info(f"Registro completado: {registered} nuevas, {skipped} existentes")

    # Iniciar procesamiento si se solicitó
    if start_processing and registered + skipped > 0:
        started = 0

        # Obtener combinaciones pendientes
        try:
            response = state_table.scan(
                FilterExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":pending": "pending"},
            )

            pending_combinations = response.get("Items", [])

            if not pending_combinations:
                logger.info("No hay combinaciones pendientes para procesar")
                return True

            logger.info(
                f"Se encontraron {len(pending_combinations)} combinaciones pendientes"
            )

            # Iniciar ejecuciones hasta el límite especificado
            for combo in pending_combinations[:max_to_start]:
                combo_id = combo["id"]

                # Iniciar ejecución de la máquina de estados
                execution_name = (
                    f"GTFSProcess-{combo_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                )

                try:
                    execution = step_functions.start_execution(
                        stateMachineArn=stateMachineArn,
                        name=execution_name,
                        input=json.dumps(combo),
                    )

                    # Actualizar estado en DynamoDB
                    state_table.update_item(
                        Key={"id": combo_id},
                        UpdateExpression="SET #s = :processing, execution_arn = :arn, started_at = :t",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={
                            ":processing": "processing",
                            ":arn": execution["executionArn"],
                            ":t": datetime.now().isoformat(),
                        },
                    )

                    started += 1
                    logger.info(
                        f"Ejecución iniciada para {combo_id}: {execution['executionArn']}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error al iniciar ejecución para {combo_id}: {str(e)}"
                    )

            logger.info(f"Se iniciaron {started} ejecuciones")

        except Exception as e:
            logger.error(f"Error al iniciar procesamiento: {str(e)}")

    return True


def get_processing_summary(state_table_name, region="eu-west-1"):
    """
    Obtiene un resumen del estado actual de procesamiento.

    Args:
        state_table_name: Nombre de la tabla DynamoDB de estado
        region: Región AWS

    Returns:
        Diccionario con el resumen del estado
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    state_table = dynamodb.Table(state_table_name)

    try:
        # Escanear toda la tabla
        response = state_table.scan()

        if "Items" not in response:
            return {
                "total": 0,
                "by_status": {},
                "timestamp": datetime.now().isoformat(),
            }

        # Contar combinaciones por estado
        items = response["Items"]
        total = len(items)

        by_status = {}
        for item in items:
            status = item.get("status", "unknown")
            by_status[status] = by_status.get(status, 0) + 1

        # Obtener las últimas ejecutadas
        recent_items = sorted(
            [item for item in items if item.get("started_at")],
            key=lambda x: x.get("started_at", ""),
            reverse=True,
        )[:5]

        # Obtener las últimas fallidas
        failed_items = [item for item in items if item.get("status") == "failed"][:5]

        return {
            "total": total,
            "by_status": by_status,
            "recent": recent_items,
            "failed": failed_items,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error al obtener resumen: {str(e)}")
        return {
            "total": 0,
            "by_status": {},
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


def reset_failed_combinations(state_table_name, region="eu-west-1"):
    """
    Restablece las combinaciones fallidas a estado pendiente.

    Args:
        state_table_name: Nombre de la tabla DynamoDB de estado
        region: Región AWS

    Returns:
        Número de combinaciones restablecidas
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    state_table = dynamodb.Table(state_table_name)

    try:
        # Obtener combinaciones fallidas
        response = state_table.scan(
            FilterExpression="#s = :failed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "failed"},
        )

        if "Items" not in response or not response["Items"]:
            logger.info("No hay combinaciones fallidas para restablecer")
            return 0

        # Restablecer cada combinación fallida
        failed_items = response["Items"]
        reset_count = 0

        for item in failed_items:
            combo_id = item["id"]

            try:
                # Actualizar estado en DynamoDB
                state_table.update_item(
                    Key={"id": combo_id},
                    UpdateExpression="SET #s = :pending, reset_at = :t, retries = :r",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":pending": "pending",
                        ":t": datetime.now().isoformat(),
                        ":r": item.get("retries", 0) + 1,
                    },
                )

                reset_count += 1
                logger.info(f"Combinación {combo_id} restablecida a pendiente")

            except Exception as e:
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
        )

    elif args.operation == "summary":
        summary = get_processing_summary(args.state_table, args.region)
        print(json.dumps(summary, indent=2))

    elif args.operation == "reset":
        reset_count = reset_failed_combinations(args.state_table, args.region)
        print(f"Se restablecieron {reset_count} combinaciones fallidas")


if __name__ == "__main__":
    main()
