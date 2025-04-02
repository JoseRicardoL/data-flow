"""
Función Lambda para activar la siguiente ejecución.
Busca la siguiente combinación pendiente y la inicia.
"""

import os
import boto3
import json
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
stepfunctions = boto3.client("stepfunctions")
ssm = boto3.client("ssm")


def handler(event, context):
    """
    Busca la siguiente combinación pendiente y activa su procesamiento.

    Args:
        event: Evento Lambda
        context: Contexto Lambda

    Returns:
        Diccionario con resultado de la operación
    """
    # Obtener tabla de estado
    state_table = dynamodb.Table(os.environ["STATE_TABLE"])

    # Obtener el ARN de la máquina de estados desde Parameter Store
    try:
        param_name = os.environ["STATE_MACHINE_PARAM"]
        response = ssm.get_parameter(Name=param_name)
        state_machine_arn = response["Parameter"]["Value"]

        if not state_machine_arn or state_machine_arn == "placeholder-will-be-updated":
            return {
                "nextExecutionTriggered": False,
                "reason": f"State machine ARN not set in Parameter Store: {param_name}",
            }
    except Exception as e:
        return {
            "nextExecutionTriggered": False,
            "error": f"Error getting state machine ARN from Parameter Store: {str(e)}",
        }

    # Buscar la siguiente combinación pendiente
    response = state_table.scan(
        FilterExpression="#s = :pending",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":pending": "pending"},
        Limit=1,
    )

    items = response.get("Items", [])

    if not items:
        return {
            "nextExecutionTriggered": False,
            "reason": "No pending combinations found",
        }

    # Obtener la siguiente combinación pendiente
    next_combination = items[0]

    # Iniciar nueva ejecución de la máquina de estados
    try:
        execution_name = f"GTFSProcess-{next_combination['id']}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        execution_response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(next_combination),
        )

        # Actualizar estado a "processing"
        state_table.update_item(
            Key={"id": next_combination["id"]},
            UpdateExpression="SET #s = :processing, execution_arn = :arn, started_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":processing": "processing",
                ":arn": execution_response["executionArn"],
                ":t": datetime.now().isoformat(),
            },
        )

        return {
            "nextExecutionTriggered": True,
            "combinationId": next_combination["id"],
            "executionArn": execution_response["executionArn"],
        }
    except Exception as e:
        return {
            "nextExecutionTriggered": False,
            "error": str(e),
            "combinationId": next_combination["id"],
        }
