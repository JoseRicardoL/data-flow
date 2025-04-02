"""
Función Lambda para actualizar el parámetro SSM con el ARN de la máquina de estados.
Utilizada como Custom Resource en CloudFormation.
"""

import boto3
import cfnresponse
import os


def handler(event, context):
    """
    Actualiza un parámetro SSM con el ARN de la máquina de estados.

    Args:
        event: Evento de Custom Resource de CloudFormation
        context: Contexto Lambda
    """
    if event["RequestType"] == "Delete":
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        return

    response_data = {}
    try:
        # Obtener el ARN de la máquina de estados
        state_machine_arn = event["ResourceProperties"]["StateMachineArn"]
        parameter_name = event["ResourceProperties"]["ParameterName"]

        # Actualizar el parámetro SSM
        ssm_client = boto3.client("ssm")
        ssm_client.put_parameter(
            Name=parameter_name, Value=state_machine_arn, Type="String", Overwrite=True
        )

        response_data["Message"] = (
            f"Parameter {parameter_name} updated with {state_machine_arn}"
        )
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
    except Exception as e:
        response_data["Error"] = str(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
