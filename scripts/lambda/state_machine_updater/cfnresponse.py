"""
Módulo para enviar respuestas a CloudFormation desde funciones Lambda en recursos personalizados.
"""

import json
import urllib.request
import urllib.parse

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send(
    event,
    context,
    response_status,
    response_data,
    physical_resource_id=None,
    no_echo=False,
):
    """
    Envía una respuesta a CloudFormation para un evento de recurso personalizado.

    Args:
        event: El evento de CloudFormation que activó la función
        context: El contexto de Lambda
        response_status: 'SUCCESS' o 'FAILED'
        response_data: Diccionario con datos a devolver
        physical_resource_id: ID del recurso físico (opcional)
        no_echo: Indica si la respuesta debe ser ocultada en los logs (opcional)
    """
    response_url = event["ResponseURL"]

    print(response_url)

    response_body = {}
    response_body["Status"] = response_status
    response_body["Reason"] = (
        "See the details in CloudWatch Log Stream: " + context.log_stream_name
    )
    response_body["PhysicalResourceId"] = (
        physical_resource_id or context.log_stream_name
    )
    response_body["StackId"] = event["StackId"]
    response_body["RequestId"] = event["RequestId"]
    response_body["LogicalResourceId"] = event["LogicalResourceId"]
    response_body["NoEcho"] = no_echo
    response_body["Data"] = response_data

    json_response_body = json.dumps(response_body)

    print("Response body:\n" + json_response_body)

    headers = {"content-type": "", "content-length": str(len(json_response_body))}

    try:
        req = urllib.request.Request(
            response_url,
            data=json_response_body.encode("utf-8"),
            headers=headers,
            method="PUT",
        )
        response = urllib.request.urlopen(req)
        print("Status code: " + response.reason)
    except Exception as e:
        print("send(..) failed executing request.urlopen(..): " + str(e))
