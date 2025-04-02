"""
Función Lambda para verificar capacidad disponible.
Controla el número máximo de ejecuciones concurrentes.
"""

import os
import boto3
import json

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Verifica si hay capacidad disponible para iniciar una nueva ejecución.
    
    Args:
        event: Evento Lambda
        context: Contexto Lambda
        
    Returns:
        Diccionario con información de capacidad
    """
    # Obtener tabla de DynamoDB y máximo de ejecuciones
    table = dynamodb.Table(os.environ['CAPACITY_TABLE'])
    MAX_CONCURRENT = int(os.environ['MAX_CONCURRENT_EXECUTIONS'])
    
    # Verificar capacidad disponible
    response = table.get_item(Key={'id': 'capacity_control'})
    
    if 'Item' not in response:
        # Inicializar registro si no existe
        table.put_item(
            Item={
                'id': 'capacity_control',
                'active_executions': 0,
                'max_executions': MAX_CONCURRENT
            }
        )
        current_active = 0
    else:
        current_active = response['Item'].get('active_executions', 0)
    
    # Verificar si hay capacidad disponible
    if current_active < MAX_CONCURRENT:
        # Incrementar contador de ejecuciones activas
        table.update_item(
            Key={'id': 'capacity_control'},
            UpdateExpression='SET active_executions = active_executions + :inc',
            ExpressionAttributeValues={':inc': 1}
        )
        return {
            'hasCapacity': True,
            'activeExecutions': current_active + 1,
            'maxExecutions': MAX_CONCURRENT
        }
    else:
        return {
            'hasCapacity': False,
            'activeExecutions': current_active,
            'maxExecutions': MAX_CONCURRENT
        }