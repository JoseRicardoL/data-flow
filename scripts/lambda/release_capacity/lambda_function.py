"""
Función Lambda para liberar capacidad.
Actualiza el contador de ejecuciones activas y el estado de la combinación.
"""

import os
import boto3
import json
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Libera capacidad y actualiza el estado de la combinación.
    
    Args:
        event: Evento Lambda con información de la combinación
        context: Contexto Lambda
        
    Returns:
        Diccionario con resultado de la operación
    """
    # Obtener tablas de DynamoDB
    capacity_table = dynamodb.Table(os.environ['CAPACITY_TABLE'])
    state_table = dynamodb.Table(os.environ['STATE_TABLE'])
    
    # Actualizar estado de procesamiento
    combination_id = event.get('combinationId')
    status = event.get('status', 'completed')
    
    if combination_id:
        # Preparar atributos de actualización
        update_exp = 'SET #s = :s, last_updated = :t'
        exp_attr_names = {'#s': 'status'}
        exp_attr_values = {
            ':s': status,
            ':t': datetime.now().isoformat()
        }
        
        # Añadir error si está presente
        if 'error' in event:
            update_exp += ', error = :e'
            exp_attr_values[':e'] = event['error']
        
        # Actualizar estado
        state_table.update_item(
            Key={'id': combination_id},
            UpdateExpression=update_exp,
            ExpressionAttributeNames=exp_attr_names,
            ExpressionAttributeValues=exp_attr_values
        )
    
    # Decrementar contador de ejecuciones activas
    try:
        capacity_table.update_item(
            Key={'id': 'capacity_control'},
            UpdateExpression='SET active_executions = active_executions - :dec',
            ConditionExpression='active_executions > :zero',
            ExpressionAttributeValues={':dec': 1, ':zero': 0}
        )
        
        return {
            'capacityReleased': True,
            'combinationId': combination_id,
            'status': status
        }
    except Exception as e:
        # Manejar errores (p.ej. condición no cumplida)
        return {
            'capacityReleased': False,
            'error': str(e),
            'combinationId': combination_id,
            'status': status
        }