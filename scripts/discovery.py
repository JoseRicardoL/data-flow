#!/usr/bin/env python3
"""
Script para descubrir todas las combinaciones de datos GTFS en S3.
Escanea el bucket para encontrar todas las combinaciones de:
explotación, contrato y versión.
"""

import boto3
import json
import re
import os
import argparse
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("discovery.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def discover_gtfs_data(bucket_name, output_file="batch_processing/combinations.json"):
    """
    Descubre todas las combinaciones únicas de explotación, contrato y versión en S3.
    
    Args:
        bucket_name: Nombre del bucket de S3 a escanear
        output_file: Archivo de salida para guardar las combinaciones
        
    Returns:
        Lista de combinaciones encontradas
    """
    s3_client = boto3.client('s3')
    combinations = []
    
    # Patrones para extraer valores
    explotation_pattern = r'explotation=(\d+)'
    contract_pattern = r'contract=(\d+)'
    version_pattern = r'version=([^/]+)'
    
    # Lista de tipos de datos GTFS a buscar para validar combinaciones
    gtfs_types = ['AGENCY', 'ROUTES', 'TRIPS', 'STOPS', 'STOP_TIMES']
    
    logger.info(f"Iniciando escaneo de bucket: {bucket_name}")
    
    try:
        # Primero listar todos los tipos de datos GTFS disponibles
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='GTFS/',
            Delimiter='/'
        )
        
        if 'CommonPrefixes' not in response:
            logger.error(f"No se encontraron directorios GTFS en el bucket {bucket_name}")
            return []
            
        # Procesar cada tipo de datos GTFS
        discovered_prefixes = set()
        
        for prefix in response['CommonPrefixes']:
            gtfs_type = prefix['Prefix'].split('/')[1]
            logger.info(f"Escaneando tipo de datos GTFS: {gtfs_type}")
            
            # Listar todas las explotaciones para este tipo
            try:
                explotation_response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=f'GTFS/{gtfs_type}/',
                    Delimiter='/'
                )
                
                if 'CommonPrefixes' not in explotation_response:
                    continue
                    
                for explotation_prefix in explotation_response['CommonPrefixes']:
                    explotation_path = explotation_prefix['Prefix']
                    explotation_match = re.search(explotation_pattern, explotation_path)
                    
                    if not explotation_match:
                        continue
                        
                    explotation = explotation_match.group(1)
                    
                    # Listar todos los contratos para esta explotación
                    contract_response = s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=explotation_path,
                        Delimiter='/'
                    )
                    
                    if 'CommonPrefixes' not in contract_response:
                        continue
                        
                    for contract_prefix in contract_response['CommonPrefixes']:
                        contract_path = contract_prefix['Prefix']
                        contract_match = re.search(contract_pattern, contract_path)
                        
                        if not contract_match:
                            continue
                            
                        contract = contract_match.group(1)
                        
                        # Listar todas las versiones para este contrato
                        version_response = s3_client.list_objects_v2(
                            Bucket=bucket_name,
                            Prefix=contract_path,
                            Delimiter='/'
                        )
                        
                        if 'CommonPrefixes' not in version_response:
                            continue
                            
                        for version_prefix in version_response['CommonPrefixes']:
                            version_path = version_prefix['Prefix']
                            version_match = re.search(version_pattern, version_path)
                            
                            if not version_match:
                                continue
                                
                            version = version_match.group(1)
                            
                            # Verificar que existe al menos un archivo en este directorio
                            check_response = s3_client.list_objects_v2(
                                Bucket=bucket_name,
                                Prefix=version_path,
                                MaxKeys=1
                            )
                            
                            if 'Contents' not in check_response:
                                continue
                                
                            # Registrar esta combinación para su procesamiento
                            combination_key = f"{explotation}_{contract}_{version}"
                            if combination_key not in discovered_prefixes:
                                discovered_prefixes.add(combination_key)
                                combinations.append({
                                    "P_EMPRESA": explotation,
                                    "P_CONTR": contract,
                                    "P_VERSION": version,
                                    "status": "pending",
                                    "discovery_time": datetime.now().isoformat(),
                                    "gtfs_type": gtfs_type
                                })
                                logger.info(f"Descubierta combinación: E={explotation}, C={contract}, V={version}")
            
            except Exception as e:
                logger.error(f"Error al procesar {gtfs_type}: {str(e)}")
                continue
        
        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Guardar resultado en archivo JSON
        with open(output_file, 'w') as f:
            result = {
                "combinations": combinations,
                "total": len(combinations),
                "timestamp": datetime.now().isoformat(),
                "bucket": bucket_name
            }
            json.dump(result, f, indent=2)
        
        logger.info(f"Descubrimiento completado: {len(combinations)} combinaciones válidas")
        logger.info(f"Resultados guardados en: {output_file}")
        
        return combinations
        
    except Exception as e:
        logger.error(f"Error durante el descubrimiento: {str(e)}")
        return []

# Si se ejecuta directamente
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Descubrir combinaciones de datos GTFS en S3')
    parser.add_argument('bucket', help='Nombre del bucket S3')
    parser.add_argument('--output', '-o', default='batch_processing/combinations.json', help='Archivo de salida JSON')
    
    args = parser.parse_args()
    discover_gtfs_data(args.bucket, args.output)