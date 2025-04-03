#!/usr/bin/env python3
"""
Script para descubrir combinaciones de datos GTFS en S3
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
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("discovery.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Lista de tipos de datos GTFS requeridos para el procesamiento completo
REQUIRED_GTFS_TYPES = ["AGENCY", "ROUTES", "TRIPS", "STOPS", "STOP_TIMES"]

# El tipo GTFS con menos datos normalmente (usado como punto de partida eficiente)
SEED_GTFS_TYPE = "AGENCY"  # Generalmente AGENCY tiene menos datos/combinaciones


def discover_gtfs_combinations(s3_client, bucket_name):
    """
    Descubre las posibles combinaciones de explotación/contrato/versión
    utilizando AGENCY como punto de partida (habitualmente tiene menos combinaciones).

    Args:
        s3_client: Cliente de boto3 para S3
        bucket_name: Nombre del bucket de S3

    Returns:
        dict: Mapa de combinaciones potenciales con su estado de validación
    """
    # Patrones para extraer valores
    explotation_pattern = r"explotation=(\d+)"
    contract_pattern = r"contract=(\d+)"
    version_pattern = r"version=([^/]+)"

    logger.info(f"Explorando combinaciones utilizando {SEED_GTFS_TYPE} como semilla")

    # Estructura para almacenar combinaciones y su estado
    combinations = {}

    # Explorar el tipo GTFS semilla para encontrar combinaciones potenciales
    try:
        # Listar todas las explotaciones para el tipo semilla
        prefix = f"GTFS/{SEED_GTFS_TYPE}/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        )

        if "CommonPrefixes" not in response:
            logger.warning(f"No se encontraron datos para {SEED_GTFS_TYPE}")
            return combinations

        # Procesar cada explotación encontrada
        for explotation_prefix in response["CommonPrefixes"]:
            explotation_path = explotation_prefix["Prefix"]
            explotation_match = re.search(explotation_pattern, explotation_path)

            if not explotation_match:
                continue

            explotation = explotation_match.group(1)

            # Listar contratos para esta explotación
            contract_response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=explotation_path, Delimiter="/"
            )

            if "CommonPrefixes" not in contract_response:
                continue

            for contract_prefix in contract_response["CommonPrefixes"]:
                contract_path = contract_prefix["Prefix"]
                contract_match = re.search(contract_pattern, contract_path)

                if not contract_match:
                    continue

                contract = contract_match.group(1)

                # Listar versiones para este contrato
                version_response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=contract_path, Delimiter="/"
                )

                if "CommonPrefixes" not in version_response:
                    continue

                for version_prefix in version_response["CommonPrefixes"]:
                    version_path = version_prefix["Prefix"]
                    version_match = re.search(version_pattern, version_path)

                    if not version_match:
                        continue

                    version = version_match.group(1)

                    # Verificar que el archivo principal de AGENCY existe
                    agency_file = f"{version_path}agency.txt"
                    try:
                        s3_client.head_object(Bucket=bucket_name, Key=agency_file)

                        # Agregar esta combinación potencial
                        combo_key = f"{explotation}_{contract}_{version}"
                        combinations[combo_key] = {
                            "P_EMPRESA": explotation,
                            "P_CONTR": contract,
                            "P_VERSION": version,
                            "valid_types": {
                                SEED_GTFS_TYPE
                            },  # Marcamos el tipo semilla como válido
                            "missing_types": set(),
                        }

                    except Exception:
                        # No existe el archivo agency.txt, saltamos esta combinación
                        continue

    except Exception as e:
        logger.error(f"Error explorando combinaciones iniciales: {str(e)}")

    logger.info(f"Descubiertas {len(combinations)} combinaciones potenciales")
    return combinations


def validate_combinations(s3_client, bucket_name, combinations):
    """
    Valida si las combinaciones potenciales tienen todos los archivos GTFS requeridos.

    Args:
        s3_client: Cliente de boto3 para S3
        bucket_name: Nombre del bucket de S3
        combinations: Diccionario de combinaciones potenciales

    Returns:
        dict: Mapa actualizado de combinaciones con estado de validación
    """
    if not combinations:
        return {}

    # Explorar los tipos GTFS restantes para validar cada combinación
    for gtfs_type in [t for t in REQUIRED_GTFS_TYPES if t != SEED_GTFS_TYPE]:
        logger.info(f"Validando archivos para tipo GTFS: {gtfs_type}")

        for combo_key, combo_data in list(combinations.items()):
            explotation = combo_data["P_EMPRESA"]
            contract = combo_data["P_CONTR"]
            version = combo_data["P_VERSION"]

            # Verificar si existe el archivo para este tipo GTFS
            file_path = f"GTFS/{gtfs_type}/explotation={explotation}/contract={contract}/version={version}/{gtfs_type.lower()}.txt"

            try:
                s3_client.head_object(Bucket=bucket_name, Key=file_path)
                # Marcar este tipo como válido para esta combinación
                combo_data["valid_types"].add(gtfs_type)
            except Exception:
                # Archivo no encontrado, marcar como faltante
                combo_data["missing_types"].add(gtfs_type)

    # Filtrar solo las combinaciones válidas (que tienen todos los tipos requeridos)
    valid_combinations = {}
    for combo_key, combo_data in combinations.items():
        if len(combo_data["valid_types"]) == len(REQUIRED_GTFS_TYPES):
            # Esta combinación tiene todos los tipos requeridos
            explotation = combo_data["P_EMPRESA"]
            contract = combo_data["P_CONTR"]
            version = combo_data["P_VERSION"]

            valid_combinations[combo_key] = {
                "P_EMPRESA": explotation,
                "P_CONTR": contract,
                "P_VERSION": version,
                "status": "pending",
                "discovery_time": datetime.now().isoformat(),
                "gtfs_types": list(combo_data["valid_types"]),
            }
            logger.info(
                f"Combinación válida: E={explotation}, C={contract}, V={version}"
            )
        else:
            # Loguear las combinaciones inválidas por falta de archivos
            missing = combo_data["missing_types"]
            explotation = combo_data["P_EMPRESA"]
            contract = combo_data["P_CONTR"]
            version = combo_data["P_VERSION"]
            logger.warning(
                f"Combinación incompleta (faltan archivos): E={explotation}, C={contract}, V={version}. "
                f"Archivos faltantes: {', '.join(missing)}"
            )

    return valid_combinations


def discover_gtfs_data(
    bucket_name, region="eu-west-1", output_file="batch_processing/combinations.json"
):
    """
    Descubre todas las combinaciones válidas de explotación, contrato y versión en S3,
    utilizando un enfoque optimizado.

    Args:
        bucket_name: Nombre del bucket de S3 a escanear
        region: Región AWS donde está el bucket
        output_file: Archivo de salida para guardar las combinaciones

    Returns:
        Lista de combinaciones encontradas
    """
    logger.info(
        f"Iniciando descubrimiento optimizado en bucket: {bucket_name}, región: {region}"
    )

    s3_client = boto3.client("s3", region_name=region)

    try:
        # Paso 1: Descubrir combinaciones potenciales usando el tipo semilla
        potential_combinations = discover_gtfs_combinations(s3_client, bucket_name)

        if not potential_combinations:
            logger.warning("No se encontraron combinaciones potenciales")
            return []

        # Paso 2: Validar combinaciones y filtrar las que tienen todos los archivos requeridos
        valid_combinations = validate_combinations(
            s3_client, bucket_name, potential_combinations
        )

        # Convertir el diccionario a lista para el formato de salida
        combinations_list = list(valid_combinations.values())

        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Guardar resultado en archivo JSON
        with open(output_file, "w") as f:
            result = {
                "combinations": combinations_list,
                "total": len(combinations_list),
                "timestamp": datetime.now().isoformat(),
                "bucket": bucket_name,
                "region": region,
            }
            json.dump(result, f, indent=2)

        logger.info(
            f"Descubrimiento completado: {len(combinations_list)} combinaciones válidas"
        )
        logger.info(f"Resultados guardados en: {output_file}")

        return combinations_list

    except Exception as e:
        logger.error(f"Error durante el descubrimiento: {str(e)}")
        return []


# Si se ejecuta directamente
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Descubrir combinaciones de datos GTFS en S3 de manera optimizada"
    )
    parser.add_argument("bucket", help="Nombre del bucket S3")
    parser.add_argument(
        "--region", default="eu-west-1", help="Región AWS (por defecto: eu-west-1)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="batch_processing/combinations.json",
        help="Archivo de salida JSON",
    )

    args = parser.parse_args()
    discover_gtfs_data(args.bucket, args.region, args.output)
