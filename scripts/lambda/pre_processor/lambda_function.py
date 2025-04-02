"""
Preprocesador de datos GTFS.

Este módulo implementa la función Lambda que realiza el preprocesamiento de los datos GTFS
antes de ejecutar los jobs de macro y macro_stops. El preprocesador recibe parámetros de
explotación, contrato y versión, y genera archivos temporales en S3 para su uso posterior.
"""

import os
import gc
import json
import boto3
import pandas as pd
import uuid
import psutil
import functools
from io import StringIO, BytesIO

s3_client = boto3.client("s3")
s3 = boto3.resource("s3")


def log_memory_usage(label: str) -> float:
    """Registra el uso de memoria actual (RSS) y lo devuelve en MB."""
    gc.collect()
    memory_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    print(f"MEMORIA [{label}]: {memory_mb:.2f} MB")
    return memory_mb


def memory_logger(func):
    """Decorador que registra el uso de memoria antes y después de la ejecución de la función."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log_memory_usage(f"Antes de {func.__name__}")
        result = func(*args, **kwargs)
        log_memory_usage(f"Después de {func.__name__}")
        return result

    return wrapper


# Sobrescribir pd.merge para monitorear el uso de memoria
_original_merge = pd.merge


@functools.wraps(_original_merge)
def merge_with_memory(*args, **kwargs):
    input_shape = getattr(args[0], "shape", "N/A") if args else "N/A"
    log_memory_usage(f"Antes del merge (input shape: {input_shape})")
    result = _original_merge(*args, **kwargs)
    result_shape = getattr(result, "shape", "N/A")
    log_memory_usage(f"Después del merge (result shape: {result_shape})")
    return result


pd.merge = merge_with_memory


def read_csv_s3(bucket, key, **kwargs):
    """Lee un archivo CSV desde S3"""
    kwargs["low_memory"] = True
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(response["Body"].read()), encoding="utf8", **kwargs)


def write_csv_s3(df, bucket, key):
    """Guarda un DataFrame como CSV en S3"""
    with StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


@memory_logger
def build_combos(trips, routes, stop_times):
    """Construye las combinaciones de rutas/viajes/paradas necesarias para el mapeo."""
    log_memory_usage("Inicio build_combos")

    combos = {}
    valid_trip_ids = set(stop_times["trip_id"].unique())
    routes_dict = routes.set_index("route_id")[
        ["route_color", "route_short_name"]
    ].to_dict("index")

    # Pre-filtrar trips que tienen stops
    valid_trips = trips[trips["trip_id"].isin(valid_trip_ids)]
    grouped_trips = valid_trips.groupby(["route_id", "trip_headsign"])

    for (route, head), group in grouped_trips:
        if route not in combos:
            combos[route] = {}

        # Obtener shapes válidos (excluyendo nulos)
        shape_ids = sorted(group["shape_id"].dropna().unique())
        group_trip_ids = set(group["trip_id"])

        # Si tiene shapes o está en los trip_ids válidos
        if shape_ids or (group_trip_ids & valid_trip_ids):
            route_color = "#1f77b4"  # Color por defecto
            if route in routes_dict and routes_dict[route]["route_color"]:
                raw_color = routes_dict[route]["route_color"]
                if pd.notna(raw_color):
                    route_color = (
                        f"#{raw_color}" if not raw_color.startswith("#") else raw_color
                    )

            combos[route][head] = {
                "group": group,
                "unique_shape_ids": shape_ids,
                "has_stops": True,
                "trip_ids": group_trip_ids,
                "route_color": route_color,
            }

    log_memory_usage("Fin build_combos")
    return combos


@memory_logger
def lambda_handler(event, context):
    """Preprocesa los archivos GTFS básicos para el procesamiento posterior."""
    log_memory_usage("Inicio de lambda_handler")

    # Generar ID único para esta ejecución
    execution_id = str(uuid.uuid4())

    # Obtener parámetros
    if isinstance(event, list) and len(event) > 0:
        event = event[0]
    elif isinstance(event, dict) and "body" in event:
        body = json.loads(event.get("body", "{}"))
        event = body

    P_EMPRESA = event.get("P_EMPRESA")
    P_VERSION = event.get("P_VERSION")
    P_CONTR = event.get("P_CONTR")

    # Acceder directamente como en el código original
    bucket = os.environ["S3_BUCKET"]

    print(
        f"Iniciando preprocesamiento: Explotación={P_EMPRESA}, Contrato={P_CONTR}, Versión={P_VERSION}, Ejecución={execution_id}"
    )

    try:
        # Cargar los datos necesarios con optimizaciones de memoria
        print("Cargando datos GTFS...")

        routes = read_csv_s3(
            bucket,
            f"GTFS/ROUTES/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/routes.txt",
            usecols=["route_id", "route_short_name", "route_color"],
            dtype={
                "route_id": object,
                "route_short_name": object,
                "route_color": object,
            },
        )
        log_memory_usage("Después de cargar routes")

        trips = read_csv_s3(
            bucket,
            f"GTFS/TRIPS/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/trips.txt",
            usecols=["route_id", "trip_id", "service_id", "shape_id", "trip_headsign"],
            dtype={
                "route_id": object,
                "trip_id": object,
                "service_id": object,
                "shape_id": object,
                "trip_headsign": object,
            },
        )
        log_memory_usage("Después de cargar trips")

        stop_times = read_csv_s3(
            bucket,
            f"GTFS/STOP_TIMES/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/stop_times.txt",
            usecols=[
                "trip_id",
                "stop_id",
                "stop_sequence",
                "arrival_time",
                "departure_time",
            ],
            dtype={
                "trip_id": object,
                "stop_id": object,
                "stop_sequence": object,
                "arrival_time": object,
                "departure_time": object,
            },
        )
        log_memory_usage("Después de cargar stop_times")

        stops = read_csv_s3(
            bucket,
            f"GTFS/STOPS/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/stops.txt",
            usecols=["stop_id", "stop_lat", "stop_lon", "stop_name"],
        )
        log_memory_usage("Después de cargar stops")

        # Intentar cargar shapes
        has_shapes = False
        try:
            shapes = read_csv_s3(
                bucket,
                f"GTFS/SHAPES/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/shapes.txt",
                usecols=[
                    "shape_id",
                    "shape_pt_lat",
                    "shape_pt_lon",
                    "shape_pt_sequence",
                ],
            )
            has_shapes = True
            log_memory_usage("Después de cargar shapes")
        except Exception as e:
            print(f"No se encontró archivo shapes.txt: {str(e)}")
            shapes = pd.DataFrame(
                columns=[
                    "shape_id",
                    "shape_pt_lat",
                    "shape_pt_lon",
                    "shape_pt_sequence",
                ]
            )

        # Normalización de tipos
        trips["trip_id"] = trips["trip_id"].astype(str)
        trips["route_id"] = trips["route_id"].astype(str)
        if "shape_id" in trips.columns:
            trips["shape_id"] = trips["shape_id"].fillna("").astype(str)

        stop_times["trip_id"] = stop_times["trip_id"].astype(str)
        stop_times["stop_id"] = stop_times["stop_id"].astype(str)

        stops["stop_id"] = stops["stop_id"].astype(str)

        if has_shapes:
            shapes["shape_id"] = shapes["shape_id"].astype(str)

        # Reset de índices como en el original
        stops = stops.reset_index(drop=True)
        stop_times = stop_times.reset_index(drop=True)
        if has_shapes:
            shapes = shapes.reset_index(drop=True)

        log_memory_usage("Después de normalizar tipos")

        # Construir combos como en el original
        combos = build_combos(trips, routes, stop_times)

        # Obtener rutas únicas y validar
        unique_routes = sorted(trips["route_id"].unique())
        print(f"Encontradas {len(unique_routes)} rutas únicas.")

        # Validar rutas como en el original
        valid_routes = [r for r in unique_routes if r in combos and combos[r]]
        if not valid_routes:
            print("No se encontraron combinaciones válidas.")
            raise Exception("No se encontraron combinaciones válidas.")

        # Directorio temporal para datos procesados con ID único
        temp_dir = f"GTFS_TEMP/preprocessed/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/{execution_id}"

        # Guardar archivos procesados
        write_csv_s3(routes, bucket, f"{temp_dir}/routes.csv")
        log_memory_usage("Después de guardar routes")

        write_csv_s3(trips, bucket, f"{temp_dir}/trips.csv")
        log_memory_usage("Después de guardar trips")

        write_csv_s3(stop_times, bucket, f"{temp_dir}/stop_times.csv")
        log_memory_usage("Después de guardar stop_times")

        write_csv_s3(stops, bucket, f"{temp_dir}/stops.csv")
        log_memory_usage("Después de guardar stops")

        if has_shapes:
            write_csv_s3(shapes, bucket, f"{temp_dir}/shapes.csv")
            log_memory_usage("Después de guardar shapes")

        # Guardar combos para uso posterior
        print("DEBUG - Verificando estructura de combos antes de serializar")
        for route_id in list(combos.keys())[:2]:
            print(f"DEBUG - Ruta {route_id} tiene {len(combos[route_id])} heads")
            for head, data in list(combos[route_id].items())[:1]:
                print(f"DEBUG - Head: {head}")
                print(f"DEBUG - Group type: {type(data['group'])}")
                print(f"DEBUG - Num trip_ids: {len(data['trip_ids'])}")
                if isinstance(data["group"], pd.DataFrame):
                    print(f"DEBUG - Group shape: {data['group'].shape}")
                    print(f"DEBUG - Group columns: {data['group'].columns.tolist()}")

        combo_data = {}
        for route_id, route_data in combos.items():
            combo_data[route_id] = {}
            for head, head_data in route_data.items():
                # Guardar información completa del grupo para reconstrucción
                if isinstance(head_data["group"], pd.DataFrame):
                    # Convertir el DataFrame a un formato serializable
                    group_data = []
                    for _, row in head_data["group"].iterrows():
                        row_dict = {}
                        for col in head_data["group"].columns:
                            # Convertir valores no serializables a string
                            row_dict[col] = str(row[col])
                        group_data.append(row_dict)
                else:
                    group_data = []

                combo_data[route_id][head] = {
                    "unique_shape_ids": head_data["unique_shape_ids"],
                    "has_stops": head_data["has_stops"],
                    "trip_ids": list(head_data["trip_ids"]),
                    "route_color": head_data["route_color"],
                    "group_data": group_data,  # Guardamos los datos del grupo
                    "group_columns": (
                        list(head_data["group"].columns)
                        if isinstance(head_data["group"], pd.DataFrame)
                        else []
                    ),
                }
        # Debugging para verificar estructura
        print("DEBUG - Estructura de combo_data antes de serializar:")
        for route_id in list(combo_data.keys())[:2]:  # Solo mostrar primeras 2 rutas
            print(f"DEBUG - Ruta {route_id}: {list(combo_data[route_id].keys())[:2]}")

        with StringIO() as json_buffer:
            json.dump(combo_data, json_buffer)
            json_buffer.seek(0)
            s3_client.put_object(
                Bucket=bucket,
                Key=f"{temp_dir}/combos.json",
                Body=json_buffer.getvalue(),
            )

        # Verificación de archivos
        print(f"DEBUG - Verificando archivo {temp_dir}/combos.json en S3...")
        try:
            meta = s3_client.head_object(Bucket=bucket, Key=f"{temp_dir}/combos.json")
            print(f"DEBUG - Archivo existe, tamaño: {meta['ContentLength']} bytes")
        except Exception as e:
            print(f"DEBUG - Error verificando archivo: {str(e)}")

        log_memory_usage("Después de guardar combos")

        # Liberar memoria
        del routes, trips, stop_times, stops, combos, combo_data
        if has_shapes:
            del shapes
        gc.collect()

        log_memory_usage("Fin del procesamiento")

        return {
            "status": "success",
            "execution_id": execution_id,
            "P_EMPRESA": P_EMPRESA,
            "P_VERSION": P_VERSION,
            "P_CONTR": P_CONTR,
            "temp_dir": temp_dir,
            "unique_routes": valid_routes,
            "has_shapes": has_shapes,
            "has_data": True,
        }

    except Exception as e:
        print(f"Error durante el preprocesamiento: {str(e)}")
        return {
            "status": "error",
            "execution_id": execution_id,
            "message": str(e),
            "P_EMPRESA": P_EMPRESA,
            "P_VERSION": P_VERSION,
            "P_CONTR": P_CONTR,
        }
