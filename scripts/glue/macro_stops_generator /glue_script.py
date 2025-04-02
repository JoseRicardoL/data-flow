"""Procesador de datos GTFS para generar archivos de macro_stops.

Este módulo procesa datos GTFS (General Transit Feed Specification) para generar
archivos de relación entre rutas, viajes, paradas y horarios para
visualización y análisis.
"""

import sys
import gc
import io
import uuid
import boto3
import logging
import functools
import pandas as pd
from awsglue.utils import getResolvedOptions


# Configuración de psutil
try:
    import psutil
except ImportError:
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
    import psutil

# Configuración básica
s3_client = boto3.client("s3")
s3 = boto3.resource("s3")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def log_memory_usage(label: str) -> float:
    """Registra el uso de memoria actual y lo devuelve en MB.

    Args:
        label: Etiqueta descriptiva para el log de memoria.

    Returns:
        Uso de memoria en MB.
    """
    gc.collect()
    memory_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    print(f"MEMORIA [{label}]: {memory_mb:.2f} MB")
    return memory_mb


def memory_logger(func):
    """Decorador que registra el uso de memoria antes y después de una función.

    Args:
        func: Función a decorar.

    Returns:
        Función decorada con registro de memoria.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log_memory_usage(f"Antes de {func.__name__}")
        result = func(*args, **kwargs)
        log_memory_usage(f"Después de {func.__name__}")
        return result

    return wrapper


# Sobrescribir pd.merge para monitoreo de memoria
_original_merge = pd.merge


@functools.wraps(_original_merge)
def merge_with_memory(*args, **kwargs):
    """Reemplazo de pd.merge que registra uso de memoria.

    Args:
        *args: Argumentos para pasar a pd.merge.
        **kwargs: Argumentos de palabra clave para pasar a pd.merge.

    Returns:
        DataFrame resultante del merge.
    """
    input_shape = getattr(args[0], "shape", "N/A") if args else "N/A"
    log_memory_usage(f"Antes del merge (input shape: {input_shape})")
    result = _original_merge(*args, **kwargs)
    result_shape = getattr(result, "shape", "N/A")
    log_memory_usage(f"Después del merge (result shape: {result_shape})")
    return result


pd.merge = merge_with_memory


def read_csv_s3(bucket, key, **kwargs):
    """Lee un archivo CSV desde S3.

    Args:
        bucket: Nombre del bucket S3.
        key: Ruta del archivo dentro del bucket.
        **kwargs: Argumentos adicionales para pd.read_csv.

    Returns:
        DataFrame con el contenido del CSV.
    """
    kwargs["low_memory"] = True

    dtype_params = {"route_id": str, "trip_id": str, "stop_id": str, "shape_id": str}

    if "dtype" in kwargs:
        kwargs["dtype"].update(dtype_params)
    else:
        kwargs["dtype"] = dtype_params

    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), encoding="utf8", **kwargs)


def write_csv_s3(df, bucket, key):
    """Guarda un DataFrame como CSV en S3.

    Args:
        df: DataFrame a guardar.
        bucket: Nombre del bucket S3.
        key: Ruta de destino dentro del bucket.
    """
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


def normalize_dataframe_ids(df, columns):
    """Normaliza columnas ID en un DataFrame para asegurar consistencia.

    Args:
        df: DataFrame a normalizar.
        columns: Lista de nombres de columnas a normalizar.

    Returns:
        DataFrame normalizado.
    """
    for col in columns:
        if col in df.columns:
            # Asegurar que la columna sea de tipo object
            if df[col].dtype != "object":
                df[col] = df[col].astype("object")

            # Usar map con lambda para preservar formato exacto
            df[col] = df[col].map(lambda x: str(x).strip())
    return df


def log_diagnostico_merge(df_left, df_right, col_join, label=""):
    """Realiza diagnóstico detallado antes de un merge para identificar problemas.

    Args:
        df_left: DataFrame izquierdo del merge.
        df_right: DataFrame derecho del merge.
        col_join: Columna de unión para el merge.
        label: Etiqueta descriptiva para el diagnóstico.

    Returns:
        Diccionario con estadísticas de diagnóstico o None si no aplica.
    """
    if col_join in df_left.columns and col_join in df_right.columns:
        values_left = set(df_left[col_join].unique())
        values_right = set(df_right[col_join].unique())

        common = values_left.intersection(values_right)
        only_left = values_left - values_right
        only_right = values_right - values_left

        print(f"DIAGNÓSTICO MERGE {label}:")
        print(f"- Valores únicos en izquierdo: {len(values_left)}")
        print(f"- Valores únicos en derecho: {len(values_right)}")
        print(f"- Valores comunes: {len(common)}")
        print(f"- Solo en izquierdo: {len(only_left)} valores")
        print(f"- Solo en derecho: {len(only_right)} valores")

        if len(common) == 0:
            print("⚠️ ALERTA: No hay valores comunes para el merge!")
            print(f"Muestra de valores izquierdos: {list(values_left)[:5]}")
            print(f"Muestra de valores derechos: {list(values_right)[:5]}")

        return {"common": common, "only_left": only_left, "only_right": only_right}
    return None


@memory_logger
def create_df_macro_stops_stream(
    trips,
    routes,
    stop_times,
    stops,
    P_EMPRESA=None,
    P_CONTR=None,
    P_VERSION=None,
    batch_size=5,
    execution_id=None,
    bucket=None,
):
    """Crea un dataframe de relación entre rutas, viajes, paradas y horarios.

    Procesa la información por lotes para optimizar el uso de memoria, escribiendo
    resultados temporales a S3 y realizando validaciones para garantizar integridad.

    Args:
        trips: DataFrame con información de viajes.
        routes: DataFrame con información de rutas.
        stop_times: DataFrame con información de horarios de paradas.
        stops: DataFrame con información de paradas.
        P_EMPRESA: Identificador de la empresa de transporte.
        P_CONTR: Identificador del contrato.
        P_VERSION: Versión de los datos.
        batch_size: Número de rutas a procesar por lote.
        execution_id: Identificador de ejecución para nombramiento de archivos.
        bucket: Nombre del bucket S3 para almacenamiento.

    Returns:
        Diccionario con información de procesamiento y archivos temporales.
    """
    print("Creando dataframe macro_stops con escritura directa a S3")
    print(
        f"INFO - Total trips: {len(trips)}, stop_times: {len(stop_times)}, stops: {len(stops)}, routes: {len(routes)}"
    )

    # Diagnóstico de datos iniciales
    print("DIAGNÓSTICO INICIAL DE DATOS:")
    print(f"- routes[route_id] únicos: {len(routes['route_id'].unique())}")
    print(f"- trips[route_id] únicos: {len(trips['route_id'].unique())}")

    # Verificar compatibilidad inicial entre trips y routes
    diagnostico_trips_routes = log_diagnostico_merge(
        trips, routes, "route_id", "TRIPS-ROUTES"
    )

    # Detección y corrección de incompatibilidades entre IDs
    if diagnostico_trips_routes and len(diagnostico_trips_routes["common"]) == 0:
        print(
            "⚠️ PROBLEMA CRÍTICO: No hay coincidencias entre route_id en trips y routes!"
        )
        print("Ejemplos de route_id en trips:", trips["route_id"].iloc[:5].tolist())
        print("Ejemplos de route_id en routes:", routes["route_id"].iloc[:5].tolist())

        # Intentar normalización básica
        print("Intentando normalizar datos para resolver el problema...")
        routes["route_id"] = routes["route_id"].astype(str).str.strip().str.lower()
        trips["route_id"] = trips["route_id"].astype(str).str.strip().str.lower()

        # Verificar resultado de normalización básica
        diagnostico_trips_routes = log_diagnostico_merge(
            trips, routes, "route_id", "POST-NORMALIZACIÓN"
        )

        if diagnostico_trips_routes and len(diagnostico_trips_routes["common"]) == 0:
            print(
                "⚠️ El problema persiste después de la normalización. Verificando más detalles..."
            )
            # Inspección más profunda
            print("Tipos de datos:")
            print(
                f"- trips[route_id]: {trips['route_id'].dtype}, ejemplo: '{trips['route_id'].iloc[0]}'"
            )
            print(
                f"- routes[route_id]: {routes['route_id'].dtype}, ejemplo: '{routes['route_id'].iloc[0]}'"
            )

            # Verificar si hay diferencias por padding con ceros (común en IDs de transporte)
            trips_padded = trips.copy()
            routes_padded = routes.copy()

            # Intentar con padding de 3 dígitos
            trips_padded["route_id_padded"] = trips_padded["route_id"].str.zfill(3)
            routes_padded["route_id_padded"] = routes_padded["route_id"].str.zfill(3)

            diagnostico_padded = log_diagnostico_merge(
                trips_padded, routes_padded, "route_id_padded", "CON-PADDING"
            )

            if diagnostico_padded and len(diagnostico_padded["common"]) > 0:
                print("✓ Se encontraron coincidencias usando padding con ceros!")
                # Usar las versiones con padding
                trips["route_id"] = trips["route_id"].str.zfill(3)
                routes["route_id"] = routes["route_id"].str.zfill(3)
                print("Datos normalizados con padding de ceros.")

    # Normalización de tipos para garantizar compatibilidad en las operaciones de merge
    trips = normalize_dataframe_ids(trips, ["trip_id", "route_id", "shape_id"])
    routes = normalize_dataframe_ids(routes, ["route_id"])
    stop_times = normalize_dataframe_ids(stop_times, ["trip_id", "stop_id"])
    stops = normalize_dataframe_ids(stops, ["stop_id"])

    # Log para diagnóstico de tipos de datos
    print(f"INFO - Tipo de datos trip_id en trips: {trips['trip_id'].dtype}")
    print(f"INFO - Tipo de datos trip_id en stop_times: {stop_times['trip_id'].dtype}")
    print(f"INFO - Tipo de datos stop_id en stop_times: {stop_times['stop_id'].dtype}")
    print(f"INFO - Tipo de datos stop_id en stops: {stops['stop_id'].dtype}")
    print(f"INFO - Tipo de datos route_id en routes: {routes['route_id'].dtype}")
    print(f"INFO - Tipo de datos route_id en trips: {trips['route_id'].dtype}")

    unique_routes = trips["route_id"].unique()
    temp_files = []
    total_rows = 0

    # Usar el execution_id provisto o generar uno nuevo
    batch_uuid = execution_id or str(uuid.uuid4())
    base_temp_path = f"GTFS_TEMP/MACRO_STOPS/{batch_uuid}/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}"

    # Diccionario para almacenar todas las líneas que pasan por cada parada
    all_stop_lines = {}

    # Definición de todas las columnas esperadas con sus tipos de datos
    macro_stops_columns = {
        "route_id": "string",
        "trip_id": "string",
        "service_id": "string",
        "shape_id": "string",
        "trip_headsign": "string",
        "trip_short_name": "string",
        "direction_id": "bigint",
        "block_id": "string",
        "arrival_time": "string",
        "stop_id": "bigint",
        "stop_sequence": "bigint",
        "stop_headsign": "string",
        "continuous_pickup_x": "bigint",
        "continuous_drop_off_x": "bigint",
        "timepoint": "bigint",
        "stop_code": "bigint",
        "stop_name": "string",
        "stop_desc": "string",
        "stop_lat": "double",
        "stop_lon": "double",
        "zone_id": "string",
        "stop_url": "string",
        "location_type": "bigint",
        "parent_station": "string",
        "stop_timezone": "string",
        "wheelchair_boarding": "string",
        "level_id": "string",
        "platform_code": "string",
        "agency_id": "string",
        "route_short_name": "string",
        "route_long_name": "string",
        "route_type": "bigint",
        "route_url": "string",
        "route_color": "string",
        "route_sort_order": "string",
        "continuous_pickup_y": "string",
        "continuous_drop_off_y": "string",
        "final_color": "string",
        "stop_id_2": "string",
        "stop_name_2": "string",
        "route_sn_trip_hs": "string",
        "lines_by_stop": "string",
        "departure_time": "string",
        "pickup_type": "string",
        "drop_off_type": "string",
        "route_desc": "string",
        "route_text_color": "string",
        "consec_na": "string",
        "wheelchair_accessible": "string",
        "bikes_allowed": "string",
    }

    # Procesamiento por lotes para reducir consumo de memoria
    for i in range(0, len(unique_routes), batch_size):
        batch_routes = unique_routes[i : i + batch_size]
        print(
            f"Procesando lote {i//batch_size + 1} de rutas para df_macro_stops ({len(batch_routes)} rutas)"
        )
        print(f"Route IDs en el lote actual: {batch_routes}")
        print(
            f"¿Cuántas de estas rutas están en routes? {sum(routes['route_id'].isin(batch_routes))}"
        )

        # Filtrar viajes de las rutas actuales
        batch_trips = trips[trips["route_id"].isin(batch_routes)].copy()

        if batch_trips.empty:
            print(f"No hay viajes para las rutas: {batch_routes}")
            continue

        # Obtener horarios relevantes para estos viajes
        trip_ids = set(batch_trips["trip_id"])
        batch_stop_times = stop_times[stop_times["trip_id"].isin(trip_ids)].copy()

        if batch_stop_times.empty:
            print(f"No hay horarios para los viajes de las rutas: {batch_routes}")
            continue

        # 1. Unir viajes con horarios
        log_diagnostico_merge(
            batch_trips, batch_stop_times, "trip_id", "BATCH-TRIPS-STOP_TIMES"
        )
        batch_df = batch_trips.merge(batch_stop_times, on="trip_id", how="inner")
        print(f"Merge trips-stop_times para lote: {len(batch_df)} filas")

        if batch_df.empty:
            continue

        # 2. Incorporar información de paradas
        log_diagnostico_merge(batch_df, stops, "stop_id", "BATCH-DF-STOPS")
        batch_df = batch_df.merge(stops, on="stop_id", how="inner")
        print(f"Merge con stops para lote: {len(batch_df)} filas")

        if batch_df.empty:
            print(f"No hay coincidencias con paradas para las rutas: {batch_routes}")
            continue

        # 3. Incorporar información de rutas
        batch_routes_df = routes[routes["route_id"].isin(batch_routes)].copy()

        # Verificación detallada del merge
        print(f"Detalles de batch_routes_df: {len(batch_routes_df)} filas")
        if len(batch_routes_df) == 0:
            print("⚠️ ALERTA: No se encontraron rutas en routes para este lote!")
            print(f"Batch routes: {batch_routes}")
            print(f"Routes disponibles: {routes['route_id'].unique()[:10]}")

            # Usar left join para identificar problemas específicos
            temp_merge = batch_df.merge(routes, on="route_id", how="left")
            missing_info = temp_merge[temp_merge["route_short_name"].isna()]
            if not missing_info.empty:
                print(f"Rutas sin coincidencia: {missing_info['route_id'].unique()}")

        # Normalización para asegurar compatibilidad
        batch_routes_df["route_id"] = (
            batch_routes_df["route_id"].astype(str).str.strip()
        )
        batch_df["route_id"] = batch_df["route_id"].astype(str).str.strip()

        # Diagnóstico detallado antes del merge
        log_diagnostico_merge(batch_df, batch_routes_df, "route_id", "BATCH-DF-ROUTES")

        # Procesamiento de colores para representación visual
        def safe_color(x):
            """Normaliza códigos de color y aplica validaciones de seguridad.

            Args:
                x: Valor de color a normalizar.

            Returns:
                String con color normalizado en formato hexadecimal.
            """
            if pd.isna(x) or str(x).lower() == "nan" or not x:
                return "#000000"  # Color negro por defecto
            try:
                color = str(x).lower().strip()
                if not color.startswith("#"):
                    color = "#" + color
                if len(color) == 4:
                    color = "#" + "".join(c + c for c in color[1:])
                if len(color) != 7:
                    return "#000000"
                if not all(c in "0123456789abcdef#" for c in color):
                    return "#000000"
                return color
            except:
                return "#000000"

        if not batch_routes_df.empty:
            batch_routes_df["Final_Color"] = batch_routes_df["route_color"].apply(
                safe_color
            )

            # Usar left join para diagnosticar problemas
            batch_df = batch_df.merge(batch_routes_df, on="route_id", how="left")

            # Verificar si se perdieron datos en el merge
            missing_routes_data = batch_df[batch_df["route_short_name"].isna()]
            if not missing_routes_data.empty:
                print(
                    f"⚠️ Se encontraron {len(missing_routes_data)} filas sin datos de rutas:"
                )
                print(
                    f"Route IDs sin coincidencia: {missing_routes_data['route_id'].unique()}"
                )

                # Recuperar solo las filas con datos completos
                batch_df = batch_df.dropna(subset=["route_short_name"])

            print(f"Merge con routes para lote: {len(batch_df)} filas")

            if batch_df.empty:
                print(
                    f"El merge con routes dejó un resultado vacío para las rutas: {batch_routes}"
                )
                continue

            # Conversiones y campos adicionales para visualización
            batch_df["route_short_name"] = batch_df["route_short_name"].astype(str)
            batch_df["stop_sequence"] = batch_df["stop_sequence"].astype(str)
            batch_df["stop_id_2"] = "Id: " + batch_df["stop_id"]  # Para visualización
            batch_df["stop_name_2"] = (
                " Nombre: " + batch_df["stop_name"]
            )  # Para visualización
            batch_df["Route_SN_Trip_HS"] = batch_df["route_short_name"] + batch_df[
                "trip_headsign"
            ].fillna("").astype(
                str
            )  # Identificador compuesto

            # Recolección de líneas por parada para reportes y visualizaciones
            for stop in batch_df["stop_id"].unique():
                lines = (
                    batch_df[batch_df["stop_id"] == stop]["route_short_name"]
                    .unique()
                    .tolist()
                )
                if stop in all_stop_lines:
                    all_stop_lines[stop].extend(lines)
                else:
                    all_stop_lines[stop] = lines

            # Placeholder - se rellenará en la fase de combinación final
            batch_df["lines_by_stop"] = ""

            # Normalización de nombres de columnas para consistencia
            if (
                "Final_Color" in batch_df.columns
                and "final_color" not in batch_df.columns
            ):
                batch_df.rename(columns={"Final_Color": "final_color"}, inplace=True)

            if (
                "Route_SN_Trip_HS" in batch_df.columns
                and "route_sn_trip_hs" not in batch_df.columns
            ):
                batch_df.rename(
                    columns={"Route_SN_Trip_HS": "route_sn_trip_hs"}, inplace=True
                )

            # Detección y eliminación de columnas duplicadas
            if batch_df.columns.duplicated().any():
                print(
                    f"ALERTA: Columnas duplicadas detectadas: {batch_df.columns[batch_df.columns.duplicated()].tolist()}"
                )
                batch_df = batch_df.loc[:, ~batch_df.columns.duplicated(keep="first")]

            # Añadir columnas faltantes y asegurar tipos correctos
            for col, dtype in macro_stops_columns.items():
                if col not in batch_df.columns:
                    batch_df[col] = None

                    if dtype == "string":
                        batch_df[col] = batch_df[col].astype("object")
                    elif dtype == "double":
                        batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce")
                    elif dtype == "bigint":
                        batch_df[col] = pd.to_numeric(
                            batch_df[col], errors="coerce"
                        ).astype("Int64")

            # Guardar lote procesado a S3
            temp_file_path = f"{base_temp_path}/batch_{i//batch_size + 1}.csv"
            write_csv_s3(batch_df, bucket, temp_file_path)
            temp_files.append(temp_file_path)
            total_rows += len(batch_df)

        # Liberar recursos para el siguiente lote
        del batch_trips, batch_stop_times
        if "batch_routes_df" in locals():
            del batch_routes_df
        if "batch_df" in locals():
            del batch_df
        gc.collect()

    # Eliminar duplicados de líneas por parada
    for stop in all_stop_lines:
        all_stop_lines[stop] = list(set(all_stop_lines[stop]))

    print(f"Procesados {total_rows} filas en total para df_macro_stops")
    print(f"Archivos temporales creados: {len(temp_files)}")

    # Resultados para la fase de combinación
    return {
        "temp_files": temp_files,
        "total_rows": total_rows,
        "stop_lines": all_stop_lines,  # Líneas por parada para el archivo final
        "columns_info": macro_stops_columns,
        "base_temp_path": base_temp_path,
    }


def combine_temp_files(temp_info, final_path, entity_type, bucket):
    """Combina archivos temporales en un solo archivo final con estructura coherente.

    Args:
        temp_info: Diccionario con información de archivos temporales.
        final_path: Ruta de destino para el archivo final.
        entity_type: Tipo de entidad ('MACRO_STOPS' u otro).
        bucket: Nombre del bucket S3.
    """
    temp_files = temp_info["temp_files"]

    # Configuración de tipos de datos para todos los IDs
    id_dtypes = {
        "route_id": str,
        "trip_id": str,
        "stop_id": str,
        "shape_id": str,
        "service_id": str,
    }

    print(f"Combinando {len(temp_files)} archivos temporales en {final_path}")

    # Verificar si hay archivos temporales
    if not temp_files:
        print(f"ALERTA: No hay archivos temporales para combinar en {final_path}")
        return

    # Preservar información de columnas para uso posterior
    columns_info = temp_info.get("columns_info", {})

    if entity_type == "MACRO_STOPS":
        all_stop_lines = temp_info["stop_lines"]
        stop_lines_mapping = {}

        # Crear mapeo final de líneas por parada
        for stop, lines in all_stop_lines.items():
            stop_lines_mapping[stop] = ", ".join(sorted(set(lines)))

        # Procesar cada archivo temporal
        for i, temp_file in enumerate(temp_files):
            try:
                content = (
                    s3_client.get_object(Bucket=bucket, Key=temp_file)["Body"]
                    .read()
                    .decode("utf-8")
                )

                df = pd.read_csv(io.StringIO(content), dtype=id_dtypes)

                df["lines_by_stop"] = df["stop_id"].map(
                    lambda x: stop_lines_mapping.get(x, "")
                )

                if df.columns.duplicated().any():
                    print(
                        f"ALERTA: Columnas duplicadas en archivo {temp_file}: {df.columns[df.columns.duplicated()].tolist()}"
                    )
                    df = df.loc[:, ~df.columns.duplicated(keep="first")]
                    print(f"Columnas después de eliminar duplicados: {len(df.columns)}")

                if i == 0:
                    write_csv_s3(df, bucket, final_path)
                else:
                    # Añadir datos sin encabezados para continuar el archivo
                    with io.StringIO() as csv_buffer:
                        df.to_csv(csv_buffer, index=False, header=False)
                        csv_buffer.seek(0)
                        append_content = csv_buffer.getvalue()

                        # Obtener el contenido existente
                        existing_content = (
                            s3_client.get_object(Bucket=bucket, Key=final_path)["Body"]
                            .read()
                            .decode("utf-8")
                        )

                        # Combinar preservando la estructura
                        s3_client.put_object(
                            Bucket=bucket,
                            Key=final_path,
                            Body=existing_content + append_content,
                            ACL="bucket-owner-full-control",
                        )

                del df
                gc.collect()

            except Exception as e:
                logging.error(f"Error procesando archivo {temp_file}: {str(e)}")
    else:
        # Fase 1: Determinar el esquema unificado de columnas
        all_columns = set()
        for temp_file in temp_files:
            try:
                content = (
                    s3_client.get_object(Bucket=bucket, Key=temp_file)["Body"]
                    .read()
                    .decode("utf-8")
                )
                df_sample = pd.read_csv(io.StringIO(content), nrows=1)
                all_columns.update(df_sample.columns)
                del df_sample
            except Exception as e:
                logging.error(f"Error al analizar esquema de {temp_file}: {str(e)}")

        # Fase 2: Establecer orden de columnas consistente
        if columns_info:
            ordered_columns = [col for col in columns_info.keys() if col in all_columns]
            ordered_columns.extend(
                [col for col in all_columns if col not in ordered_columns]
            )
        else:
            ordered_columns = sorted(all_columns)

        print(
            f"Esquema unificado: {len(ordered_columns)} columnas: {ordered_columns[:5]}..."
        )

        # Fase 3: Integración secuencial de archivos temporales
        total_rows_processed = 0

        for i, temp_file in enumerate(temp_files):
            try:
                print(f"Integrando archivo {i+1}/{len(temp_files)}: {temp_file}")
                content = (
                    s3_client.get_object(Bucket=bucket, Key=temp_file)["Body"]
                    .read()
                    .decode("utf-8")
                )

                df = pd.read_csv(io.StringIO(content))

                # Normalización de esquema para consistencia
                for col in ordered_columns:
                    if col not in df.columns:
                        df[col] = None

                # Aplicar orden unificado de columnas
                df = df[ordered_columns]

                # Escritura incremental manteniendo integridad estructural
                if i == 0:
                    write_csv_s3(df, bucket, final_path)
                else:
                    with io.StringIO() as csv_buffer:
                        df.to_csv(csv_buffer, index=False, header=False)
                        csv_buffer.seek(0)
                        append_content = csv_buffer.getvalue()

                        if append_content.strip():
                            existing_content = (
                                s3_client.get_object(Bucket=bucket, Key=final_path)[
                                    "Body"
                                ]
                                .read()
                                .decode("utf-8")
                            )
                            s3_client.put_object(
                                Bucket=bucket,
                                Key=final_path,
                                Body=existing_content + append_content,
                            )

                total_rows_processed += len(df)
                print(
                    f"Progreso: {len(df)} filas añadidas. Acumulado: {total_rows_processed} filas"
                )

                del df
                gc.collect()

            except Exception as e:
                logging.error(f"Error en procesamiento de {temp_file}: {str(e)}")

        # Fase 4: Verificación de integridad estructural final
        try:
            final_content = (
                s3_client.get_object(Bucket=bucket, Key=final_path)["Body"]
                .read()
                .decode("utf-8")
            )
            first_line = final_content.split("\n", 1)[0]

            if not any(col in first_line for col in ordered_columns[:3]):
                logging.warning(
                    "Detectada posible inconsistencia estructural en el archivo final"
                )

                header_line = ",".join(ordered_columns) + "\n"
                data_lines = "\n".join(final_content.splitlines()[1:])
                s3_client.put_object(
                    Bucket=bucket, Key=final_path, Body=header_line + data_lines
                )
                logging.info(
                    "Estructura normalizada para compatibilidad con servicios de análisis"
                )
        except Exception as e:
            logging.error(f"Error en validación estructural final: {str(e)}")

    print("Limpiando archivos temporales")
    base_temp_path = temp_info["base_temp_path"]

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_temp_path)
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket, Delete={"Objects": objects_to_delete, "Quiet": True}
            )
    except Exception as e:
        logging.warning(f"Error limpiando archivos temporales: {str(e)}")

    print(f"Archivo final guardado en: {final_path}")


def main():

    # Obtener parámetros usando el bronze_bucket existente en lugar de duplicar
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "P_EMPRESA",
            "P_VERSION",
            "P_CONTR",
            "temp_dir",
            "execution_id",
            "bronze_bucket",
        ],
    )

    P_EMPRESA = args["P_EMPRESA"]
    P_VERSION = args["P_VERSION"]
    P_CONTR = args["P_CONTR"]
    temp_dir = args["temp_dir"]
    execution_id = args["execution_id"]
    bucket = args["bronze_bucket"]

    log_memory_usage("Inicio de MacroStopsGenerator")

    try:
        # Leer archivos preprocesados
        trips = read_csv_s3(bucket, f"{temp_dir}/trips.csv")
        routes = read_csv_s3(bucket, f"{temp_dir}/routes.csv")
        stop_times = read_csv_s3(bucket, f"{temp_dir}/stop_times.csv")
        stops = read_csv_s3(bucket, f"{temp_dir}/stops.csv")

        # Mostrar información básica de los archivos leídos
        print("INFORMACIÓN DE ARCHIVOS CARGADOS:")
        print(f"- trips: {len(trips)} filas, columnas: {trips.columns.tolist()}")
        print(f"- routes: {len(routes)} filas, columnas: {routes.columns.tolist()}")
        print(
            f"- stop_times: {len(stop_times)} filas, columnas: {stop_times.columns.tolist()}"
        )
        print(f"- stops: {len(stops)} filas, columnas: {stops.columns.tolist()}")

        # Verificación preventiva de integridad de datos
        if routes.empty:
            print("⚠️ ERROR: El DataFrame de rutas está vacío. No se puede continuar.")
            raise ValueError("El DataFrame de rutas está vacío")

        if trips.empty:
            print("⚠️ ERROR: El DataFrame de viajes está vacío. No se puede continuar.")
            raise ValueError("El DataFrame de viajes está vacío")

        # Verificar si hay rutas en común entre trips y routes
        trips_route_ids = set(trips["route_id"].unique())
        routes_route_ids = set(routes["route_id"].unique())
        common_routes = trips_route_ids.intersection(routes_route_ids)

        print(
            f"Rutas en trips: {len(trips_route_ids)}, Rutas en routes: {len(routes_route_ids)}"
        )
        print(f"Rutas en común: {len(common_routes)}")

        if len(common_routes) == 0:
            print("⚠️ ERROR CRÍTICO: No hay rutas en común entre trips y routes!")
            print("Muestra de route_id en trips:", list(trips_route_ids)[:5])
            print("Muestra de route_id en routes:", list(routes_route_ids)[:5])

            # Intento de normalización avanzada
            print("Intentando normalización avanzada de IDs...")

            # Normalizar trips
            trips["route_id_orig"] = trips["route_id"].copy()
            trips["route_id"] = trips["route_id"].astype(str).str.strip().str.lower()

            # Normalizar routes
            routes["route_id_orig"] = routes["route_id"].copy()
            routes["route_id"] = routes["route_id"].astype(str).str.strip().str.lower()

            # Verificar si se resolvió el problema
            trips_route_ids = set(trips["route_id"].unique())
            routes_route_ids = set(routes["route_id"].unique())
            common_routes = trips_route_ids.intersection(routes_route_ids)

            print(
                f"Después de normalización básica - Rutas en común: {len(common_routes)}"
            )

            # Si aún no hay coincidencias, probar con padding de ceros
            if len(common_routes) == 0:
                print("Intentando normalización con padding de ceros...")

                # Aplicar padding de 3 dígitos (común en IDs de transporte)
                trips["route_id"] = trips["route_id"].str.zfill(3)
                routes["route_id"] = routes["route_id"].str.zfill(3)

                trips_route_ids = set(trips["route_id"].unique())
                routes_route_ids = set(routes["route_id"].unique())
                common_routes = trips_route_ids.intersection(routes_route_ids)

                print(f"Después de padding - Rutas en común: {len(common_routes)}")

                # Si sigue sin haber coincidencias, intentar con otras estrategias comunes
                if len(common_routes) == 0:
                    print("Intentando normalización quitando ceros a la izquierda...")

                    # Revertir al valor original y probar quitando ceros a la izquierda
                    trips["route_id"] = (
                        trips["route_id_orig"].astype(str).str.strip().str.lstrip("0")
                    )
                    routes["route_id"] = (
                        routes["route_id_orig"].astype(str).str.strip().str.lstrip("0")
                    )

                    trips_route_ids = set(trips["route_id"].unique())
                    routes_route_ids = set(routes["route_id"].unique())
                    common_routes = trips_route_ids.intersection(routes_route_ids)

                    print(
                        f"Después de quitar ceros - Rutas en común: {len(common_routes)}"
                    )

            # Eliminar columnas auxiliares
            if "route_id_orig" in trips.columns:
                trips.drop("route_id_orig", axis=1, inplace=True)
            if "route_id_orig" in routes.columns:
                routes.drop("route_id_orig", axis=1, inplace=True)

            if len(common_routes) == 0:
                print(
                    "⚠️ ALERTA: No se pudieron encontrar rutas en común después de múltiples intentos de normalización."
                )
                print(
                    "Continuando con el proceso, pero es probable que no se generen resultados."
                )

        macro_stops_info = create_df_macro_stops_stream(
            trips,
            routes,
            stop_times,
            stops,
            P_EMPRESA,
            P_CONTR,
            P_VERSION,
            batch_size=5,
            execution_id=execution_id,
            bucket=bucket,
        )

        output_path_macro_stops = f"MAPS/MACRO_STOPS/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/{P_VERSION}_macro_stops"

        # Verificar si se generaron archivos temporales para combinar
        if not macro_stops_info["temp_files"]:
            print("⚠️ ALERTA: No se generaron archivos temporales para combinar.")
            print("Verificando si se deben generar archivos de emergencia...")

            # Si no hay archivos temporales, intentar crear uno de emergencia
            # con los datos que tenemos para evitar fallo total
            if len(common_routes) > 0:
                print("Generando archivo de emergencia con las rutas disponibles...")

                # Usar solo rutas en común
                emergency_routes = list(common_routes)[
                    :1
                ]  # Tomar solo una ruta para prueba

                # Repetir el proceso con solo una ruta
                emergency_trips = trips[trips["route_id"].isin(emergency_routes)].copy()
                emergency_stops_info = create_df_macro_stops_stream(
                    emergency_trips,
                    routes,
                    stop_times,
                    stops,
                    P_EMPRESA,
                    P_CONTR,
                    P_VERSION,
                    batch_size=1,
                    execution_id=f"{execution_id}_emergency",
                    bucket=bucket,
                )

                # Si se generó al menos un archivo, usar eso
                if emergency_stops_info["temp_files"]:
                    print("✓ Se generó archivo de emergencia. Usando estos datos.")
                    macro_stops_info = emergency_stops_info
                else:
                    print("⚠️ No se pudo generar archivo de emergencia.")

        combine_temp_files(
            macro_stops_info, output_path_macro_stops, "MACRO_STOPS", bucket
        )

        log_memory_usage("Fin de MacroStopsGenerator")

        # Verificación final
        if macro_stops_info["total_rows"] > 0:
            print(
                f"Proceso MacroStopsGenerator completado con éxito. Archivo generado: {output_path_macro_stops}"
            )
            print(f"Total de filas procesadas: {macro_stops_info['total_rows']}")
        else:
            print(
                f"⚠️ Proceso MacroStopsGenerator completado, pero no se generaron datos. Verifique los archivos de entrada."
            )

    except Exception as e:
        logger.error(f"Error en MacroStopsGenerator: {str(e)}")
        # Información de diagnóstico extendida en caso de error
        print("DIAGNÓSTICO DE ERROR:")
        print(f"Tipo de error: {type(e).__name__}")
        print(f"Mensaje de error: {str(e)}")

        # Intentar proporcionar información útil sobre el contexto del error
        if "trips" in locals() and "routes" in locals():
            print(
                f"Estado de datos: trips={len(trips) if not trips.empty else 'vacío'}, routes={len(routes) if not routes.empty else 'vacío'}"
            )
            if not trips.empty and not routes.empty:
                print(
                    f"Ejemplo route_id en trips: {trips['route_id'].iloc[0] if len(trips) > 0 else 'N/A'}"
                )
                print(
                    f"Ejemplo route_id en routes: {routes['route_id'].iloc[0] if len(routes) > 0 else 'N/A'}"
                )

                # Verificar si hay rutas en común
                trips_routes = set(trips["route_id"].unique())
                routes_routes = set(routes["route_id"].unique())
                common = trips_routes.intersection(routes_routes)
                print(
                    f"Rutas en común: {len(common)} de {len(trips_routes)} en trips y {len(routes_routes)} en routes"
                )

        raise e


if __name__ == "__main__":

    try:
        main()
        print("Ejecución completada correctamente.")
    except Exception as e:
        print(f"Error en la ejecución principal: {str(e)}")
        sys.exit(1)
