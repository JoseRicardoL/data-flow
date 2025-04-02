"""Procesador de datos GTFS para generar archivos de macro optimizados.

Este módulo procesa datos GTFS (General Transit Feed Specification) para generar
archivos de relación entre rutas, viajes y shapes, optimizados para visualización 
y análisis. Implementa técnicas de procesamiento por lotes y optimización de memoria 
para manejar grandes volúmenes de datos en AWS Glue.
"""

import sys
import gc
import io
import uuid
import boto3
import psutil
import logging
import functools
import pandas as pd
from awsglue.utils import getResolvedOptions


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
            # Convertir a string, eliminar espacios y normalizar
            df[col] = df[col].astype(str).str.strip()
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
def create_df_macro_stream(
    trips,
    routes,
    shapes=None,
    P_EMPRESA=None,
    P_CONTR=None,
    P_VERSION=None,
    batch_size=5,
    execution_id=None,
    bucket=None,
):
    """Crea un dataframe macro con rutas, viajes y formas procesando por lotes.

    Procesa la información por lotes para optimizar el uso de memoria, escribiendo
    resultados temporales a S3 y realizando validaciones para garantizar integridad.

    Args:
        trips: DataFrame con información de viajes.
        routes: DataFrame con información de rutas.
        shapes: DataFrame con información de formas (opcional).
        P_EMPRESA: Identificador de la empresa de transporte.
        P_CONTR: Identificador del contrato.
        P_VERSION: Versión de los datos.
        batch_size: Número de rutas a procesar por lote.
        execution_id: Identificador de ejecución para nombramiento de archivos.
        bucket: Nombre del bucket S3 para almacenamiento.

    Returns:
        Diccionario con información de procesamiento y archivos temporales.
    """
    print("Creando dataframe macro con escritura directa a S3")
    print(
        f"INFO - Total trips: {len(trips)}, routes: {len(routes)}, shapes: {len(shapes) if shapes is not None else 'No disponible'}"
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

    if shapes is not None and not shapes.empty:
        shapes = normalize_dataframe_ids(shapes, ["shape_id"])
        print(f"INFO - Tipo de datos shape_id en shapes: {shapes['shape_id'].dtype}")

    # Log para diagnóstico de tipos de datos
    print(f"INFO - Tipo de datos trip_id en trips: {trips['trip_id'].dtype}")
    print(f"INFO - Tipo de datos route_id en routes: {routes['route_id'].dtype}")
    print(f"INFO - Tipo de datos route_id en trips: {trips['route_id'].dtype}")
    if "shape_id" in trips.columns:
        print(f"INFO - Tipo de datos shape_id en trips: {trips['shape_id'].dtype}")

    unique_routes = trips["route_id"].unique()
    temp_files = []
    total_rows = 0

    # Usar el execution_id provisto o generar uno nuevo
    batch_uuid = execution_id or str(uuid.uuid4())
    base_temp_path = f"GTFS_TEMP/MACRO/{batch_uuid}/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}"

    # Definir columnas esperadas y sus tipos
    macro_columns = {
        "shape_id": "string",
        "final_color": "string",
        "route_short_name": "string",
        "trip_headsign": "string",
        "shape_pt_lat": "double",
        "shape_pt_lon": "double",
        "shape_pt_sequence": "bigint",
        "shape_dist_traveled": "double",
        "route_id": "string",
        "trip_id": "string",
        "service_id": "string",
        "trip_short_name": "string",
        "direction_id": "bigint",
        "block_id": "string",
        "agency_id": "string",
        "route_long_name": "string",
        "route_desc": "string",
        "route_type": "bigint",
        "route_url": "string",
        "route_color": "string",
        "route_text_color": "string",
        "route_sort_order": "string",
        "continuous_pickup": "string",
        "continuous_drop_off": "string",
    }

    # Procesamiento por lotes para reducir consumo de memoria
    for i in range(0, len(unique_routes), batch_size):
        batch_routes = unique_routes[i : i + batch_size]
        print(
            f"Procesando lote {i//batch_size + 1} de rutas para df_macro ({len(batch_routes)} rutas)"
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

        # Solo necesitamos las rutas de este lote
        batch_routes_df = routes[routes["route_id"].isin(batch_routes)].copy()

        # Verificación detallada del merge
        print(f"Detalles de batch_routes_df: {len(batch_routes_df)} filas")
        if len(batch_routes_df) == 0:
            print("⚠️ ALERTA: No se encontraron rutas en routes para este lote!")
            print(f"Batch routes: {batch_routes}")
            print(f"Routes disponibles: {routes['route_id'].unique()[:10]}")

            # Usar left join para identificar problemas específicos
            temp_merge = batch_trips[["route_id"]].merge(
                routes[["route_id", "route_short_name"]], on="route_id", how="left"
            )
            missing_info = temp_merge[temp_merge["route_short_name"].isna()]
            if not missing_info.empty:
                print(f"Rutas sin coincidencia: {missing_info['route_id'].unique()}")

            # Intentar continuar con las rutas disponibles
            continue

        # Normalización para asegurar compatibilidad
        batch_routes_df["route_id"] = (
            batch_routes_df["route_id"].astype(str).str.strip()
        )
        batch_trips["route_id"] = batch_trips["route_id"].astype(str).str.strip()
        batch_trips["trip_id"] = batch_trips["trip_id"].astype(str).str.strip()

        if "shape_id" in batch_trips.columns:
            batch_trips["shape_id"] = (
                batch_trips["shape_id"].fillna("").astype(str).str.strip()
            )

        # Procesamiento de colores
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

        batch_routes_df["Final_Color"] = batch_routes_df["route_color"].apply(
            safe_color
        )
        batch_routes_df["Final_Color"] = batch_routes_df["Final_Color"].astype(str)

        # Diagnóstico detallado antes del merge
        log_diagnostico_merge(
            batch_trips, batch_routes_df, "route_id", "BATCH-TRIPS-ROUTES"
        )

        # Primer merge - trips con routes
        df_macro_part = batch_trips.merge(batch_routes_df, on="route_id", how="inner")
        print(f"Merge trips-routes para lote: {len(df_macro_part)} filas")

        if df_macro_part.empty:
            print(
                f"El merge trips-routes dejó un resultado vacío para las rutas: {batch_routes}"
            )
            continue

        df_macro_part["route_short_name"] = df_macro_part["route_short_name"].astype(
            str
        )

        # Procesamiento de shapes
        if (
            shapes is not None
            and not shapes.empty
            and "shape_id" in df_macro_part.columns
        ):
            try:
                # Obtener solo las shapes necesarias para este lote
                shape_ids = set(df_macro_part["shape_id"].dropna().unique())
                if shape_ids:
                    batch_shapes = shapes[shapes["shape_id"].isin(shape_ids)].copy()
                    batch_shapes["shape_id"] = (
                        batch_shapes["shape_id"].fillna("").astype(str).str.strip()
                    )

                    # Verificar si hay shapes_ids comunes
                    log_diagnostico_merge(
                        df_macro_part, batch_shapes, "shape_id", "DF_MACRO-SHAPES"
                    )
                    common_shapes = set(df_macro_part["shape_id"]) & set(
                        batch_shapes["shape_id"]
                    )

                    if common_shapes:
                        # PRIMERO: Unir con shapes
                        df_macro_part = pd.merge(
                            df_macro_part, batch_shapes, on="shape_id", how="inner"
                        )
                        print(f"Merge con shapes para lote: {len(df_macro_part)} filas")

                        # SEGUNDO: Filtrar columnas y eliminar duplicados
                        df_macro_filtered = df_macro_part[
                            [
                                "shape_id",
                                "Final_Color",
                                "route_short_name",
                                "trip_headsign",
                            ]
                        ].drop_duplicates()

                        # TERCERO: Volver a unir con shapes
                        log_diagnostico_merge(
                            df_macro_filtered,
                            batch_shapes,
                            "shape_id",
                            "DF_MACRO_FILTERED-SHAPES",
                        )
                        df_macro_part = df_macro_filtered.merge(
                            batch_shapes, on="shape_id", how="inner"
                        )
                        print(
                            f"Merge final con shapes para lote: {len(df_macro_part)} filas"
                        )
                    else:
                        print(
                            f"⚠️ No hay shapes_ids comunes para las rutas: {batch_routes}"
                        )
                        # Mantener solo columnas relevantes sin shapes
                        df_macro_part = df_macro_part[
                            [
                                "Final_Color",
                                "route_short_name",
                                "trip_headsign",
                                "route_id",
                                "trip_id",
                                "service_id",
                                "shape_id",
                            ]
                        ].drop_duplicates()
                        print(
                            f"Continuando solo con datos básicos: {len(df_macro_part)} filas"
                        )
                else:
                    print(f"No hay shape_ids válidos para las rutas: {batch_routes}")
            except Exception as e:
                logging.error(f"Error procesando shapes para lote: {str(e)}")
                print(f"⚠️ Error en procesamiento de shapes: {str(e)}")
                # Intentar continuar solo con datos básicos
                df_macro_part = df_macro_part[
                    [
                        "Final_Color",
                        "route_short_name",
                        "trip_headsign",
                        "route_id",
                        "trip_id",
                        "service_id",
                        "shape_id",
                    ]
                ].drop_duplicates()
                print(f"Continuando solo con datos básicos: {len(df_macro_part)} filas")

        # Normalizar columnas
        if (
            "Final_Color" in df_macro_part.columns
            and "final_color" not in df_macro_part.columns
        ):
            df_macro_part.rename(columns={"Final_Color": "final_color"}, inplace=True)

        # Asegurar que todas las columnas esperadas existan
        for col, dtype in macro_columns.items():
            if col not in df_macro_part.columns:
                df_macro_part[col] = None

                if dtype == "string":
                    df_macro_part[col] = df_macro_part[col].astype("object")
                elif dtype == "double":
                    df_macro_part[col] = pd.to_numeric(
                        df_macro_part[col], errors="coerce"
                    )
                elif dtype == "bigint":
                    df_macro_part[col] = pd.to_numeric(
                        df_macro_part[col], errors="coerce"
                    ).astype("Int64")

        # Detección y eliminación de columnas duplicadas
        if df_macro_part.columns.duplicated().any():
            print(
                f"ALERTA: Columnas duplicadas detectadas: {df_macro_part.columns[df_macro_part.columns.duplicated()].tolist()}"
            )
            df_macro_part = df_macro_part.loc[
                :, ~df_macro_part.columns.duplicated(keep="first")
            ]

        # Guardar lote procesado a S3
        temp_file_path = f"{base_temp_path}/batch_{i//batch_size + 1}.csv"
        write_csv_s3(df_macro_part, bucket, temp_file_path)
        temp_files.append(temp_file_path)
        total_rows += len(df_macro_part)

        # Liberar memoria
        del batch_trips, batch_routes_df, df_macro_part
        gc.collect()

    print(f"Procesados {total_rows} filas en total para df_macro")
    print(f"Archivos temporales creados: {len(temp_files)}")

    return {
        "temp_files": temp_files,
        "total_rows": total_rows,
        "columns_info": macro_columns,
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

    print(f"Combinando {len(temp_files)} archivos temporales en {final_path}")

    # Verificar si hay archivos temporales
    if not temp_files:
        print(f"ALERTA: No hay archivos temporales para combinar en {final_path}")
        return

    # Preservar información de columnas para uso posterior
    columns_info = temp_info.get("columns_info", {})

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
        # Incluir columnas adicionales encontradas manteniendo consistencia
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

            # Garantizar consistencia de esquema en cada archivo
            df = pd.read_csv(io.StringIO(content))

            # Normalización de esquema para consistencia
            for col in ordered_columns:
                if col not in df.columns:
                    df[col] = None

            # Aplicar orden unificado de columnas
            df = df[ordered_columns]

            # Escritura incremental manteniendo integridad estructural
            if i == 0:
                # Primer archivo establece estructura con encabezados
                write_csv_s3(df, bucket, final_path)
            else:
                # Archivos subsecuentes mantienen estructura sin duplicar encabezados
                with io.StringIO() as csv_buffer:
                    df.to_csv(csv_buffer, index=False, header=False)
                    csv_buffer.seek(0)
                    append_content = csv_buffer.getvalue()

                    # Evitar adición de contenido vacío
                    if append_content.strip():
                        existing_content = (
                            s3_client.get_object(Bucket=bucket, Key=final_path)["Body"]
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

            # Optimización de memoria
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

        # Validar integridad de encabezados
        if not any(col in first_line for col in ordered_columns[:3]):
            logging.warning(
                "Detectada posible inconsistencia estructural en el archivo final"
            )

            # Restauración de integridad estructural
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

    log_memory_usage("Inicio de MacroGenerator")

    try:
        # Leer archivos preprocesados
        trips = read_csv_s3(bucket, f"{temp_dir}/trips.csv")
        routes = read_csv_s3(bucket, f"{temp_dir}/routes.csv")

        # Intentar leer shapes si existen
        try:
            shapes = read_csv_s3(bucket, f"{temp_dir}/shapes.csv")
        except Exception as e:
            logger.warning(
                f"No se encontró archivo shapes.csv en el directorio temporal: {str(e)}"
            )
            shapes = None

        # Procesar macros con la misma función del código original
        macro_info = create_df_macro_stream(
            trips,
            routes,
            shapes,
            P_EMPRESA,
            P_CONTR,
            P_VERSION,
            batch_size=5,
            execution_id=execution_id,
            bucket=bucket,  # Pasar el bucket explícitamente
        )

        # Establecer ruta de salida idéntica a la original
        output_path_macro = f"MAPS/MACRO/explotation={P_EMPRESA}/contract={P_CONTR}/version={P_VERSION}/{P_VERSION}_macro"

        # Combinar archivos usando la función exacta del original
        combine_temp_files(macro_info, output_path_macro, "MACRO", bucket)

        log_memory_usage("Fin de MacroGenerator")

        print(
            f"Proceso MacroGenerator completado con éxito. Archivo generado: {output_path_macro}"
        )

    except Exception as e:
        logger.error(f"Error en MacroGenerator: {str(e)}")
        raise e


if __name__ == "__main__":

    try:
        main()
        print("Ejecución completada correctamente.")
    except Exception as e:
        print(f"Error en la ejecución principal: {str(e)}")
        sys.exit(1)
