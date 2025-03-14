import sys
import json
import logging
import shutil
import os
import boto3
from io import StringIO

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when
from pyspark.sql.functions import concat_ws

s3_client = boto3.client("s3")
region = boto3.Session().region_name

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "bronze_bucket", "json_input"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


bronze_bucket = args["bronze_bucket"]
json_input = args["json_input"]
explotations = json.loads(json_input)

p_empresa = json.loads(explotations[0]["body"])["P_EMPRESA"]
print("p_empresa", p_empresa)
print(type(p_empresa))
p_contr = json.loads(explotations[0]["body"])["P_CONTR"]
print("p_contr", p_contr)
print(type(p_contr))
p_contr_flag = json.loads(explotations[0]["body"])["P_CONTR_FLAG"]
print("p_contr_flag", p_contr_flag)
p_version = json.loads(explotations[0]["body"])["P_VERSION"]
print("p_version", p_version)
p_fechad = json.loads(explotations[0]["body"])["P_FECHAD"]
print("p_fechad", p_fechad)
print(type(p_fechad))
p_fechah = json.loads(explotations[0]["body"])["P_FECHAH"]
print("p_fechah", p_fechah)
print(type(p_fechah))
p_env = json.loads(explotations[0]["body"])["P_ENV"]
print("p_env", p_env)

p_lines = []
for explotation in explotations:
    p_lines.append(json.loads(explotation["body"])["P_LINE"])
lineas_activas = tuple(p_lines)
print(lineas_activas)
contract_codes = []

for line in p_lines:
    contract_codes.append(
        spark.sql(
            f"SELECT '{p_empresa}' AS EMPRESA, '{line}' AS LINE, '{p_contr}' AS CONTRACT_CODE"
        )
    )

# Union all the DataFrames
final_contract_codes_df = contract_codes[0]
for df in contract_codes[1:]:
    final_contract_codes_df = final_contract_codes_df.union(df)

final_contract_codes_df.createOrReplaceTempView("contract_codes")
result = spark.sql("SELECT * FROM contract_codes")
print("Contract codes")
result.show()


eventos_horario_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/EVENTOSHORARIO/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(eventos_horario_path)
try:
    eventos_horario_aux = spark.read.option("header", True).csv(eventos_horario_path)
    eventos_horario = eventos_horario_aux.dropDuplicates()
    print(eventos_horario.count())
    eventos_horario.createOrReplaceTempView("EVENTOSHORARIO")
    df_eventos_horario = spark.sql("SELECT * FROM EVENTOSHORARIO")
    df_eventos_horario.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {eventos_horario_path}")

jornadas_tipo_dia_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/JORNADASTIPODIA/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(jornadas_tipo_dia_path)
try:
    jornadas_tipo_dia_aux = (
        spark.read.option("header", True)
        .option("inferSchema", "true")
        .csv(jornadas_tipo_dia_path)
    )
    jornadas_tipo_dia = jornadas_tipo_dia_aux.dropDuplicates()
    print(jornadas_tipo_dia.count())
    jornadas_tipo_dia.createOrReplaceTempView("JORNADASTIPODIA")
except Exception as e:
    # Si ocurre un error (por ejemplo, si la ruta no existe), imprimir un mensaje y no interrumpir el job
    logging.error(
        f"No se pudo leer el archivo en la ruta {jornadas_tipo_dia_path}: {str(e)}"
    )
    print(f"El path no existe o hubo un error al leerlo: {jornadas_tipo_dia_path}")

lineas_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/LINEAS/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(lineas_path)
try:
    lineas_aux = spark.read.option("header", True).csv(lineas_path)
    lineas = lineas_aux.dropDuplicates()
    print(lineas.count())
    lineas.createOrReplaceTempView("LINEAS")
    lineas = spark.sql("SELECT * FROM LINEAS")
    lineas.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {lineas_path}")

nodos_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/NODOS/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(nodos_path)
try:
    nodos_aux = spark.read.option("header", True).csv(nodos_path)
    nodos = nodos_aux.dropDuplicates()
    print(nodos.count())
    nodos.createOrReplaceTempView("NODOS")
    nodos = spark.sql("SELECT * FROM NODOS")
    nodos.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {nodos_path}")

nodos_ruta_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/NODOSRUTA/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(nodos_ruta_path)
try:
    nodos_ruta_aux = spark.read.option("header", True).csv(nodos_ruta_path)
    nodos_ruta = nodos_ruta_aux.dropDuplicates()
    print(nodos_ruta.count())
    nodos_ruta.createOrReplaceTempView("NODOSRUTA")
    nodos_ruta = spark.sql("SELECT * FROM NODOSRUTA")
    nodos_ruta.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {nodos_ruta_path}")


lineas_sublinea_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/LINEASSUBLINEA/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(lineas_sublinea_path)
try:
    lineas_sublinea_aux = spark.read.option("header", True).csv(lineas_sublinea_path)
    lineas_sublinea = lineas_sublinea_aux.dropDuplicates()
    print(lineas_sublinea.count())
    lineas_sublinea.createOrReplaceTempView("LINEASSUBLINEA")
    lineas_sublinea = spark.sql("SELECT * FROM LINEASSUBLINEA")
    lineas_sublinea.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {lineas_sublinea_path}")

rutas_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/RUTAS/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(rutas_path)
try:
    rutas_aux = spark.read.option("header", True).csv(rutas_path)
    rutas = rutas_aux.dropDuplicates()
    print(rutas.count())
    rutas.createOrReplaceTempView("RUTAS")
    rutas = spark.sql("SELECT * FROM RUTAS")
    rutas.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {rutas_path}")

rutas_sublinea_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/RUTASSUBLINEA/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(rutas_sublinea_path)
try:
    rutas_sublinea_aux = spark.read.option("header", True).csv(rutas_sublinea_path)
    rutas_sublinea = rutas_sublinea_aux.dropDuplicates()
    print(rutas_sublinea.count())
    rutas_sublinea.createOrReplaceTempView("RUTASSUBLINEA")
    rutas_sublinea = spark.sql("SELECT * FROM RUTASSUBLINEA")
    rutas_sublinea.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {rutas_sublinea_path}")

secciones_ruta_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/SECCIONESRUTA/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(secciones_ruta_path)
try:
    secciones_ruta_aux = spark.read.option("header", True).csv(secciones_ruta_path)
    secciones_ruta = secciones_ruta_aux.dropDuplicates()
    print(secciones_ruta.count())
    secciones_ruta.createOrReplaceTempView("SECCIONESRUTA")
    secciones_ruta = spark.sql("SELECT * FROM SECCIONESRUTA")
    secciones_ruta.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {secciones_ruta_path}")

viajes_horario_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/VIAJESHORARIO/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(viajes_horario_path)
try:
    viajes_horario_aux = (
        spark.read.option("header", True)
        .option("inferSchema", "true")
        .option("maxPartitionBytes", "134217728")
        .csv(viajes_horario_path)
    )
    viajes_horario = viajes_horario_aux.dropDuplicates()
    print(viajes_horario.count())
    viajes_horario.createOrReplaceTempView("VIAJESHORARIO")
except Exception as e:
    # Si ocurre un error (por ejemplo, si la ruta no existe), imprimir un mensaje y no interrumpir el job
    logging.error(
        f"No se pudo leer el archivo en la ruta {viajes_horario_path}: {str(e)}"
    )
    print(f"El path no existe o hubo un error al leerlo: {viajes_horario_path}")


configrutas_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/CONFIGRUTAS/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(configrutas_path)
try:
    configrutas_aux = (
        spark.read.option("header", True)
        .option("inferSchema", "true")
        .option("maxPartitionBytes", "134217728")
        .csv(configrutas_path)
    )
    configrutas = configrutas_aux.dropDuplicates()
    print(configrutas.count())
    configrutas.createOrReplaceTempView("CONFIGRUTAS")
except Exception as e:
    # Si ocurre un error (por ejemplo, si la ruta no existe), imprimir un mensaje y no interrumpir el job
    logging.error(f"No se pudo leer el archivo en la ruta {configrutas_path}: {str(e)}")
    print(f"El path no existe o hubo un error al leerlo: {configrutas_path}")

vwisae_tramos_seccion_path = f"s3://{bronze_bucket}/SAEBaseNext/landing/VWISAE_TRAMOSECCION/explotation={p_empresa}/contract={p_contr}/version={p_version}/"
print(vwisae_tramos_seccion_path)
try:
    vwisae_tramos_seccion_aux = spark.read.option("header", True).csv(
        vwisae_tramos_seccion_path
    )
    vwisae_tramos_seccion = vwisae_tramos_seccion_aux.dropDuplicates()
    print(vwisae_tramos_seccion.count())
    vwisae_tramos_seccion.createOrReplaceTempView("VWISAE_TRAMOSECCION")
    vwisae_tramos_seccion = spark.sql("SELECT * FROM VWISAE_TRAMOSECCION")
    vwisae_tramos_seccion.printSchema()
except Exception as e:
    print(f"El path no existe o hubo un error al leerlo: {vwisae_tramos_seccion_path}")


def process(queries, P_EMPRESA, P_VERSION, P_CONTR, P_FECHAD, P_FECHAH, contract_codes):
    """
    Procesa las tablas: routes, stops, trips, calendar,
    calendar_dates, stop_times, trayectos
    """

    try:
        if "shapes" in queries.items():
            return

        else:

            for file_name, query in queries.items():

                try:
                    contract_codes_sql = spark.sql(
                        "SELECT * FROM contract_codes"
                    ).toPandas()
                    sql_for_copy = "WITH contract_codes AS (\n"
                    for i, row in contract_codes_sql.iterrows():
                        sql_for_copy += f"    SELECT '{row['EMPRESA']}' AS EMPRESA, '{row['LINE']}' AS LINE, '{row['CONTRACT_CODE']}' AS CONTRACT_CODE FROM DUAL"
                        if i < len(contract_codes_sql) - 1:
                            sql_for_copy += " UNION ALL\n"
                        else:
                            sql_for_copy += "\n"
                    sql_for_copy += ")\n" + query.replace(
                        "( contract_codes )", "contract_codes"
                    )
                    print(f"\n===== SQL DE ({file_name}) =====")
                    print(sql_for_copy)
                except Exception as e:
                    print(f"Error al crear SQL para depuración: {str(e)}")

                file_path = "".join(
                    f"GTFS/{file_name.upper()}"
                    f"/explotation={P_EMPRESA}"
                    f"/contract={P_CONTR}"
                    f"/version={P_VERSION}/"
                )
                # s3://{bronze_bucket}/
                #                    f"/{file_name}.txt"
                logging.info(f"Procesando {file_name}...")
                df = spark.sql(query)
                # df.show()
                print(df.count())
                try:
                    cleaned_df = df.select(
                        [
                            when(col(c).isNull(), "").otherwise(col(c)).alias(c)
                            for c in df.columns
                        ]
                    )
                    cleaned_df.show()
                    single_column_df = cleaned_df.select(
                        concat_ws(",", *cleaned_df.columns).alias("combined")
                    )

                    # Escribir los encabezados primero en el archivo
                    headers = ",".join(
                        cleaned_df.columns
                    )  # Crear la línea con los nombres de las columnas

                    # Escribir los encabezados en un archivo temporal en memoria
                    output_buffer = StringIO()
                    output_buffer.write(headers + "\n")  # Agregar los encabezados

                    # Escribir los datos del DataFrame en el buffer
                    for row in single_column_df.collect():
                        output_buffer.write(row["combined"] + "\n")

                    # Obtener todo el contenido del buffer
                    output_content = output_buffer.getvalue()

                    # Escribir el contenido en el archivo S3
                    s3_client.put_object(
                        Bucket=bronze_bucket,
                        Key=f"{file_path}{file_name}.txt",
                        Body=output_content,
                    )
                    # Verificar que el archivo se guardó correctamente en S3
                    print(
                        f"Archivo guardado correctamente en: {file_path}{file_name}.txt"
                    )

                    # Escribir los encabezados primero en el archivo de salida
                    # s3_client.put_object(Bucket=bronze_bucket, Key=f"{file_path}{file_name}.txt", Body=headers + "\n")

                    # single_column_df.coalesce(1).write.option("header", "true").mode("overwrite").format("text").save(f"s3://{bronze_bucket}/{file_path}")
                    # response = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=file_path)

                    # Buscar el archivo 'part-00000-*.txt'
                    # part_file = None
                    # for obj in response.get('Contents', []):
                    #    if obj['Key'].startswith(f"{file_path}part-00000-"):
                    #        part_file = obj['Key']
                    #        break

                    # if part_file:
                    #    print("part_file= ",part_file)
                    # Copiar el archivo 'part-00000-*.txt' a 'file.txt'
                    #    s3_client.copy_object(Bucket=bronze_bucket, CopySource=f"{bronze_bucket}/{part_file}", Key=f"{file_path}{file_name}.txt")

                    # Eliminar el archivo original 'part-00000-*' después de la copia
                    #    s3_client.delete_object(Bucket=bronze_bucket, Key=part_file)

                    #    print(f"Archivo renombrado correctamente a: {file_path}{file_name}")
                    # else:
                    #    print(f"No se encontró el archivo con el patrón {source_prefix}part-00000-*.txt.")

                except Exception as e:
                    print(f"Error occurred while writing to S3: {str(e)}")
                    raise e

    except Exception as e:
        logging.error(f"Error processing the event: {e}")
        raise


print("p_contr_flag= ", p_contr_flag)
print("p_empresa = ", p_empresa)
print("p_empresa = ", type(p_empresa))

# Multiples contrato
queries = {}

if p_empresa == "60":
    queries.update(
        {
            "calendar": f"""
            SELECT
                '' AS service_id,
                0 AS monday,
                0 AS tuesday,
                0 AS wednesday,
                0 AS thursday,
                0 AS friday,
                0 AS saturday,
                0 AS sunday,
                '' AS start_date,
                '' AS end_date
            LIMIT 0
        """,
            "trips": f"""
            WITH LineaRutaOrden AS (
                SELECT DISTINCT
                    li.Linea,
                    REPLACE(REPLACE(REPLACE(li.Label, '.', ''), ' ', ''), '-', '') AS label,
                    rs.Sublinea,
                    rs.Ruta,
                    ru.Sentido
                FROM Lineas li
                INNER JOIN RutasSublinea rs ON rs.Linea = li.Linea
                INNER JOIN Rutas ru ON ru.Ruta = rs.Ruta
            )
            SELECT DISTINCT
                vh.Linea AS route_id,
                concat(vh.TipoDia, '_', vh.Servicio, '_', CAST(vh.ViajeLinea AS STRING), '_',
                    CAST(vh.NumViaje AS STRING), '_', CAST(cr.Ruta AS STRING)) AS trip_id,
                concat(vh.TipoDia, '_', vh.Servicio) AS service_id,
                concat(substring(CAST(lro.Linea AS STRING), 1, 4), '_',
                    lro.label, '_',
                    'T', substring(CAST(lro.Ruta AS STRING), 1, 4), '_',
                    CASE
                        WHEN lro.Sentido = 1 THEN 'I'
                        WHEN lro.Sentido = 2 THEN 'V'
                        WHEN lro.Sentido = 3 THEN 'I'
                    END) AS shape_id,
                ru.Destino AS trip_headsign,
                NULL AS trip_short_name,
                CAST(CASE WHEN ru.sentido = 3 THEN 0 ELSE ru.Sentido-1 END AS INT) AS direction_id,
                NULL AS block_id,
                NULL AS wheelchair_accessible,
                NULL AS bikes_allowed
            FROM JornadasTipoDia hd
            INNER JOIN ViajesHorario vh ON vh.tipodia = hd.TipoDia
            INNER JOIN Lineas li ON li.Linea = vh.Linea
            INNER JOIN CONFIGRUTAS cr on vh.Ruta = cr.Ruta
            INNER JOIN rutas ru ON ru.Ruta = cr.Ruta
            INNER JOIN LineaRutaOrden lro ON lro.Linea = vh.Linea AND lro.Ruta = cr.Ruta AND lro.Sentido = ru.Sentido
            LEFT JOIN LineasSublinea ls ON ls.linea = li.linea
        """,
            "pre_stop_times": f"""
                WITH nodos_ruta AS (
                    SELECT DISTINCT
                        nr.Nodo,
                        nr.TipoNodo,
                        nr.Ruta,
                        nr.Posicion,
                        ROW_NUMBER() OVER (
                            PARTITION BY l.linea, nr.ruta, vh.NumViaje, vh.TipoDia, hd.JornadaTipo, vh.ruta, vh.servicio, vh.sublinea
                            ORDER BY nr.posicion
                        ) AS orden,
                        l.linea,
                        vh.NumViaje,
                        vh.TipoDia,
                        vh.servicio,
                        vh.ViajeLinea,
                        vh.sublinea
                    FROM NodosRuta nr
                    INNER JOIN Nodos n ON n.Nodo = nr.Nodo AND n.Tipo = nr.TipoNodo
                    INNER JOIN ViajesHorario vh ON vh.Ruta = nr.Ruta
                    INNER JOIN Lineas l ON l.linea = vh.linea
                    INNER JOIN JornadasTipoDia hd ON hd.TipoDia = vh.TipoDia
                    WHERE hd.TipoDia NOT LIKE 'TU%'
                    AND hd.TipoDia NOT LIKE 'MIC%'
                    AND hd.TipoDia NOT LIKE 'TR%'
                ),
                tiempos AS (
                    SELECT DISTINCT
                        concat(
                            lpad(CAST(FLOOR(instante / 3600) AS STRING), 2, '0'), ':',
                            lpad(CAST(FLOOR((instante % 3600) / 60) AS STRING), 2, '0'), ':',
                            lpad(CAST(FLOOR(instante % 60) AS STRING), 2, '0')
                        ) AS arrival_time,
                        *
                    FROM EventosHorario
                ),
                tabla AS (
                    SELECT DISTINCT
                        nr.*,
                        eh.arrival_time,
                        eh.instante
                    FROM nodos_ruta nr
                    LEFT JOIN tiempos eh ON eh.nodo = nr.Nodo 
                        AND eh.TipoNodo = nr.TipoNodo 
                        AND eh.TipoDia = nr.TipoDia
                        AND eh.servicio = nr.servicio
                        AND eh.NumViaje = nr.NumViaje
                ),
                min_arrival_time AS (
                    SELECT 
                        linea,
                        ruta,
                        sublinea,
                        NumViaje,
                        TipoDia,
                        Servicio,
                        min(substring(arrival_time, 1, 5)) AS min_arrival_time
                    FROM tabla
                    GROUP BY linea, ruta, sublinea, NumViaje, TipoDia, Servicio
                )
                SELECT DISTINCT
                    concat(t.TipoDia, '_', t.Servicio, '_', CAST(t.ViajeLinea AS STRING), '_', 
                        CAST(t.NumViaje AS STRING), '_', CAST(t.ruta AS STRING)) AS trip_id,
                    t.instante/60 AS arrival_time,
                    t.Nodo AS stop_id,
                    t.orden AS stop_sequence,
                    t.Ruta AS stop_headsign,
                    t.Posicion AS shape_dist_traveled,
                    NULL AS pickup_type,
                    NULL AS drop_off_type,
                    NULL AS continuous_pickup,
                    NULL AS continuous_drop_off,
                    0 AS timepoint
                FROM tabla t
                INNER JOIN Rutas r ON t.ruta = r.ruta
                LEFT JOIN min_arrival_time mat ON t.linea = mat.linea 
                    AND t.ruta = mat.ruta 
                    AND t.sublinea = mat.sublinea 
                    AND t.NumViaje = mat.NumViaje
                    AND t.TipoDia = mat.TipoDia
                    AND t.Servicio = mat.Servicio
                ORDER BY trip_id, stop_sequence
        """,
            "stops": f"""
            SELECT DISTINCT
                n.ID AS stop_id,
                n.Label AS stop_code,
                REPLACE(n.Nombre, ',', ' -') AS stop_name,
                'Parada' AS stop_desc,
                n.Posy AS stop_lat,
                n.Posx AS stop_lon,
                0 AS zone_id,
                NULL AS stop_url,
                0 AS location_type,
                NULL AS parent_station,
                'Europe/Madrid' AS stop_timezone,
                NULL AS wheelchair_boarding,
                NULL AS level_id,
                NULL AS platform_code
            FROM JornadasTipoDia jtd
            INNER JOIN ViajesHorario vh ON vh.tipodia = jtd.TipoDia
            LEFT OUTER JOIN NodosRuta nr ON nr.Ruta = vh.Ruta
            INNER JOIN Nodos n ON n.Nodo = nr.Nodo AND n.Tipo = nr.TipoNodo
            WHERE n.tipo = 1
        """,
            "shapes": f"""
            WITH LineaRutaOrden AS (
                SELECT DISTINCT
                    li.Linea,
                    REPLACE(REPLACE(REPLACE(li.Label, '.', ''), ' ', ''), '-', '') AS label,
                    rs.Sublinea,
                    rs.Ruta,
                    ru.Sentido,
                    sr.Orden,
                    sr.Seccion
                FROM Lineas li
                INNER JOIN RutasSublinea rs ON rs.Linea = li.Linea
                INNER JOIN Rutas ru ON ru.Ruta = rs.Ruta
                INNER JOIN seccionesRuta sr ON sr.Linea = rs.Linea AND sr.Ruta = rs.Ruta
                INNER JOIN ViajesHorario vh ON vh.Linea = li.Linea
                INNER JOIN JornadasTipoDia jtd ON jtd.TipoDia = vh.TipoDia
            )
            SELECT
                concat(substring(CAST(lro.Linea AS STRING), 1, 4), '_',
                    REPLACE(REPLACE(REPLACE(lro.Label, '.', ''), ' ', ''), '-', ''), '_',
                    'T', substring(CAST(lro.Ruta AS STRING), 1, 4), '_',
                    CASE
                        WHEN lro.Sentido = 1 THEN 'I'
                        WHEN lro.Sentido = 2 THEN 'V'
                        WHEN lro.Sentido = 3 THEN 'I'
                    END) AS shape_id,
                wts.Posy AS shape_pt_lat,
                wts.Posx AS shape_pt_lon,
                ROW_NUMBER() OVER (PARTITION BY lro.Ruta ORDER BY lro.Orden ASC) AS shape_pt_sequence,
                wts.Distancia AS shape_dist_traveled
            FROM LineaRutaOrden lro
            INNER JOIN vwISAE_TramoSeccion wts ON wts.Id = lro.Seccion
            ORDER BY shape_id, shape_pt_sequence
        """,
            "routes": f"""
            SELECT DISTINCT
                l.Linea AS route_id,
                concat(CAST({p_empresa} AS STRING), '_', CAST({p_contr} AS STRING)) AS agency_id,
                REPLACE(l.Label, ' ', '') AS route_short_name,
                l.Nombre AS route_long_name,
                NULL AS route_desc,
                3 AS route_type,
                NULL AS route_url,
                'CC0000' AS route_color,
                'FFFFFF' AS route_text_color,
                NULL AS route_sort_order,
                NULL AS continuous_pickup,
                NULL AS continuous_drop_off
            FROM Lineas l
            INNER JOIN ViajesHorario vh ON vh.Linea = l.Linea
            INNER JOIN JornadasTipoDia jtd ON jtd.TipoDia = vh.TipoDia
            ORDER BY route_id
        """,
            "calendar_dates": f"""
            SELECT DISTINCT
                concat(hd.TipoDia, '_', vh.Servicio) AS service_id,
                REPLACE(hd.JornadaTipo,'-','') AS date,
                1 AS exception_type
            FROM JornadasTipoDia hd
            INNER JOIN ViajesHorario vh ON vh.tipodia = hd.TipoDia
            ORDER BY service_id, date
        """,
            "trips_detail_by_date": f"""
            WITH LineaRutaOrden AS (
                SELECT DISTINCT
                    li.Linea,
                    REPLACE(REPLACE(REPLACE(li.Label, '.', ''), ' ', ''), '-', '') AS label,
                    rs.Sublinea,
                    rs.Ruta,
                    ru.Sentido
                FROM Lineas li
                INNER JOIN RutasSublinea rs ON rs.Linea = li.Linea
                INNER JOIN Rutas ru ON ru.Ruta = rs.Ruta
            )
            SELECT DISTINCT
                vh.Linea AS route_id,
                concat(vh.TipoDia, '_', vh.Servicio, '_', CAST(vh.ViajeLinea AS STRING), '_',
                    CAST(vh.NumViaje AS STRING), '_', CAST(cr.Ruta AS STRING)) AS trip_id,
                concat(vh.TipoDia, '_', vh.Servicio) AS service_id,
                concat(substring(CAST(lro.Linea AS STRING), 1, 4), '_',
                    lro.label, '_',
                    'T', substring(CAST(lro.Ruta AS STRING), 1, 4), '_',
                    CASE
                        WHEN lro.Sentido = 1 THEN 'I'
                        WHEN lro.Sentido = 2 THEN 'V'
                        WHEN lro.Sentido = 3 THEN 'I'
                    END) AS shape_id,
                ru.Destino AS trip_headsign,
                NULL AS trip_short_name,
                CAST(CASE WHEN ru.sentido = 3 THEN 0 ELSE ru.Sentido-1 END AS INT) AS direction_id,
                NULL AS block_id,
                hd.JornadaTipo AS date_calendia_detail,
                NULL AS wheelchair_accessible,
                NULL AS bikes_allowed
            FROM JornadasTipoDia hd
            INNER JOIN ViajesHorario vh ON vh.tipodia = hd.TipoDia
            INNER JOIN Lineas li ON li.Linea = vh.Linea
            INNER JOIN CONFIGRUTAS cr on vh.Ruta = cr.Ruta
            INNER JOIN rutas ru ON ru.Ruta = cr.Ruta
            INNER JOIN LineaRutaOrden lro ON lro.Linea = vh.Linea AND lro.Ruta = cr.Ruta AND lro.Sentido = ru.Sentido
            LEFT JOIN LineasSublinea ls ON ls.linea = li.linea

        """,
        }
    )

process(queries, p_empresa, p_version, p_contr, p_fechad, p_fechah, contract_codes)


response = {
    "statusCode": 200,
    "body": json.dumps(
        {
            "P_EMPRESA": p_empresa,
            "P_VERSION": p_version,
            "P_FECHAD": p_fechad,
            "P_FECHAH": p_fechah,
            "P_CONTR": p_contr,
        }
    ),
}

print(response)
job.commit()
