#!/usr/bin/env python3
"""
Gestor del procesamiento de datos GTFS.
Coordina la ejecución del preprocesador y los jobs de macro y macro-stops.
"""

import json
import boto3
import time
import os
import logging
import uuid
import argparse
from datetime import datetime, timedelta

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ProcessorManager")

class GTFSProcessorManager:
    def __init__(self, bucket_name, combinations_file="batch_processing/combinations.json", 
                 status_file="batch_processing/status.json", lambda_name=None,
                 macro_job_name=None, macro_stops_job_name=None, region="eu-west-1"):
        """
        Inicializa el gestor de procesamiento.
        
        Args:
            bucket_name: Nombre del bucket S3
            combinations_file: Archivo con las combinaciones a procesar
            status_file: Archivo para registrar el estado del procesamiento
            lambda_name: Nombre de la función Lambda del preprocesador
            macro_job_name: Nombre del job Glue para macro
            macro_stops_job_name: Nombre del job Glue para macro_stops
            region: Región de AWS
        """
        self.bucket_name = bucket_name
        self.combinations_file = combinations_file
        self.status_file = status_file
        self.region = region
        self.combinations = []
        
        # Inicializar clientes AWS
        self.glue_client = boto3.client('glue', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        
        # Nombres de recursos
        self.preprocessor_function = lambda_name or "GTFSPreprocessor"
        self.macro_job_name = macro_job_name or "MacroGenerator"
        self.macro_stops_job_name = macro_stops_job_name or "MacroStopsGenerator"
        
        # Crear directorio para archivos de estado si no existe
        os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
        
        # Cargar configuración
        self.load_combinations()
        self.load_status()
        
        logger.info(f"Gestor inicializado para bucket {bucket_name} en región {region}")
    
    def load_combinations(self):
        """Carga las combinaciones desde el archivo JSON"""
        try:
            with open(self.combinations_file, 'r') as f:
                data = json.load(f)
                self.combinations = data.get("combinations", [])
                logger.info(f"Cargadas {len(self.combinations)} combinaciones")
        except FileNotFoundError:
            logger.error(f"Archivo {self.combinations_file} no encontrado")
            self.combinations = []
    
    def load_status(self):
        """Carga el estado actual de procesamiento o crea uno nuevo"""
        try:
            with open(self.status_file, 'r') as f:
                self.status = json.load(f)
                
                # Actualizar estado con nuevas combinaciones si es necesario
                existing_combinations = {self._get_combo_key(c): c for c in self.status.get("combinations", [])}
                
                for combo in self.combinations:
                    key = self._get_combo_key(combo)
                    if key not in existing_combinations:
                        combo["status"] = "pending"
                        existing_combinations[key] = combo
                
                self.status["combinations"] = list(existing_combinations.values())
                self._update_counts()
                
                logger.info(f"Estado de procesamiento cargado: {self.status['total']} total, "
                           f"{self.status['pending']} pendientes, {self.status['completed']} completados, "
                           f"{self.status['failed']} fallidos")
                
        except FileNotFoundError:
            logger.info("Archivo de estado no encontrado, creando uno nuevo")
            self.status = {
                "total": len(self.combinations),
                "pending": len(self.combinations),
                "preprocessing": 0,
                "processing_macro": 0,
                "processing_macro_stops": 0,
                "completed": 0,
                "failed": 0,
                "started_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "combinations": self.combinations
            }
            self.save_status()
    
    def _get_combo_key(self, combination):
        """Genera una clave única para una combinación"""
        return f"{combination['P_EMPRESA']}_{combination['P_CONTR']}_{combination['P_VERSION']}"
    
    def _update_counts(self):
        """Actualiza los contadores de estado basado en las combinaciones"""
        counts = {
            "total": len(self.status["combinations"]),
            "pending": 0,
            "preprocessing": 0,
            "processing_macro": 0,
            "processing_macro_stops": 0,
            "completed": 0,
            "failed": 0
        }
        
        for combo in self.status["combinations"]:
            status = combo.get("status", "pending")
            if status in counts:
                counts[status] += 1
        
        self.status.update(counts)
        self.status["last_updated"] = datetime.now().isoformat()
    
    def save_status(self):
        """Guarda el estado actual en el archivo JSON"""
        self._update_counts()
        with open(self.status_file, 'w') as f:
            json.dump(self.status, f, indent=2)
        logger.info("Estado guardado")
    
    def run_preprocessor(self, combination):
        """
        Ejecuta el preprocesador para una combinación específica.
        
        Args:
            combination: Diccionario con P_EMPRESA, P_CONTR y P_VERSION
            
        Returns:
            Resultado del preprocesador o None si falla
        """
        try:
            # Preparar la entrada para el preprocesador
            payload = {
                "statusCode": 200,
                "body": json.dumps({
                    "P_EMPRESA": combination["P_EMPRESA"],
                    "P_VERSION": combination["P_VERSION"],
                    "P_CONTR": combination["P_CONTR"]
                })
            }
            
            logger.info(f"Invocando preprocesador para E={combination['P_EMPRESA']}, "
                       f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
            
            # Invocar la función Lambda
            response = self.lambda_client.invoke(
                FunctionName=self.preprocessor_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            # Procesar la respuesta
            response_payload = json.loads(response['Payload'].read().decode())
            
            if response_payload.get("status") == "success":
                logger.info(f"Preprocesamiento exitoso: execution_id={response_payload.get('execution_id')}")
                combination["preprocessor_result"] = response_payload
                combination["status"] = "preprocessed"
                return response_payload
            else:
                logger.error(f"Error en preprocesamiento: {response_payload}")
                combination["status"] = "failed"
                combination["error"] = response_payload.get("message", "Error desconocido")
                return None
                
        except Exception as e:
            logger.error(f"Error al ejecutar preprocesador: {str(e)}")
            combination["status"] = "failed"
            combination["error"] = str(e)
            return None
    
    def start_glue_job(self, job_name, arguments):
        """
        Inicia un job de Glue con los argumentos especificados.
        
        Args:
            job_name: Nombre del job de Glue
            arguments: Diccionario con los argumentos para el job
            
        Returns:
            ID del job o None si falla
        """
        try:
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments=arguments
            )
            job_run_id = response['JobRunId']
            logger.info(f"Job {job_name} iniciado con ID: {job_run_id}")
            return job_run_id
        except Exception as e:
            logger.error(f"Error al iniciar job {job_name}: {str(e)}")
            return None
    
    def check_job_status(self, job_name, job_run_id):
        """
        Verifica el estado de un job de Glue.
        
        Args:
            job_name: Nombre del job de Glue
            job_run_id: ID de la ejecución del job
            
        Returns:
            Estado del job o "FAILED" si hay error
        """
        try:
            response = self.glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
            status = response['JobRun']['JobRunState']
            return status
        except Exception as e:
            logger.error(f"Error al verificar estado de job {job_name}: {str(e)}")
            return "FAILED"
    
    def process_combination(self, combination):
        """
        Procesa una combinación: preprocesamiento y luego macro/macro_stops en paralelo.
        
        Args:
            combination: Diccionario con P_EMPRESA, P_CONTR y P_VERSION
            
        Returns:
            True si el proceso completo fue exitoso, False si no
        """
        # Actualizar estado
        combination["start_time"] = datetime.now().isoformat()
        combination["status"] = "preprocessing"
        self.save_status()
        
        # Ejecutar preprocesador
        logger.info(f"Iniciando procesamiento para E={combination['P_EMPRESA']}, "
                   f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
        
        preprocessor_result = self.run_preprocessor(combination)
        
        if not preprocessor_result:
            # Falló el preprocesamiento
            logger.error(f"Falló el preprocesamiento para E={combination['P_EMPRESA']}, "
                         f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
            combination["end_time"] = datetime.now().isoformat()
            self.save_status()
            return False
        
        # Si el preprocesamiento fue exitoso, iniciar jobs de Glue en paralelo
        temp_dir = preprocessor_result.get("temp_dir")
        execution_id = preprocessor_result.get("execution_id")
        
        if not temp_dir or not execution_id:
            logger.error(f"Faltan datos necesarios en el resultado del preprocesador")
            combination["status"] = "failed"
            combination["error"] = "Datos faltantes en resultado del preprocesador"
            combination["end_time"] = datetime.now().isoformat()
            self.save_status()
            return False
        
        # Argumentos comunes para ambos jobs
        # Convertir los argumentos a formato esperado por Glue (todos como strings)
        common_args = {
            "--JOB_NAME": self.macro_job_name,  # Para el job de macro
            "--P_EMPRESA": combination["P_EMPRESA"],
            "--P_VERSION": combination["P_VERSION"],
            "--P_CONTR": combination["P_CONTR"],
            "--temp_dir": temp_dir,
            "--execution_id": execution_id,
            "--bronze_bucket": self.bucket_name,
            "--S3_BUCKET": self.bucket_name,  # Para compatibilidad
        }
        
        # Crear un JSON para el argumento --json_input (útil para proveer el contexto completo)
        json_input = [
            {
                "statusCode": 200,
                "body": json.dumps({
                    "P_EMPRESA": combination["P_EMPRESA"],
                    "P_VERSION": combination["P_VERSION"],
                    "P_CONTR": combination["P_CONTR"]
                })
            }
        ]
        common_args["--json_input"] = json.dumps(json_input)
        
        # Iniciar MacroGenerator
        logger.info(f"Iniciando job de macro para E={combination['P_EMPRESA']}, "
                   f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
        combination["status"] = "processing_macro"
        self.save_status()
        macro_job_id = self.start_glue_job(self.macro_job_name, common_args)
        combination["macro_job_id"] = macro_job_id
        
        # Actualizar argumentos para macro_stops
        common_args["--JOB_NAME"] = self.macro_stops_job_name  # Cambiar para el job de macro_stops
        
        # Iniciar MacroStopsGenerator
        logger.info(f"Iniciando job de macro_stops para E={combination['P_EMPRESA']}, "
                   f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
        combination["status"] = "processing_macro_stops"
        self.save_status()
        macro_stops_job_id = self.start_glue_job(self.macro_stops_job_name, common_args)
        combination["macro_stops_job_id"] = macro_stops_job_id
        
        if not macro_job_id or not macro_stops_job_id:
            combination["status"] = "failed"
            combination["error"] = "Error al iniciar jobs de Glue"
            combination["end_time"] = datetime.now().isoformat()
            self.save_status()
            return False
        
        self.save_status()
        
        # Monitorear jobs hasta que ambos finalicen
        macro_completed = False
        macro_stops_completed = False
        
        max_time = datetime.now() + timedelta(hours=4)  # Limite de 4 horas para evitar bloqueos
        check_interval = 30  # segundos entre verificaciones
        
        while not (macro_completed and macro_stops_completed) and datetime.now() < max_time:
            time.sleep(check_interval)
            
            if not macro_completed and macro_job_id:
                macro_status = self.check_job_status(self.macro_job_name, macro_job_id)
                if macro_status in ["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"]:
                    macro_completed = True
                    combination["macro_status"] = macro_status
                    logger.info(f"Job de macro finalizado con estado: {macro_status}")
            
            if not macro_stops_completed and macro_stops_job_id:
                macro_stops_status = self.check_job_status(self.macro_stops_job_name, macro_stops_job_id)
                if macro_stops_status in ["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"]:
                    macro_stops_completed = True
                    combination["macro_stops_status"] = macro_stops_status
                    logger.info(f"Job de macro_stops finalizado con estado: {macro_stops_status}")
            
            self.save_status()
        
        # Si llegamos al límite de tiempo
        if datetime.now() >= max_time:
            if not macro_completed:
                combination["macro_status"] = "MONITORING_TIMEOUT"
                logger.warning(f"Tiempo de monitoreo excedido para job de macro")
            
            if not macro_stops_completed:
                combination["macro_stops_status"] = "MONITORING_TIMEOUT"
                logger.warning(f"Tiempo de monitoreo excedido para job de macro_stops")
        
        # Determinar el resultado final
        macro_success = combination.get("macro_status") == "SUCCEEDED"
        macro_stops_success = combination.get("macro_stops_status") == "SUCCEEDED"
        
        if macro_success and macro_stops_success:
            combination["status"] = "completed"
            result = True
            logger.info(f"Procesamiento completo exitoso para E={combination['P_EMPRESA']}, "
                       f"C={combination['P_CONTR']}, V={combination['P_VERSION']}")
        else:
            combination["status"] = "failed"
            combination["error"] = f"Macro: {combination.get('macro_status', 'UNKNOWN')}, " \
                                 f"MacroStops: {combination.get('macro_stops_status', 'UNKNOWN')}"
            result = False
            logger.error(f"Procesamiento fallido para E={combination['P_EMPRESA']}, "
                        f"C={combination['P_CONTR']}, V={combination['P_VERSION']}: {combination['error']}")
        
        combination["end_time"] = datetime.now().isoformat()
        self.save_status()
        return result
    
    def process_all(self, batch_size=5, max_retries=0):
        """
        Procesa todas las combinaciones pendientes en lotes.
        
        Args:
            batch_size: Número de combinaciones a procesar en paralelo
            max_retries: Número máximo de reintentos para combinaciones fallidas
            
        Returns:
            Diccionario con resultados del procesamiento
        """
        pending_combinations = [c for c in self.status["combinations"] if c["status"] == "pending"]
        total_pending = len(pending_combinations)
        
        if total_pending == 0:
            logger.info("No hay combinaciones pendientes para procesar")
            return self.status
        
        logger.info(f"Iniciando procesamiento de {total_pending} combinaciones pendientes")
        
        for i in range(0, total_pending, batch_size):
            batch = pending_combinations[i:i+batch_size]
            logger.info(f"Procesando lote {i//batch_size + 1} ({len(batch)} combinaciones)")
            
            active_processes = {}
            
            # Iniciar procesamiento para cada combinación en el lote
            for combination in batch:
                key = self._get_combo_key(combination)
                active_processes[key] = combination
                self.process_combination(combination)
            
            # Esperar a que todas las combinaciones del lote terminen
            while active_processes:
                time.sleep(10)  # Verificar cada 10 segundos
                completed_keys = []
                
                for key, combo in active_processes.items():
                    if combo["status"] in ["completed", "failed"]:
                        completed_keys.append(key)
                
                # Eliminar combinaciones completadas de la lista de activos
                for key in completed_keys:
                    active_processes.pop(key)
            
            logger.info(f"Lote {i//batch_size + 1} completado")
        
        # Si hay reintentos configurados, procesar los fallidos
        if max_retries > 0:
            logger.info(f"Configurados {max_retries} reintentos para combinaciones fallidas")
            for retry in range(max_retries):
                failed_combinations = [c for c in self.status["combinations"] if c["status"] == "failed"]
                
                if not failed_combinations:
                    logger.info("No hay combinaciones fallidas para reintentar")
                    break
                
                logger.info(f"Reintento {retry+1}/{max_retries}: {len(failed_combinations)} combinaciones")
                
                for combination in failed_combinations:
                    # Restaurar estado a pendiente
                    combination["status"] = "pending"
                    combination["retries"] = combination.get("retries", 0) + 1
                    combination.pop("error", None)
                    combination.pop("preprocessor_result", None)
                    combination.pop("macro_job_id", None)
                    combination.pop("macro_stops_job_id", None)
                    combination.pop("macro_status", None)
                    combination.pop("macro_stops_status", None)
                    combination.pop("start_time", None)
                    combination.pop("end_time", None)
                
                self.save_status()
                
                # Procesar las fallidas (ahora pendientes) por lotes
                self.process_all(batch_size=batch_size, max_retries=0)
        
        # Resultado final
        self._update_counts()
        logger.info("Procesamiento finalizado")
        logger.info(f"Resumen: {self.status['total']} total, "
                   f"{self.status['completed']} completados, "
                   f"{self.status['failed']} fallidos, "
                   f"{self.status['pending']} pendientes")
        
        return self.status
    
    def retry_failed(self):
        """
        Reintenta las combinaciones que fallaron.
        
        Returns:
            Número de combinaciones restablecidas a pendientes
        """
        failed_combinations = [c for c in self.status["combinations"] if c["status"] == "failed"]
        
        if not failed_combinations:
            logger.info("No hay combinaciones fallidas para reintentar")
            return 0
        
        logger.info(f"Reintentando {len(failed_combinations)} combinaciones fallidas")
        
        for combination in failed_combinations:
            # Restaurar estado a pendiente
            combination["status"] = "pending"
            combination["retries"] = combination.get("retries", 0) + 1
            combination.pop("error", None)
            combination.pop("preprocessor_result", None)
            combination.pop("macro_job_id", None)
            combination.pop("macro_stops_job_id", None)
            combination.pop("macro_status", None)
            combination.pop("macro_stops_status", None)
            combination.pop("start_time", None)
            combination.pop("end_time", None)
        
        self.save_status()
        logger.info(f"{len(failed_combinations)} combinaciones restablecidas a pendientes")
        return len(failed_combinations)
    
    def get_status_summary(self):
        """
        Genera un resumen del estado actual.
        
        Returns:
            Diccionario con el resumen del estado
        """
        self._update_counts()
        return {
            "total": self.status["total"],
            "completed": self.status["completed"],
            "failed": self.status["failed"],
            "pending": self.status["pending"],
            "preprocessing": self.status["preprocessing"],
            "processing_macro": self.status["processing_macro"],
            "processing_macro_stops": self.status["processing_macro_stops"],
            "started_at": self.status.get("started_at"),
            "last_updated": self.status["last_updated"]
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestor de procesamiento de datos GTFS")
    parser.add_argument("bucket", help="Nombre del bucket S3")
    parser.add_argument("--region", default="eu-west-1", help="Región de AWS")
    parser.add_argument("--batch-size", type=int, default=5, help="Tamaño del lote para procesamiento")
    parser.add_argument("--max-retries", type=int, default=0, help="Máximo número de reintentos para combinaciones fallidas")
    parser.add_argument("--retry-failed", action="store_true", help="Reintentar combinaciones fallidas")
    parser.add_argument("--lambda-name", help="Nombre de la función Lambda del preprocesador")
    parser.add_argument("--macro-job", help="Nombre del job Glue para macro")
    parser.add_argument("--macro-stops-job", help="Nombre del job Glue para macro_stops")
    parser.add_argument("--combinations-file", default="batch_processing/combinations.json", help="Archivo con combinaciones")
    parser.add_argument("--status-file", default="batch_processing/status.json", help="Archivo de estado")
    
    args = parser.parse_args()
    
    manager = GTFSProcessorManager(
        bucket_name=args.bucket,
        combinations_file=args.combinations_file,
        status_file=args.status_file,
        lambda_name=args.lambda_name,
        macro_job_name=args.macro_job,
        macro_stops_job_name=args.macro_stops_job,
        region=args.region
    )
    
    if args.retry_failed:
        count = manager.retry_failed()
        print(f"Se restablecieron {count} combinaciones fallidas a pendientes")
    
    result = manager.process_all(batch_size=args.batch_size, max_retries=args.max_retries)
    
    print("\nResumen final:")
    print(f"Total de combinaciones: {result['total']}")
    print(f"Completadas: {result['completed']}")
    print(f"Fallidas: {result['failed']}")
    print(f"Pendientes: {result['pending']}")