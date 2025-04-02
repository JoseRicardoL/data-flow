#!/usr/bin/env python3
import json
import sys
import os


def generate_args(
    job_name, bucket, state_input_file, task_input_file, parameters_file, output_file
):
    """
    Genera args.json basado en los archivos de Step Functions.

    Args:
        job_name: Nombre del trabajo Glue
        bucket: Nombre del bucket S3
        state_input_file: Ruta al archivo de State Input
        task_input_file: Ruta al archivo de Task Input
        parameters_file: Ruta al archivo de Parameters
        output_file: Ruta donde se guardará el args.json
    """
    args_dict = {
        "--JOB_NAME": job_name,
        "--bronze_bucket": bucket,
        "--S3_BUCKET": bucket,
    }

    # Procesar Task Input (prioridad más alta)
    if os.path.exists(task_input_file):
        try:
            with open(task_input_file, "r", encoding="utf-8") as f:
                task_input = json.load(f)

            if "Arguments" in task_input and isinstance(task_input["Arguments"], dict):
                for key, value in task_input["Arguments"].items():
                    args_dict[key] = value
                print(
                    f"Argumentos extraídos de Task Input: {len(task_input['Arguments'])} parámetros"
                )
        except Exception as e:
            print(f"Error al procesar Task Input: {e}")

    # Procesar State Input (como respaldo)
    if os.path.exists(state_input_file):
        try:
            with open(state_input_file, "r", encoding="utf-8") as f:
                state_input = json.load(f)

            # Extraer campos clave y agregarlos como argumentos si no existen
            key_fields = [
                "P_EMPRESA",
                "P_VERSION",
                "P_CONTR",
                "temp_dir",
                "execution_id",
            ]
            for field in key_fields:
                if field in state_input and f"--{field}" not in args_dict:
                    args_dict[f"--{field}"] = state_input[field]

            print(f"Verificados campos clave de State Input")
        except Exception as e:
            print(f"Error al procesar State Input: {e}")

    # Procesar Parameters (menor prioridad)
    if os.path.exists(parameters_file):
        try:
            with open(parameters_file, "r", encoding="utf-8") as f:
                parameters = json.load(f)

            if "Arguments" in parameters and isinstance(parameters["Arguments"], dict):
                # Los parámetros suelen contener referencias, pero podemos usar sus claves
                for key in parameters["Arguments"]:
                    # Eliminar el sufijo ".$" si existe y verificar si el argumento no existe
                    clean_key = key.replace(".$", "")
                    if clean_key not in args_dict:
                        # Buscamos el valor en el State Input
                        if os.path.exists(state_input_file):
                            with open(state_input_file, "r", encoding="utf-8") as f:
                                state_input = json.load(f)

                            # Si el parámetro hace referencia a un campo del state input, usamos ese valor
                            param_name = clean_key.strip("--")
                            if param_name in state_input:
                                args_dict[clean_key] = state_input[param_name]

            print(f"Verificados parámetros de Parameters")
        except Exception as e:
            print(f"Error al procesar Parameters: {e}")

    # Verificar que tengamos todos los argumentos necesarios
    required_args = [
        "--P_EMPRESA",
        "--P_VERSION",
        "--P_CONTR",
        "--temp_dir",
        "--execution_id",
        "--bronze_bucket",
    ]

    missing_args = [arg for arg in required_args if arg not in args_dict]
    if missing_args:
        print(
            f"ADVERTENCIA: Faltan los siguientes argumentos: {', '.join(missing_args)}"
        )

    # Guardar archivo de salida
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(args_dict, f, indent=2)
        print(f"Archivo de argumentos generado: {output_file}")
    except Exception as e:
        print(f"Error al escribir el archivo de argumentos: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(
            "Uso: python generate_args.py <job_name> <bucket> <state_input_file> <task_input_file> <parameters_file> <output_file>"
        )
        sys.exit(1)

    job_name = sys.argv[1]
    bucket = sys.argv[2]
    state_input_file = sys.argv[3]
    task_input_file = sys.argv[4]
    parameters_file = sys.argv[5]
    output_file = sys.argv[6]

    generate_args(
        job_name,
        bucket,
        state_input_file,
        task_input_file,
        parameters_file,
        output_file,
    )
