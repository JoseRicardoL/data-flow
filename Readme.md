# Oracle Extraction ETL Pipeline

Este proyecto implementa un pipeline ETL usando AWS Glue para extraer datos de bases de datos Oracle, procesarlos y generar archivos GTFS (General Transit Feed Specification).

## Entorno Cloud

### Requisitos

- AWS CLI configurado con credenciales
- Python 3.6+
- Permisos para desplegar recursos en AWS CloudFormation

### Comandos Principales

```bash
# Validar template CloudFormation
make validate

# Desplegar stack en AWS
make deploy

# Ejecutar job Glue
make test

# Ver logs de ejecución
make logs

# Descargar resultados
make download

# Limpiar recursos
make clean

# Actualizar codigo del job
make update
```

## Entorno de Desarrollo Local

Para facilitar el desarrollo y pruebas, este proyecto incluye un entorno local que emula AWS Glue utilizando Docker con la imagen oficial de AWS.

### Requisitos

- Docker y Docker Compose
- VSCode con extensión Remote - Containers (opcional)
- Credenciales AWS configuradas (para acceso a AWS Secrets Manager)

### Configuración

1. **Iniciar entorno Docker:**

   ```bash
   make local-up
   ```

2. **Acceder a Jupyter:**

   - Abrir http://localhost:8888
   - O usar VSCode con Remote - Containers

3. **Detener entorno:**

   ```bash
   make local-down
   ```

### Desarrollo con VSCode

VSCode puede conectarse directamente al contenedor para desarrollo:

1. Instalar extensión "Remote - Containers"
2. Ejecutar `make local-up` 
3. En VSCode: "Remote-Containers: Reopen in Container"
4. Trabajar con la IDE, con soporte completo para Python, Jupyter y AWS Glue

### Jupyter Notebooks

El entorno incluye Jupyter Lab con un notebook específico para desarrollo interactivo:

- `local/notebooks/oracle_extraction.ipynb`

Este notebook permite:
- Desarrollar y probar código paso a paso
- Conectar a Oracle usando los mismos secretos que en producción
- Visualizar resultados interactivamente
- Explorar datos y consultas SQL

