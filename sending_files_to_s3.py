import json
import time
import boto3
import random
from datetime import datetime, UTC
from decouple import config
from loguru import logger

# Configurar logger
logger.add(
    "logs/file_{time}.log",
    rotation="1 day",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)

# Configura tu cliente Boto3 con las credenciales adecuadas
s3 = boto3.client("s3")

BUCKET_NAME = config("BUCKET_NAME")
PREFIX = "streaming_jsons/"  # carpeta/prefijo donde se subirán los archivos


def generate_json_record():
    """Genera un registro de ejemplo en formato dict."""
    return {
        "event_time": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": f"user_{random.randint(1, 100)}",
        "action": random.choice(["click", "login", "purchase"]),
        "value": random.random() * 100,
    }


logger.info("Iniciando el proceso de generación y envío de datos a S3")

while True:
    try:
        # Generar uno o varios registros de ejemplo
        logger.debug("Generando nuevos registros")
        data = [generate_json_record() for _ in range(5)]

        # Convertir a JSON string
        json_str = "\n".join(json.dumps(d) for d in data)

        # Nombrar el archivo con timestamp para evitar colisiones
        file_name = f"{PREFIX}events_{int(time.time())}.json"

        # Subir a S3
        logger.info(f"Intentando subir archivo: {file_name}")
        s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json_str)
        logger.success(f"Archivo subido exitosamente: {file_name}")

        # Esperar 2-3 minutos
        sleep_time = random.choice([30, 60, 80])  # 1, 2 o 3 minutos
        logger.info(f"Esperando {sleep_time} segundos antes del próximo envío")
        time.sleep(sleep_time)

    except Exception as e:
        logger.error(f"Error durante la ejecución: {str(e)}")
        raise
