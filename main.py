import boto3
import configparser
import logging
import os
import pandas as pd
import re
from botocore.exceptions import ClientError
from transform_data import transform_dataframe

# Verificar si la carpeta 'logs' existe, si no, créala
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configuración del logger
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Cargar configuración
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
aws_access = parser.get("aws_boto_crentials", "access_key")
aws_secret = parser.get("aws_boto_crentials", "secret_key")


def create_s3_client():
    """
    Create and return an S3 client.
    Returns:
        A boto3 client to interact with S3.
    """
    s3 = boto3.client(
        "s3", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret
    )
    return s3


def download_file(bucket: str, file_key: str, download_path: str) -> None:
    """
    Download a file from an S3 bucket.
    Args:
        bucket: The name of the bucket
        file_key: The key of the file in the bucket
        download_path: The path where the file will be downloaded
    Returns: None
    """
    try:
        s3 = create_s3_client()
        s3.download_file(bucket, file_key, download_path)
        logger.info(f"Archivo {file_key} descargado correctamente.")
    except ClientError as e:
        logger.error(f"Error al descargar el archivo: {e}")


def transform_data(input_path: str, output_path: str) -> None:
    """
    Transforms the data from the CSV file.
    Args:
        input_path: The path of the file to transform
        output_path: The path where the transformed data will be saved
    Returns: None
    """
    try:
        # Leer datos
        data = pd.read_csv(input_path)

        # Transformar datos usando la función importada
        data = transform_dataframe(data)

        # Guardar datos transformados
        data.to_csv(output_path, index=False)
        logger.info("Datos transformados y guardados correctamente.")

        if os.path.exists(input_path):
            os.remove(input_path)
            logger.info(f"Archivo local {input_path} eliminado correctamente.")

    except Exception as e:
        logger.error(f"Error al transformar los datos: {e}")


def upload_file(bucket: str, file_key: str, upload_path: str) -> None:
    """
    Upload a file to an S3 bucket.
    Args
        bucket: The name of the bucket
        file_key: The key with which the file will be saved in the bucket
        upload_path: The path of the file to upload
    Returns: None
    """
    try:
        s3 = create_s3_client()

        # Lista de objetos en el bucket
        objects = s3.list_objects_v2(Bucket=bucket)

        # Verificar si el nombre del archivo ya está en uso
        if "Contents" in objects:
            existing_files = [obj["Key"] for obj in objects["Contents"]]
            original_file_key = file_key
            counter = 1
            while file_key in existing_files:
                # Modificar el nombre del archivo
                file_key = re.sub(r"(\.\w+)$", f"_{counter}\\1", original_file_key)
                counter += 1

                logger.warning(f"El archivo {original_file_key} ya existe.")
                logger.warning(f"Cambiando el nombre a {file_key}.")

        # Subir el archivo
        s3.upload_file(upload_path, bucket, file_key)
        logger.info(f"Archivo {file_key} subido correctamente.")

        # Eliminar el archivo local
        if os.path.exists(upload_path):
            os.remove(upload_path)
            logger.info(f"Archivo local {upload_path} eliminado correctamente.")

    except ClientError as e:
        logger.error(f"Error al subir el archivo: {e}")


def main():
    """
    Función principal para descargar, transformar y subir un archivo.
    Returns: None
    """
    # Nombre del bucket donde esta el archivo
    bucket_name = "mybuckettestdk"

    # Nombre del archivo a descargar
    file_key = "Datos_Ejemplos_ETL.csv"

    # Nombre del archivo descargado
    download_path = "downloaded_Datos_Ejemplos_ETL.csv"

    # Nombre del archivo después de la transformacion
    transformed_path = "transformed_data.csv"

    # Descargar archivo
    download_file(bucket_name, file_key, download_path)

    # Transformar datos
    transform_data(download_path, transformed_path)

    # Subir archivo transformado
    upload_file(bucket_name, "transformed_data.csv", transformed_path)


if __name__ == "__main__":
    main()
