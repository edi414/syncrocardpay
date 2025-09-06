import os
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError

from utils.logger import setup_logger

logger = setup_logger("s3_utils")


def _get_s3_client():
    region = os.getenv("AWS_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def list_s3_files(bucket: str, prefix: str = "") -> List[str]:
    """Lista os arquivos (apenas nomes de arquivo) em um bucket/prefixo S3.

    Retorna somente o nome do arquivo (basename) para manter compatibilidade
    com a lógica existente que compara por `file_name`.
    """
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    result_files: List[str] = []

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                result_files.append(os.path.basename(key))
    except ClientError as e:
        logger.error(f"Erro ao listar arquivos no S3: {e}")

    return result_files


def download_s3_file(bucket: str, key: str, local_path: str) -> bool:
    """Baixa um objeto do S3 para um caminho local."""
    s3 = _get_s3_client()
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)
        return True
    except ClientError as e:
        logger.error(f"Erro ao baixar {bucket}/{key} para {local_path}: {e}")
        return False


def upload_s3_file(bucket: str, key: str, local_path: str) -> bool:
    """Faz upload de um arquivo local para o S3."""
    s3 = _get_s3_client()
    try:
        s3.upload_file(local_path, bucket, key)
        return True
    except ClientError as e:
        logger.error(f"Erro ao enviar {local_path} para {bucket}/{key}: {e}")
        return False


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Converte um s3://bucket/prefix/key em (bucket, key)."""
    if not uri.startswith("s3://"):
        raise ValueError("URI S3 inválida")
    without_scheme = uri[len("s3://"):]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


