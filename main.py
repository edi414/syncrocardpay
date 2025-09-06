from ftplib import FTP_TLS
import os
from datetime import datetime

from utils.logger import setup_logger
from utils.connection_db import (
    get_file_processing_status
)
from utils.s3_utils import list_s3_files, download_s3_file, upload_s3_file
from scripts.leitor_extratos import (
    analyze_files_to_process,
    process_file
)

host = os.getenv('HOST')
user = os.getenv('FTPS_USER')
password = os.getenv('FTPS_PASSWORD')
ftps_port = int(os.getenv('FTPS_PORT', '21'))
local_directory = os.path.dirname(os.path.abspath(__file__))
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX', '')
# Na Lambda, usar /tmp para logs
if os.path.exists('/var/task'):  # Detecta se está rodando na Lambda
    log_directory = "/tmp"
    log_filename = os.path.join(log_directory, f"log_{datetime.now().strftime('%d%m%y_%H_%M_%S')}.txt")
else:
    log_directory = os.path.join(local_directory, "outputs", "log")
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(log_directory, f"log_{datetime.now().strftime('%d%m%y_%H_%M_%S')}.txt")
logger = setup_logger("main", level=20, log_file=log_filename)  # 20 = INFO level

connection_database = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT')
}

def main():
    sftp_files = []
    ftps = None
    try:
        try:
            ftps = FTP_TLS(host)
            ftps.login(user=user, passwd=password)
            ftps.prot_p()
            
            logger.info("Conexão FTPS estabelecida com sucesso.")

            ftps.cwd("/Saida")
            sftp_files = ftps.nlst()
            logger.info(f"Arquivos encontrados no SFTP: {len(sftp_files)}")
            
        except Exception as e:
            logger.warning(f"Não foi possível conectar ao SFTP: {e}")
            logger.info("Continuando apenas com sincronização via S3")

        if not S3_BUCKET:
            logger.error("S3_BUCKET não configurado. Defina S3_BUCKET e S3_PREFIX no ambiente.")
            return

        s3_files = list_s3_files(S3_BUCKET, S3_PREFIX)
        logger.info(f"Arquivos encontrados no S3: {len(s3_files)}")

        db_status = get_file_processing_status(**connection_database)
        logger.info(f"Arquivos registrados no banco: {len(db_status)}")

        files_to_process, files_to_report = analyze_files_to_process(
            sftp_files, 
            s3_files, 
            db_status
        )

        if files_to_report:
            logger.warning("Arquivos desincronizados encontrados:")
            for report in files_to_report:
                logger.warning(f"- {report['file']}: {report['message']}")

        for file_name in files_to_process:
            logger.info(f"Processando arquivo: {file_name}")

            # Na Lambda, usar /tmp para arquivos temporários
            if os.path.exists('/var/task'):  # Detecta se está rodando na Lambda
                local_file_path = os.path.join("/tmp", file_name)
            else:
                local_file_path = os.path.join(local_directory, file_name)

            # Tenta obter do S3 primeiro
            if file_name in s3_files:
                key = f"{S3_PREFIX}/{file_name}".lstrip('/')
                if download_s3_file(S3_BUCKET, key, local_file_path):
                    logger.info(f"Arquivo baixado do S3 para processamento: {file_name}")
                else:
                    logger.error(f"Falha ao baixar do S3: {file_name}")
                    continue
            # Se não estiver no S3, tenta baixar do SFTP
            elif ftps and file_name in sftp_files:
                try:
                    with open(local_file_path, 'wb') as local_file:
                        ftps.retrbinary(f'RETR {file_name}', local_file.write)
                    logger.info(f"Arquivo baixado do SFTP para processamento: {file_name}")
                except Exception as e:
                    logger.error(f"Erro ao baixar arquivo do SFTP: {file_name} - {e}")
                    continue
            else:
                logger.warning(f"Arquivo não encontrado no S3 nem no SFTP: {file_name}")
                continue

            # Caminho remoto alvo (S3)
            remote_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{file_name}"
            success = process_file(file_name, local_file_path, remote_path, connection_database, is_tryout=False)

            if not success:
                os.remove(local_file_path)
                logger.error(f"Erro ao processar arquivo {file_name}")

    except Exception as e:
        logger.error(f"Erro ao executar o processo: {e}")
    finally:
        if ftps:
            ftps.quit()

def lambda_handler(event, context):
    """AWS Lambda entrypoint"""
    main()
    return {"status": "ok"}