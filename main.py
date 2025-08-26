from ftplib import FTP_TLS
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from utils.logger import setup_logger
from utils.connection_db import (
    get_google_drive_files,
    get_file_processing_status
)
from scripts.leitor_extratos import (
    analyze_files_to_process,
    process_file
)
import shutil

host = os.getenv('HOST')
user = os.getenv('FTPS_USER')
password = os.getenv('FTPS_PASSWORD')
ftps_port = int(os.getenv('FTPS_PORT', '21'))
local_directory = os.path.dirname(os.path.abspath(__file__))
google_drive_directory = os.getenv('GOOGLE_DRIVE_DIRECTORY')
log_directory = os.path.join(local_directory, "outputs", "log")
os.makedirs(log_directory, exist_ok=True)
log_filename = os.path.join(log_directory, f"log_{datetime.now().strftime('%d%m%y_%H_%M_%S')}.txt")
logger = setup_logger("main", level=logging.INFO, log_file=log_filename)

connection_database = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT')
}

def main():
    sftp_files = []
    try:
        try:
            ftps = FTP_TLS(host)
            ftps.login(user=user, passwd=password)
            ftps.prot_p()
            
            logger.info("Conexão FTPS estabelecida com sucesso.")

            ftps.cwd("/Saida")
            sftp_files = ftps.nlst()
            logger.info(f"Arquivos encontrados no SFTP: {len(sftp_files)}")
            
            ftps.quit()
        except Exception as e:
            logger.warning(f"Não foi possível conectar ao SFTP: {e}")
            logger.info("Continuando apenas com sincronização do Google Drive")

        google_drive_files = get_google_drive_files(google_drive_directory)
        logger.info(f"Arquivos encontrados no Google Drive: {len(google_drive_files)}")

        db_status = get_file_processing_status(**connection_database)
        logger.info(f"Arquivos registrados no banco: {len(db_status)}")

        files_to_process, files_to_report = analyze_files_to_process(
            sftp_files, 
            google_drive_files, 
            db_status
        )

        if files_to_report:
            logger.warning("Arquivos desincronizados encontrados:")
            for report in files_to_report:
                logger.warning(f"- {report['file']}: {report['message']}")

        for file_name in files_to_process:
            logger.info(f"Processando arquivo: {file_name}")

            local_file_path = os.path.join(local_directory, file_name)
            google_drive_path = os.path.join(google_drive_directory, file_name)

            if file_name in google_drive_files:
                shutil.copy2(google_drive_path, local_file_path)
                logger.info(f"Arquivo copiado do Google Drive para processamento: {file_name}")
            else:
                logger.warning(f"Arquivo não encontrado no Google Drive: {file_name}")
                continue

            success = process_file(file_name, local_file_path, google_drive_path, connection_database, is_tryout=False)

            if not success:
                os.remove(local_file_path)
                logger.error(f"Erro ao processar arquivo {file_name}")

    except Exception as e:
        logger.error(f"Erro ao executar o processo: {e}")

if __name__ == "__main__":
    main()