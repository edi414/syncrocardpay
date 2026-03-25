import psycopg2
from datetime import datetime
from scripts.reading_tricard import ExtratoTricard, parse_tricard_date
from utils.logger import setup_logger

logger = setup_logger("leitor_tricard")

# Mapeamento tipo de arquivo -> tabela destino
FILE_TYPE_TABLE = {
    'VENDA': 'tricard_vendas',
    'FINANCEIRO': 'tricard_financeiro',
    'SALDO': 'tricard_saldos',
}


def detect_tricard_type(file_name):
    """Detecta o tipo de arquivo TRICARD pelo nome"""
    if '_VENDA.' in file_name:
        return 'VENDA'
    elif '_FINANCEIRO.' in file_name:
        return 'FINANCEIRO'
    elif '_SALDO.' in file_name:
        return 'SALDO'
    return None


def insert_df_to_db(conn, schema, table, df):
    """Insere DataFrame no banco usando conexão compartilhada"""
    records = [tuple(x) for x in df.to_numpy()]
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.executemany(query, records)


def register_file_processing(conn, file_name, data_geracao, status, error=None, s3_uri=None):
    """Registra processamento na controle_arquivos e retorna file_id"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unica_transactions.controle_arquivos
            (nome_arquivo, data_geracao, data_processamento, status_processamento,
            erro_processamento, arquivo_google_drive_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (file_name, data_geracao, datetime.now(), status, error, s3_uri)
        )
        return cur.fetchone()[0]


def process_tricard_file(file_name, local_file_path, s3_uri, connection_params, is_tryout=False):
    """Processa um arquivo TRICARD (VENDA, FINANCEIRO ou SALDO)"""
    conn = None
    file_type = detect_tricard_type(file_name)

    if not file_type:
        logger.error(f"Tipo TRICARD não reconhecido no arquivo: {file_name}")
        return False

    table_name = FILE_TYPE_TABLE[file_type]

    try:
        conn = psycopg2.connect(
            host=connection_params['host'],
            port=connection_params['port'],
            user=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )
        conn.autocommit = False

        # Parse do arquivo
        extrato = ExtratoTricard(file_path=local_file_path, file_type=file_type)
        header, df = extrato.process_file()

        if header is None:
            error_msg = f"Falha ao parsear header do arquivo {file_name}"
            logger.error(error_msg)
            register_file_processing(conn, file_name, datetime.now().date(), 'ERRO', error_msg, s3_uri)
            conn.commit()
            return False

        # Extrair data de geração do header
        data_geracao_str = header.get('data_emissao', '')
        data_geracao = parse_tricard_date(data_geracao_str) or datetime.now().date()

        # Se não há registros de detalhe, registrar como sucesso (arquivo vazio é normal)
        if df is None or df.empty:
            logger.info(f"Arquivo {file_name} sem registros de detalhe - registrando como SUCESSO")
            register_file_processing(conn, file_name, data_geracao, 'SUCESSO', None, s3_uri)
            conn.commit()

            if not is_tryout:
                _upload_to_s3(s3_uri, local_file_path)

            return True

        # Registrar processamento → file_id
        file_id = register_file_processing(conn, file_name, data_geracao, 'SUCESSO', None, s3_uri)

        if not file_id:
            raise Exception("Falha ao registrar processamento do arquivo")

        # Adicionar file_id ao DataFrame
        df['file_id'] = file_id

        # Inserir no banco
        insert_df_to_db(conn, 'unica_transactions', table_name, df)
        logger.info(f"{len(df)} registros inseridos na tabela {table_name} do arquivo {file_name}")

        conn.commit()

        if not is_tryout:
            _upload_to_s3(s3_uri, local_file_path)

        return True

    except Exception as e:
        if conn:
            conn.rollback()

        error_msg = str(e)
        logger.error(f"Erro ao processar arquivo TRICARD {file_name}: {error_msg}")

        # Registrar erro com nova conexão
        try:
            err_conn = psycopg2.connect(
                host=connection_params['host'],
                port=connection_params['port'],
                user=connection_params['user'],
                password=connection_params['password'],
                database=connection_params['database']
            )
            err_conn.autocommit = True
            register_file_processing(err_conn, file_name, datetime.now().date(), 'ERRO', error_msg, s3_uri)
            err_conn.close()
        except Exception as register_error:
            logger.error(f"Erro ao registrar erro de processamento: {register_error}")

        return False

    finally:
        if conn:
            conn.close()


def _upload_to_s3(s3_uri, local_file_path):
    """Upload do arquivo para S3"""
    try:
        from utils.s3_utils import parse_s3_uri, upload_s3_file
        if isinstance(s3_uri, str) and s3_uri.startswith('s3://'):
            bucket, key = parse_s3_uri(s3_uri)
            if upload_s3_file(bucket, key, local_file_path):
                logger.info(f"Arquivo enviado para S3: {s3_uri}")
            else:
                logger.error(f"Falha ao enviar para S3: {s3_uri}")
    except Exception as e:
        logger.error(f"Erro no upload S3: {e}")
