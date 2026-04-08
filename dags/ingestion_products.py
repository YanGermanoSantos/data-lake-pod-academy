import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configurações e Constantes Globais
SUBJECT = 'products'
AWS_CONN_ID = 'aws'
POSTGRES_CONN_ID = 'postgres'
BUCKET_NAME = 'pod-academy-lake-370943306683'


@dag(
    dag_id='products_ingestion_taskflow',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['pod_academy', 'data_lake', 'ingestion', 'products'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
)

def sales_ingestion_pipeline():

    # 1. Extrai os dados do banco e salva localmente
    @task
    def extract_db_data(**context) -> str:
        # Usa a data lógica da execução (idempotente) em vez de datetime.now() global
        logical_date = context['logical_date']
        ref = logical_date.strftime('%Y%m%d')
        ts_proc = logical_date.strftime('%Y%m%d%H%M%S')
        
        filename = f'{SUBJECT}_{ref}_{ts_proc}'
        query = f'SELECT * FROM public.{SUBJECT}'

        # Criação da engine e extração
        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        connection_string = f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        engine = create_engine(connection_string)
        
        df = pd.read_sql(query, engine)
        
        # Salva o arquivo localmente
        path = f'/tmp/{filename}.csv'
        df.to_csv(path, index=False)

        # O retorno da função envia o valor automaticamente para o XCom
        return filename

    # 2. Faz o upload do arquivo para o S3
    @task
    def ingest_data_to_s3(filename: str):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        prefix = f'0000_ingestion/{SUBJECT}/{filename}.csv'
        
        try:
            s3_hook.load_file(
                filename=f'/tmp/{filename}.csv',
                key=prefix,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            logging.info(f'Arquivo {filename}.csv enviado com sucesso para s3://{BUCKET_NAME}/{prefix}')
        except Exception as e:
            logging.error(f'Erro ao enviar arquivo para S3: {e}')
            raise e  # Propaga o erro para o Airflow marcar a task como falha

    # O XCom transitando como argumento gera a dependência automática entre as tasks
    extracted_filename = extract_db_data()
    ingest_data_to_s3(extracted_filename)

# Invoca a função para o Airflow registrar a DAG
sales_ingestion_pipeline()