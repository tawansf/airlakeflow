from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

GEEKO_URL_API = os.getenv("GEEKO_URL_API")
BRONZE_PATH = f'{os.getenv("BRONZE_PATH")}/crypto'

def bronze_ingestion_data_bitcoin():
    url = GEEKO_URL_API
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        data['processed_at'] = datetime.now().isoformat()

        filename = f"bitcoin_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        filepath = os.path.join(BRONZE_PATH, filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(data, f)
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_datawarehouse')
        insert_sql = """INSERT INTO bronze.bitcoin_raw (payload) VALUES (%s);"""
        pg_hook.run(insert_sql, parameters=[json.dumps(data)])

        print(f'Sucesso: Arquivo {filepath} criado e dados inseridos no banco de dados.')
    else:
        raise Exception(f"Erro ao obter dados do Bitcoin: {response.status_code}")

with DAG(
    dag_id="01_bronze_ingestion_data_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'crypto', 'ingestion']
) as dag:
    ingestion_data_bitcoin = PythonOperator(
        task_id='bronze_ingestion_data_bitcoin',
        python_callable=bronze_ingestion_data_bitcoin
    )

    ingestion_data_bitcoin