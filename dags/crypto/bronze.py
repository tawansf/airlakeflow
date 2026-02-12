from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

BRONZE_PATH = "/opt/airflow/data/bronze/crypto"

def bronze_ingestao_dados_bitcoin():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_last_updated_at=true"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        data['processed_at'] = datetime.now().isoformat()

        filename = f"bitcoin_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        filepath = os.path.join(BRONZE_PATH, filename)
        os.makedirs(BRONZE_PATH, exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(data, f)
        
        print(f"Dados do Bitcoin salvos em: {filepath}")
    else:
        raise Exception(f"Erro ao obter dados do Bitcoin: {response.status_code}")

with DAG(
    dag_id="01_bronze_ingestao_dados_bitcoin",
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'crypto']
) as dag:
    ingestao_dados_bitcoin = PythonOperator(
        task_id='bronze_ingestao_dados_bitcoin',
        python_callable=bronze_ingestao_dados_bitcoin
    )

    ingestao_dados_bitcoin