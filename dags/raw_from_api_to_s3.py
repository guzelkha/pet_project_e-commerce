from airflow import DAG
from minio import Minio
import logging
import duckdb
import pendulum # для работы с datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# конфигурация DAG
OWNER = "guzelkha"
DAG_ID = "raw_from_api_to_s3"

# используемые таблицы в DAG
LAYER = "raw" # слой данных
SOURCE = "frankfurter"

# S3
MINIO_ACCESS_KEY = "minioadmin" 
MINIO_SECRET_KEY = "minioadmin" 

# настройки по умолчанию для DAG
args = {
    'owner': OWNER,
    'catchup': True, 
    'start_date': pendulum.datetime(2025, 9, 3, tz="Europe/Moscow"), # дата начала работы DAG
    'retries': 1, # количество попыток перезапуска при ошибке
    'retry_delay': pendulum.duration(hours=1), # пауза между попытками
}

def get_dates(**context):
    
    """
    Возвращает даты начала и конца интервала в формате 'YYYY-MM-DD'
    """

    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_api_data_to_s3(**context):
    
    """ 
    Достает данные из Fake Store Api и сохраняет в S3 
    """
    
    # получаем даты из context
    start_date, end_date = get_dates(**context)
    logging.info(f" Start load for dates {start_date}/{end_date}")
    
    con = duckdb.connect()
    
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path'
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = 'minioadmin'
        SET s3_secret_access_key = 'minioadmin'
        SET s3_use_ssl = FALSE;
        
        COPY
        (
            SELECT * FROM
            read_json_auto('https://api.frankfurter.dev/v1/{start_date}?base=USD&symbols=EUR,GBP')
        )
        TO
        's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )
    
    con.close()
    logging.info(f" Downloaded for date {start_date}/{end_date} success")
    
    with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 5 * * *",
        rags=["s3", "raw"], 
        # исключаем параллелизм, чтобы работать локально и не грузить докер:
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
    ) as dag:
        #tasks
        start = EmptyOperator(
            task_id="start"
        )
        
        get_and_transfer_api_data_to_s3 = PythonOperator(
            task_id="get_and_transfer_api_data_to_s3",
            python_callable=get_and_transfer_api_data_to_s3
        )
        
        end = EmptyOperator(
            task_id="end"
        )
        
        start >> get_and_transfer_api_data_to_s3 >> end