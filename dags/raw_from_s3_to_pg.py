from airflow import DAG
import logging
import duckdb
import pendulum # для работы с datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import variable

# конфигурация DAG
OWNER = "guzelkha"
DAG_ID = "raw_from_s3_to_pg"

# используемые таблицы в DAG
LAYER = "raw" # слой данных
SOURCE = "frankfurter"
SCHEMA = "ods"
TARGET_TABLE = "fct_frankfurter"

# S3
MINIO_ACCESS_KEY = "minioadmin" 
MINIO_SECRET_KEY = "minioadmin" 

#DUCKDB
PASSWORD = "postgres"


# настройки по умолчанию для DAG
args = {
    'owner': OWNER,
    'catchup': True, 
    'start_date': pendulum.datetime(2025, 8, 3, tz="Europe/Moscow"), # дата начала работы DAG
    'retries': 3, # количество попыток перезапуска при ошибке
    'retry_delay': pendulum.duration(minutes=10), # пауза между попытками
}

def get_dates(**context):
    
    """
    Возвращает даты начала и конца интервала в формате 'YYYY-MM-DD'
    """

    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_raw_data_to_ods_pg(**context):
    """ """
    # получаем даты из context
    start_date, end_date = get_dates(**context)
    logging.info(f" Start load for dates {start_date}/{end_date}")
    
    con = duckdb.connect()   
    
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = 'minioadmin';
        SET s3_secret_access_key = 'minioadmin';
        SET s3_use_ssl = FALSE;
        
        CREATE SECRET dw_postgres (
            TYPE postgres,
            HOST 'postgres_dwh'
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD 'postgres',
        );
        
        ATTACH '' AS dwh_postgres_db (TYPE postgres,
            HOST 'postgres_dwh'
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD 'postgres')
            
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        SELECT * FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """
    )
    
    con.close()
    logging.info(f"Download for date {start_date} success")
    
with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 5 * * *", # 0 — минуты (0) 5 — часы (5 утра) * — любой день месяца * — любой месяц * — любой день недели
    tags=["s3", "ods", "pg"], 
    # исключаем параллелизм, чтобы работать локально и не грузить докер:
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    #tasks
    start = EmptyOperator(
        task_id="start",
    )
    
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000, #длительность работы сенсора
        poke_interval=60, #частота проверки
    )
    
    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_raw_data_to_ods_pg
    )
    
    end = EmptyOperator(
        task_id="end"
    )
    
    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end
    
