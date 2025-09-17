FROM apache/airflow:2.10.2
RUN pip install duckdb==1.2.2 minio requests