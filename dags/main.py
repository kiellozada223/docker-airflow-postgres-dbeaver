from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
import os

dag_path = os.getcwd()

def transform_data():
    booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
    hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

    df = pd.merge(booking, client, on='client_id')
    df.rename(columns={'name':'client_name', 'type':'client_type'}, inplace=True)

    data = pd.merge(df, hotel, on='hotel_id')
    data.rename(columns={'name':'hotel_name'}, inplace=True)
    
    data.booking_date = pd.to_datetime(data.booking_date, format='mixed')

    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace('EUR', 'GBP', inplace=True)

    data = data.drop('address',axis=1)

    data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)

default_args = {
    'owner': 'mike',
    'start_date': datetime(2023,11,9),
    'email_on_failure':False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

ingestion_dag = DAG(
    'booking_ingestion_v3',
    default_args=default_args,
    description='Aggregate booking records',
    schedule_interval=None,
)

transform = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_data,
    dag = ingestion_dag
)

load = PostgresOperator(
    task_id = 'create_postgres_table',
    postgres_conn_id = 'postgres_localhost',
    sql="""
        CREATE TABLE IF NOT EXISTS booking_record (
            client_id INT NOT NULL,
            booking_date TEXT NOT NULL,
            room_type VARCHAR(100) NOT NULL,
            hotel_id INT NOT NULL,
            booking_cost NUMERIC,
            currency TEXT,
            age NUMERIC,
            client_name VARCHAR(100),
            client_type VARCHAR(100),
            hotel_name VARCHAR(100)
        );
        """,
    autocommit = True,
    dag=ingestion_dag,
)

store = PostgresOperator(
    task_id = 'copy_data_to_postgres',
    postgres_conn_id = 'postgres_localhost',
    sql = """
            COPY booking_record FROM '/Users/mike/airflow/airflow-docker-postgre3/processed_data/processed_data.csv' DELIMITER ',' CSV HEADER;
            """,
    autocommit = True,
    dag=ingestion_dag,
)

transform >> load >> store