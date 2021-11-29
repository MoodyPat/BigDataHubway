from airflow import DAG
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator

from datetime import datetime, timedelta
import os
import pandas as pd
import math
import numpy as np

args = {
	'owner': 'airflow'
}

dag = DAG('Test54', default_args=args, description='Simple Example DAG',
schedule_interval='56 18 * * *',
start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)
		
create_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow/',
    directory='hubway_raw',
    dag=dag,
)

create_output_dir = CreateDirectoryOperator(
    task_id='create_output_dir',
    path='/home/airflow/',
    directory='hubway_output',
    dag=dag,
)

clear_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/hubway_raw',
    pattern='*',
    dag=dag,
)

clear_output_dir = ClearDirectoryOperator(
    task_id='clear_output_dir',
    directory='/home/airflow/hubway_output',
    pattern='*',
    dag=dag,
)

download_files = BashOperator(
    task_id='download_csv_files',
    bash_command = "kaggle datasets download -d acmeyer/hubway-data -p /home/airflow/hubway_raw --unzip",
    dag=dag,
)

def copy2HDFS():
	print('Looping through files')
	for filename in os.listdir("/home/airflow/hubway_raw/"):
		if 'hubway-tripdata' in filename:
			filemonth = filename.split('-')[0]
			print(f'Found file {filename}')
			dir_raw = f'/user/hadoop/hubway/raw/{filemonth}'
			dir_final = f'/user/hadoop/hubway/final/{filemonth}'
			
			print(f'\tCreating directory {dir_raw}')
			hdfs_create_directory_raw = HdfsMkdirFileOperator(
				task_id='mkdir_hdfs_files',
				directory=dir_raw,
				hdfs_conn_id='hdfs',
				dag=dag,
			).execute('')
			print(f'\tCreating directory {dir_final}')
			hdfs_create_directory_final = HdfsMkdirFileOperator(
				task_id='mkdir_hdfs_files',
				directory=dir_final,
				hdfs_conn_id='hdfs',
				dag=dag,
			).execute('')
			print(f'\tCopying file to HDFS (/user/hadoop/hubway/raw/{filemonth}/{filename})')
			hdfs_put_files = HdfsPutFileOperator(
				task_id='upload_files_to_hdfs',
				local_file=f'/home/airflow/hubway_raw/{filename}',
				remote_file=f'/user/hadoop/hubway/raw/{filemonth}/{filename}',
				hdfs_conn_id='hdfs',
				dag=dag,
			).execute('')
			
copy_files_to_hdfs = PythonOperator(
    task_id='copy_files_to_hdfs',
    python_callable=copy2HDFS,
)

pyspark_convert_data = SparkSubmitOperator(
	task_id='pyspark_convert_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/convert_data.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_convert_data',
	application_args=['--hdfs_source_dir', '/user/hadoop/hubway/raw', '--hdfs_target_dir', '/user/hadoop/hubway/final', '--hdfs_target_format', 'csv'],
    verbose=True,
    dag = dag
)

pyspark_create_excel = SparkSubmitOperator(
	task_id='pyspark_create_excel',
    conn_id='spark',
    application='/home/airflow/airflow/python/create_excel.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_convert_data',
	application_args=['--hdfs_source_dir', '/user/hadoop/hubway/final', '--hdfs_target_dir', '/user/hadoop/hubway/final'],
    verbose=True,
    dag = dag
)


	


create_import_dir >> clear_import_dir >> create_output_dir >> clear_output_dir >> download_files >> copy_files_to_hdfs >> pyspark_convert_data >> pyspark_create_excel >> dummy_op