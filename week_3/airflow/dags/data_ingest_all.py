import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    else:
      table = pv.read_csv(src_file)
      pq.write_table(table, dest_file)

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download_parquetize_upload(dag, url_template, local_csv_path_template, local_parquet_path_template, gcs_path_template):
  with dag:
    # switched to parquet file 
    download_dataset_task = BashOperator(
      task_id="download_dataset_task",
      bash_command=f"curl -sSLf {url_template} > {local_parquet_path_template}"
    )

    # format_to_parquet_task = PythonOperator(
    #   task_id="format_to_parquet_task",
    #   python_callable=format_to_parquet,
    #   op_kwargs={
    #       "src_file": local_csv_path_template,
    #       "dest_file": local_parquet_path_template,
    #   },
    # )

    local_to_gcs_task = PythonOperator(
      task_id="local_to_gcs_task",
      python_callable=upload_to_gcs,
      op_kwargs={
          "bucket": BUCKET,
          "object_name": gcs_path_template,
          "local_file": local_parquet_path_template,
      },
    )

    cleanup_task = BashOperator(
      task_id="cleanup_task",
      bash_command=f"rm {local_parquet_path_template}"
    )

    download_dataset_task >> local_to_gcs_task >> cleanup_task


def download_csv_parquetize_upload(dag, url_template, local_csv_path_template, local_parquet_path_template, gcs_path_template):
  with dag:
    # switched to parquet file 
    download_dataset_task = BashOperator(
      task_id="download_dataset_task",
      bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
    )

    format_to_parquet_task = PythonOperator(
      task_id="format_to_parquet_task",
      python_callable=format_to_parquet,
      op_kwargs={
          "src_file": local_csv_path_template,
          "dest_file": local_parquet_path_template,
      },
    )

    local_to_gcs_task = PythonOperator(
      task_id="local_to_gcs_task",
      python_callable=upload_to_gcs,
      op_kwargs={
          "bucket": BUCKET,
          "object_name": gcs_path_template,
          "local_file": local_parquet_path_template,
      },
    )

    cleanup_task = BashOperator(
      task_id="cleanup_task",
      bash_command=f"rm {local_parquet_path_template} {local_csv_path_template}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup_task


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

YELLOW_URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
YELLOW_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
YELLOW_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
YELLOW_GCS_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data_dag = DAG(
  dag_id="yellow_taxi_data_v1",
  default_args=default_args,
  schedule_interval="0 6 2 * *",
  start_date=datetime(2019, 1, 1),
  end_date=datetime(2020, 1, 1),
  catchup=True,
  max_active_runs=3,
  tags=['dtc-de'],
) 

download_parquetize_upload(
  dag=yellow_taxi_data_dag, 
  url_template=YELLOW_URL_TEMPLATE, 
  local_csv_path_template=YELLOW_CSV_FILE_TEMPLATE, 
  local_parquet_path_template=YELLOW_PARQUET_FILE_TEMPLATE, 
  gcs_path_template=YELLOW_GCS_TEMPLATE
)

GREEN_URL_TEMPLATE = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
GREEN_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
GREEN_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
GREEN_GCS_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_data_dag = DAG(
  dag_id="green_taxi_data_v1",
  default_args=default_args,
  schedule_interval="0 7 2 * *",
  start_date=datetime(2019, 1, 1),
  end_date=datetime(2020, 1, 1),
  catchup=True,
  max_active_runs=3,
  tags=['dtc-de'],
) 

download_parquetize_upload(
  dag=green_taxi_data_dag, 
  url_template=GREEN_URL_TEMPLATE, 
  local_csv_path_template=GREEN_CSV_FILE_TEMPLATE, 
  local_parquet_path_template=GREEN_PARQUET_FILE_TEMPLATE, 
  gcs_path_template=GREEN_GCS_TEMPLATE
)

FHV_URL_TEMPLATE = URL_PREFIX + "/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
FHV_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
FHV_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
FHV_GCS_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_taxi_data_dag = DAG(
  dag_id="fhv_taxi_data_v1",
  default_args=default_args,
  schedule_interval="0 8 2 * *",
  start_date=datetime(2019, 1, 1),
  end_date=datetime(2020, 1, 1),
  catchup=True,
  max_active_runs=3,
  tags=['dtc-de'],
) 

download_parquetize_upload(
  dag=fhv_taxi_data_dag, 
  url_template=FHV_URL_TEMPLATE, 
  local_csv_path_template=FHV_CSV_FILE_TEMPLATE, 
  local_parquet_path_template=FHV_PARQUET_FILE_TEMPLATE, 
  gcs_path_template=FHV_GCS_TEMPLATE
)



ZONES_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/taxi_zone_lookup.csv"
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/taxi_zone_lookup.parquet"
ZONES_GCS_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"

zones_data_dag = DAG(
  dag_id="zones_data_v1",
  default_args=default_args,
  schedule_interval="@once",
  start_date=days_ago(1),
  catchup=True,
  max_active_runs=3,
  tags=['dtc-de'],
) 

download_csv_parquetize_upload(
  dag=zones_data_dag, 
  url_template=ZONES_URL_TEMPLATE, 
  local_csv_path_template=ZONES_CSV_FILE_TEMPLATE, 
  local_parquet_path_template=ZONES_PARQUET_FILE_TEMPLATE, 
  gcs_path_template=ZONES_GCS_TEMPLATE
)
