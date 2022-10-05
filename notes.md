# Notes

## ETL vs ELT

ETL

- data warehouse solution
- schema on write
- slower to write
- cleaner, and more compliant data in warehouse

ELT

- data lake solution
- schema on read
- faster and more flexible
- can transform into more and more model as and when needed

## ingestion command shortcut

```
python ingest_data.py --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

```

```
docker run --network=week_2_taxi-analytics taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```

```
export GOOGLE_APPLICATION_CREDENTIALS="/Users/weichun/Downloads/data-playground-362712-9b0d10f95bf7.json"
```

## airflow setup

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### no frills version

- removed the redis queue, worker, triggerer & flower services from the docker-compose.yaml file. Be aware that after line 71 you have a dependecy to Redis too. You have to take off!
- changed Core Executor mode from CeleryExecutor to LocalExecutor
- Stopped all containers and cleaned everything with docker-compose down --volumes --rmi all
- Re Run entire program

### airflow manual rerun dags for backfill

```
docker exec -it fcc643493660 bash # into airflow container
airflow dags backfill fhv_taxi_data_v1 --reset-dagruns -s 2019-01-01 -e 2020-01-01
```

## Data Processing

Batch Jobs

- weekly, daily, hourly
- run on python scirpts, sql, spark, flink

When to Use Spark

- bunch of files in data lake -> process it -> data lake
- Cannot run SQL on Data Lake, although now you can with some providers using (hive, presto, athena)
- if you can express job as sql -> then go for hive/presto/athena
- example of non-sql job -> to train a model

Actions vs Transformers
Transformers - lazy (not exec immediately)

- selecting columns
- filtering
- joins
- group by

Actions - eager (exec immediately)

- show, take, head
- write
