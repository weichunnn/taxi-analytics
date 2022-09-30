## ETL vs ELT

ETL

- data warehouse solution
- schema on write

ELT

- data lake solution
- schema on read

## airflow setup

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### no frills version

- removed the redis queue, worker, triggerer & flower services from the docker-compose.yaml file. Be aware that after line 71 you have a dependecy to Redis too. You have to take off!
- changed Core Executor mode from CeleryExecutor to LocalExecutor
- Stopped all containers and cleaned everything with docker-compose down --volumes --rmi all
- Re Run entire program

## airflow manual rerun dags for backfill

```
docker exec -it fcc643493660 bash # into airflow container
airflow dags backfill fhv_taxi_data_v1 --reset-dagruns -s 2019-01-01 -e 2020-01-01
```
