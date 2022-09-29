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
