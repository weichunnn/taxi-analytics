import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):
  user = params.user
  password = params.password
  host = params.host 
  port = params.port 
  db = params.db
  table_name = params.table_name
  url = params.url
  csv_name = 'output.csv'
  parquet_name = 'output.parquet'
  os.system(f"wget {url} -O {parquet_name}")

  # simulate csv file format for backward compatability
  df = pd.read_parquet(parquet_name)
  df.to_csv(csv_name)

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

  df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
  df = next(df_iter)
  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

  df.head(n = 0).to_sql(name=table_name, con=engine, if_exists="replace")
  df.to_sql(name=table_name, con=engine, if_exists="append")

  while True:
    try:
      t_start = time()
      df = next(df_iter)

      df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
      df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
      df.to_sql(name=table_name, con=engine, if_exists="append")
      
      t_end = time()
      duration = round(t_end - t_start, 3)
      print(f'chunk inserted with {duration} seconds')
    except StopIteration:
      print("finished ingesting data into the postgres database")
      break

if __name__ == "__main__":
  # https://docs.python.org/3/library/argparse.html
  parser = argparse.ArgumentParser(description='ingest CSV data to postgres')
  parser.add_argument('--user', required=True, help='user name for postgres')
  parser.add_argument('--password', required=True, help='password for postgres')
  parser.add_argument('--host', required=True, help='host for postgres')
  parser.add_argument('--port', required=True, help='port for postgres')
  parser.add_argument('--db', required=True, help='database name for postgres')
  parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
  parser.add_argument('--url', required=True, help='url of the csv file')

  args = parser.parse_args()
  main(args)