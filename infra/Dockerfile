FROM python:3.9.10

RUN apt-get install wget
RUN pip install psycopg2 pandas sqlalchemy pyarrow

WORKDIR /app
COPY ingest_data.py .

ENTRYPOINT [ "python", "ingest_data.py" ]