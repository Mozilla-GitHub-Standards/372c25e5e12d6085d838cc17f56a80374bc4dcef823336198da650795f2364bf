FROM postgres:10

RUN apt-get update && apt-get install -y postgresql-10-hll
COPY ./hll.sql /docker-entrypoint-initdb.d/hll.sql
