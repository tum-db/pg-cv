FROM postgres:12

ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD 123456
ENV POSTGRES_DB continuous-view

COPY benchmark/sql/main.sql /docker-entrypoint-initdb.d/

RUN apt-get update
RUN apt-get install -y --no-install-recommends python3 python3-pip "postgresql-plpython3-$PG_MAJOR"

COPY requirements.txt .
RUN pip3 install -r /requirements.txt
