#!/bin/sh

batch_sizes=(1000)
iterations=10

user=postgres
password=123456
host=localhost
port=5432

./data/truncate_last.sh
for i in "${batch_sizes[@]}"; do
  make up &

  until PGPASSWORD=${password} psql -h ${host} -p ${port} -U ${user} -c '\l'; do
    echo >&2 "$(date +%Y%m%dt%H%M%S) Postgres is unavailable - sleeping"
    sleep 30
  done

  BENCHMARK_SQL=("truncate table main.runtimes;" \
		"call main.main('clear');" \
		"call main.main('setup');" \
		"call main.main('benchmark_all ${iterations} ${i}');" \
		"call main.main('clear');" \
		"\copy main.runtimes to runtimes_${i}.csv csv header;")

  for sql in "${BENCHMARK_SQL[@]}"; do
		echo "${sql}"
		PGPASSWORD=${password} psql -h ${host} -p ${port} -U ${user} -d continuous-view -c "${sql}"
	done

  make down
done
