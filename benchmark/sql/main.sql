create extension if not exists plpython3u;

drop schema if exists main cascade;
create schema main;
grant all on schema main to postgres;
grant all on schema main to public;

create table if not exists main.runtimes (
    query_type      text    not null,
    query           text    not null,
    runtime_insert  numeric not null,
    runtime_refresh numeric not null,
    runtime_query   numeric not null
);

create or replace procedure main.main(operation text)
    language plpython3u
as
$$
import sys

init_modules = sys.modules.copy().keys()

src_path = '/benchmark/python/benchmark'
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import benchmark_main

exc_info = None
try:
    benchmark_main.main(plpy, operation)
except:
    exc_info = sys.exc_info()

for m in sys.modules.copy().keys():
    if m not in init_modules:
        del (sys.modules[m])

if exc_info is not None:
    raise exc_info[0].with_traceback(exc_info[1], exc_info[2])
$$;