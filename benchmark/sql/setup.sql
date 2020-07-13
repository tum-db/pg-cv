create schema if not exists bv;
grant all on schema bv to postgres;
grant all on schema bv to public;

create schema if not exists cv;
grant all on schema cv to postgres;
grant all on schema cv to public;

create schema if not exists dv;
grant all on schema dv to postgres;
grant all on schema dv to public;

create schema if not exists ev;
grant all on schema ev to postgres;
grant all on schema ev to public;

create schema if not exists fv;
grant all on schema fv to postgres;
grant all on schema fv to public;

create extension if not exists plpython3u;

create table if not exists cv.continuous_views (
    cv_name name not null primary key
);

create table if not exists cv.stream_pipelines (
    cv_name              name not null,
    stream_name          name not null,
    pipeline_name        name not null,
    insert_function_name name not null,
    query                text not null
);

create table if not exists cv.static_pipelines (
    cv_name       name not null,
    pipeline_name name not null
);

create or replace function cv.createContinuousView(cv_name name, query text) returns void
    language plpython3u
as
$$
import sys

init_modules = sys.modules.copy().keys()

src_path = '/src/python/view_generator'
if src_path not in sys.path:
    sys.path.insert(0, '/src/python/view_generator')

import continuous_view

exc_info = None
try:
    continuous_view.create_continuous_view(plpy, cv_name, query)
except:
    exc_info = sys.exc_info()

for m in sys.modules.copy().keys():
    if m not in init_modules:
        del (sys.modules[m])

if exc_info is not None:
    raise exc_info[0].with_traceback(exc_info[1], exc_info[2])
$$;


create or replace function cv.createContinuousViewFromFile(cv_name name, filename text) returns void
    language plpython3u
as
$$
import sys

init_modules = sys.modules.copy().keys()

src_path = '/src/python/view_generator'
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import continuous_view

exc_info = None
try:
    continuous_view.create_continuous_view_from_file(plpy, cv_name, filename)
except:
    exc_info = sys.exc_info()

for m in sys.modules.copy().keys():
    if m not in init_modules:
        del (sys.modules[m])

if exc_info is not None:
    raise exc_info[0].with_traceback(exc_info[1], exc_info[2])
$$;
