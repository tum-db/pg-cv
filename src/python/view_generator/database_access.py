from typing import List

from parse_cte import CommonTableExpression


# wrap expressions for PostgreSQL
class DatabaseAccess:

    def __init__(self, plpy):
        self.plpy = plpy

    # insert continuous view into registry table
    def continuous_views_insert(self, view_name):
        plan_continuous_views_insert = self.plpy.prepare('insert into cv.continuous_views values ($1);', ['name'])
        self.plpy.execute(plan_continuous_views_insert, [view_name])

    # insert some meta-information for stream pipelines
    def stream_pipelines_insert(self, cv_name, stream_name, pipeline_name, insert_function, query):
        plan_stream_pipelines_insert = self.plpy.prepare('insert into cv.stream_pipelines values ($1, $2, $3, $4, $5);',
                                                         ['name', 'name', 'name', 'name', 'text'])
        self.plpy.execute(plan_stream_pipelines_insert, [cv_name, stream_name, pipeline_name, insert_function, query])

    # register static pipelines
    def static_pipelines_insert(self, cv_name, pipeline_name):
        plan_static_pipelines_insert = self.plpy.prepare('insert into cv.static_pipelines values ($1, $2);',
                                                         ['name', 'name'])
        self.plpy.execute(plan_static_pipelines_insert, [cv_name, pipeline_name])

    # create schema (drop existing one)
    def create_schema(self, cv_name):
        plan = f'drop schema if exists {cv_name} cascade;\n' \
               f'create schema {cv_name};\n' \
               f'grant all on schema {cv_name} to postgres;\n' \
               f'grant all on schema {cv_name} to public;'
        self.plpy.execute(plan)

    # update PostgreSQL's search path
    def add_to_search_path(self, cv_name):
        plan = f'select set_config(\'search_path\', \'{cv_name},\'||current_setting(\'search_path\'), true);'
        self.plpy.execute(plan)

    # create an auxiliary table for the stream pipeline
    def create_stream_pipeline(self, pipeline_name, query):
        plan = f'create table if not exists {pipeline_name} as {query};\n' \
               f'truncate table {pipeline_name}'
        self.plpy.execute(plan)

    # create index for given table
    def create_index(self, index_name: str, table_name: str, keys: List[str]):
        key_string = ','.join(keys)
        plan = f'create index {index_name} on {table_name} ({key_string});'
        self.plpy.execute(plan)

    # create a static pipeline using materialized views
    def create_static_pipeline(self, pipeline_name, query):
        plan = f'create materialized view if not exists {pipeline_name} as {query};'
        self.plpy.execute(plan)

    # create an upper pipeline (join query with all upper ctes)
    def create_upper_pipeline(self, pipeline_name: str, ctes: List[CommonTableExpression], query: str) -> None:
        str_ctes = ''
        # reconstruct the original query but only with upper pipelines
        for cte in ctes:
            str_ctes = f'{str_ctes}{"," if str_ctes != "" else ""}\n    {cte.name.normalized} as {cte.query.normalized}'
        str_ctes = f'with {str_ctes}\n' if str_ctes != "" else f''

        # create the materialized viow for all upper pipelines
        plan = f'create materialized view if not exists {pipeline_name} as\n' \
               f'{str_ctes}' \
               f'{query};'
        self.plpy.execute(plan)

    # create an insert function from declared variables and the body
    def create_insert_function(self, function_name, stream_name, declarations, body):
        # initialise the search_path for later reset
        plan = f'create or replace function {function_name}(entries {stream_name}[])\n' \
               f'   returns void\n' \
               f'   language  plpgsql\n' \
               f'as\n' \
               f'$$\n' \
               f'declare\n' \
               f'search_path text := current_setting(\'search_path\');\n' \
               f'{declarations}\n' \
               f'begin\n' \
               f'{body}\n' \
               f'end\n' \
               f'$$;'
        self.plpy.execute(plan)
