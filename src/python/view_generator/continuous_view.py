import sqlparse

from database_access import DatabaseAccess
from function_generator import FunctionGenerator
from parse_cte import parse_cte, PipelineType
from stream import construct_insert_function_name


def create_continuous_view(plpy, cv_name: str, query: str) -> None:
    # wrap plpy object
    db = DatabaseAccess(plpy)

    # register continuous view, will fail if another view with the same name exists
    db.continuous_views_insert(cv_name)

    # extract common table expressions and the actual query (ordering of the ctes does not change)
    parsed_query = sqlparse.parse(query)
    query_string, ctes = parse_cte(parsed_query)

    upper_pipelines = []

    # new schema for the view (add to search path allows to use same table names)
    db.create_schema(f'cv_{cv_name}')
    db.add_to_search_path(f'cv_{cv_name}')

    for cte_name, cte in ctes.items():
        pipeline_name = f'cv_{cv_name}.{cte_name}'

        if cte.pipeline_type == PipelineType.STREAM:
            stream_name = cte.get_stream()

            # construct the insert function for the stream
            function_name = construct_insert_function_name(stream_name)

            # create an auxiliary table for aggregates
            db.create_stream_pipeline(pipeline_name, cte.query.normalized)

            # store stream query for later use
            db.stream_pipelines_insert(cv_name, stream_name, pipeline_name, function_name, cte.query.normalized)

            # if we group by attributes create an index on the primary keys
            if len(cte.primary_keys()) > 0:
                db.create_index(f'cv_{cv_name}_{cte_name}_index', pipeline_name, cte.primary_keys())

            # construct the insert function for the stream
            declarations, body = FunctionGenerator().generate_stream_insert_function(f'cv_{cv_name}', stream_name,
                                                                                     pipeline_name, cte)

            # create insert function
            db.create_insert_function(function_name, stream_name, declarations, body)

        elif cte.pipeline_type == PipelineType.STATIC:
            # evaluate static pipelines and save them
            db.create_static_pipeline(pipeline_name, cte.query.normalized)
            # create index on static pipeline
            if len(cte.keys_for_index()) > 0:
                db.create_index(f'cv_{cv_name}_{cte_name}_index', pipeline_name, cte.keys_for_index())

        elif cte.pipeline_type == PipelineType.UPPER:
            # merge all upper pipelines
            upper_pipelines.append(cte)

    # create query from all upper pipelines
    db.create_upper_pipeline(f'cv.{cv_name}', upper_pipelines, query_string)


def create_continuous_view_from_file(plpy, cv_name: str, filename: str) -> None:
    # read file (replace all newlines)
    file = open(filename, 'r')
    query = ' '.join(file.read().replace('\n', ' ').split())
    create_continuous_view(plpy, cv_name, query)
