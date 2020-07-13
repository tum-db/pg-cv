from database import Database


class BenchmarkQuery:

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config

    def execute(self, timing=False):
        query_type = self.config['query_type']
        query = self.config['query']

        # functions that are benchmarked
        # perform batched insert into stream_pipeline using the records from base_lineitem
        insert_fun = f'{query_type}.{query}_insert'
        # compute query result by refreshing materialized view for upper pipelines
        refresh_fun = f'{query_type}.{query}_refresh'
        # retrieve result
        query_fun = f'{query_type}.{query}_query'

        # start with filling the stream pipeline
        self.db.execute_function(insert_fun, timing=timing)
        self.db.commit()
        # refresh the query result, by executing the upper pipeline
        self.db.execute_function(refresh_fun, timing=timing)
        self.db.commit()

        # warm up before accessing the query result
        self.db.execute_function(query_fun)
        self.db.execute_function(query_fun)
        self.db.execute_function(query_fun)
        # timing for the query
        self.db.execute_function(query_fun, timing=timing)
