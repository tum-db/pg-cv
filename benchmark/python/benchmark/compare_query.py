from database import Database


# compare the results of the queries
class CompareQuery:

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config

    def execute(self):
        query_types = self.config['query_types']
        query = self.config['query']

        # execute queries and keep results
        for query_type in query_types:
            functions = [f'{query_type}.{query}_insert', f'{query_type}.{query}_refresh']
            for function in functions:
                self.db.execute_function(function)

        # compare always pairs of different approaches
        index = 0
        while index + 1 < len(query_types):
            equal = self.db.is_result_equal(f'{query_types[index]}.{query}_query',
                                            f'{query_types[index + 1]}.{query}_query')
            if equal:
                self.db.notice(f'{query_types[index]}.{query} and {query_types[index + 1]}.{query} are equal!')
            else:
                raise RuntimeError(f'{query_types[index]}.{query} and {query_types[index + 1]}.{query} are not equal!')

            index = index + 1
