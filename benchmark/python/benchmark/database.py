from typing import List

from timing import Timing


class Database:

    def __init__(self, plpy, timing: Timing):
        self.plpy = plpy
        self.timing = timing

    # execute a file and replace all placeholders with values from the given dict
    def execute_file(self, filepath: str, values: dict):
        sql_file = open(filepath, 'r')
        query = sql_file.read()
        query = query.replace('${', '{')
        query = query.format(**values)
        self.plpy.execute(query)

    # perform a commit
    def commit(self):
        self.plpy.commit()

    # send a notice
    def notice(self, msg):
        self.plpy.notice(msg)

    # execute a PlPgSQL-Function, the execution time can be recorded using the parameter timing
    def execute_function(self, function: str, arguments: List[str] = [], timing=False):
        args = ','.join(arguments)
        if timing:
            # record timing using explain analyze
            query = f'explain (analyze, format json) select {function}({args});'
            r = self.plpy.execute(query)[0]
            # store timing results
            self.timing.add_time(function, r)
        else:
            query = f'select {function}({args});'
            self.plpy.execute(query)

    # check if the result of the two given functions is identical
    def is_result_equal(self, function1: str, function2: str, arguments1: List[str] = [],
                        arguments2: List[str] = []) -> bool:
        args1 = ','.join(arguments1)
        args2 = ','.join(arguments2)
        query1 = f'select * from {function1}({args1})'
        query2 = f'select * from {function2}({args2})'
        comp1 = f'{query1} except {query2};'
        comp2 = f'{query2} except {query1};'

        result1 = self.plpy.execute(comp1)
        result2 = self.plpy.execute(comp2)

        # perform subset check in both directions, if successful, the results are identical
        return len(result1) == 0 and len(result2) == 0

    # insert values into given table
    def insert_into(self, table: str, values: List):
        values = ','.join(values)
        query = f'insert into {table} values {values};'
        self.notice(query)
        self.plpy.execute(query)
