import json


class Timing:

    def __init__(self, config: dict):
        self.results = {}
        for query_type in config['query_types']:
            for query in config['queries']:
                self.results[f'{query_type}.{query}'] = {'insert': [], 'refresh': [], 'query': []}

    # add a timing result
    def add_time(self, label: str, execution_time: str):
        time = float(json.loads(execution_time['QUERY PLAN'])[0]['Execution Time'])

        result = self.results[label.split('_')[0]]
        result[label.split('_')[1]].append(time)

    # save the timing results to main.runtimes and commit them
    def save(self, db):
        values = []
        for key in self.results:
            query_type = key.split('.')[0]
            query = key.split('.')[1]
            runtimes_insert = self.results[key]['insert']
            runtimes_refresh = self.results[key]['refresh']
            runtimes_query = self.results[key]['query']

            for i in range(len(runtimes_insert)):
                values.append(
                    f'(\'{query_type}\', \'{query}\', {runtimes_insert[i]}, {runtimes_refresh[i]}, {runtimes_query[i]})')

        db.insert_into('main.runtimes', values)
        db.commit()
