from os import path

from database import Database


# setup tables for metadata and generation functions
class SetupViews:

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config

    def execute(self):
        src = self.config['src']

        self.db.execute_file(path.join(src, 'setup.sql'), self.config)


# load TPC-H relations
class SetupPublic:

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config

    def execute(self):
        src = self.config['src']
        dataset = self.config['dataset']

        self.db.execute_file(path.join(src, dataset, 'setup.sql'), self.config)


# load a single query using the given maintenance strategy
class SetupQuery:

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config

    def execute(self):
        src = self.config['src']
        dataset = self.config['dataset']
        query_type = self.config['query_type']
        query = self.config['query']

        # first, perform setup, i.e., generate auxiliary table and insert functions
        # then, register benchmark functions that perform batched inserts, refresh the query result and query it
        files = ['setup.sql', 'insert.sql', 'refresh.sql', 'query.sql']

        for file in files:
            if path.isfile(path.join(src, dataset, query_type, file)):
                self.db.execute_file(path.join(src, dataset, query_type, file), self.config)
            else:
                self.db.execute_file(path.join(src, dataset, query_type, query, file), self.config)
