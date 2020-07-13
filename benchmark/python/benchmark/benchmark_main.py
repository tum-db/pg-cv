from benchmark_query import BenchmarkQuery
from clear import ClearViews, ClearQuery, ClearPublic
from compare_query import CompareQuery
from database import Database
from setup import SetupPublic, SetupViews, SetupQuery
from timing import Timing


# remove all possible side effects of a query
def clear_query(db: Database, config: dict, query_type: str, query: str):
    config['query_type'] = query_type
    config['query'] = query

    ClearQuery(db, config).execute()
    ClearViews(db, config).execute()
    db.commit()


# setup query and corresponding auxiliary tables needed for the maintenance approach
def setup_query(db: Database, config: dict, query_type: str, query: str):
    config['query_type'] = query_type
    config['query'] = query

    SetupViews(db, config).execute()
    SetupQuery(db, config).execute()
    db.commit()


# benchmark a query and clear the result after that
def benchmark(db: Database, config: dict, query_type: str, query: str, timing=False):
    clear_query(db, config, query_type, query)
    setup_query(db, config, query_type, query)
    config['query_type'] = query_type
    config['query'] = query

    BenchmarkQuery(db, config).execute(timing)

    clear_query(db, config, query_type, query)
    db.commit()


# check if the result of all maintenance approaches is identical
def compare(db: Database, config: dict, query: str):
    for query_type in config['query_types']:
        clear_query(db, config, query_type, query)
    for query_type in config['query_types']:
        setup_query(db, config, query_type, query)
    config['query'] = query

    CompareQuery(db, config).execute()

    for query_type in config['query_types']:
        clear_query(db, config, query_type, query)
    db.commit()


def main(plpy, args: str):
    # some config parameter
    # the map is passed down to the functions
    config = {'src': '/benchmark/sql',
              'dataset': 'tpch',
              'query_types': ['bv', 'cv', 'dv', 'ev', 'fv'],
              'queries': ['q1', 'q3', 'q6', 'q15', 'q20'],
              'batch_size': 1000,
              'max_batch_size': 10000}

    # perform some basic argument parsing
    args = args.split()
    operation = args[0]

    timing = Timing(config)
    db = Database(plpy, timing)

    # load the TPC-H relations
    if operation == 'setup':
        SetupPublic(db, config).execute()
        db.commit()

    # create auxiliary tables, views and functions for the given maintenance approach
    elif operation == 'setup_query' and len(args) == 3:
        clear_query(db, config, args[1], args[2])
        setup_query(db, config, args[1], args[2])

    # check for correctness of the given query
    elif operation == 'compare' and len(args) == 2:
        compare(db, config, args[1])

    # check correctness of all available queries
    elif operation == 'compare_all' and len(args) == 2:
        config['batch_size'] = int(args[1])
        for query in config['queries']:
            compare(db, config, query)

    # benchmark the given query for the obtained batch_size
    elif operation == 'benchmark' and len(args) == 3:
        benchmark(db, config, args[1], args[2], True)
        timing.save(db)

    # benchmark all queries and all maintenance approaches for the given batch size
    elif operation == 'benchmark_all' and len(args) == 3:
        config['batch_size'] = int(args[2])
        for query_type in config['query_types']:
            for query in config['queries']:

                # warmup (discard first three iterations)
                benchmark(db, config, query_type, query, False)
                benchmark(db, config, query_type, query, False)
                benchmark(db, config, query_type, query, False)

                for i in range(int(args[1])):
                    benchmark(db, config, query_type, query, True)

        # write execution times to the database
        timing.save(db)

    # clear everything, including TPC-H relations
    elif operation == 'clear':
        for query_type in config['query_types']:
            for query in config['queries']:
                clear_query(db, config, query_type, query)
        ClearPublic(db, config).execute()
        db.commit()

    else:
        raise RuntimeError('Missing arguments!')
