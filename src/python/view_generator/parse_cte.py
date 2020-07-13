from collections import OrderedDict

from sqlparse.tokens import Keyword, Name

from errors import Errors
from parse_query import get_tables_in_from, get_tables_in_rest, get_attributes, get_select_expressions, \
    get_group_by_expressions
from parsesql_helper import find_first_ttype, compare_tokens
from stream import is_stream


class PipelineType:
    STREAM = 'stream pipeline'
    STATIC = 'static pipeline'
    UPPER = 'upper pipeline'


# Wrapper object for common table expressions
class CommonTableExpression:
    def __init__(self, name, query):
        self.name = name
        self.query = query
        self.pipeline_type = PipelineType.STATIC

    def __str__(self):
        name = self.name.normalized
        pipeline_type = self.pipeline_type
        query = self.query.normalized
        return f'<CTE name="{name}", pipeline_type="{pipeline_type}", query="{query}">'

    # get name of cte
    def pipeline_name(self):
        if self.name.ttype == Name:
            return self.name.normalized
        else:
            return find_first_ttype(self.name, Name).normalized

    def get_stream(self):
        for table in self.tables_in_from():
            if is_stream(table):
                return table

        return None

    def tables_in_from(self):
        return get_tables_in_from(self.query)

    def tables_in_rest(self):
        return get_tables_in_rest(self.query)

    def attributes(self):
        return get_attributes(self.name, self.query)

    def select_expressions(self):
        return get_select_expressions(self.query)

    def group_by_expressions(self):
        return get_group_by_expressions(self.query)

    def primary_keys(self):
        attributes = self.attributes()
        select_expressions = self.select_expressions()
        group_by_expressions = self.group_by_expressions()
        primary_keys = []

        # attributes in the select statement that are also in the group by stream are the primary keys for the
        # auxiliary table
        for group_by_expression in group_by_expressions:
            index = None
            for i in range(len(select_expressions)):
                if compare_tokens(select_expressions[i], group_by_expression):
                    index = i
                    break

            if index is not None:
                primary_keys.append(attributes[index])

        return primary_keys

    def keys_for_index(self):
        attributes = self.attributes()
        group_by_expressions = self.group_by_expressions()

        if len(group_by_expressions) == 0:
            return attributes
        else:
            return self.primary_keys()


# Construct a CTE object from query
def _construct_cte(parsed_query):
    found_as = False
    name = None
    query = None

    # perform some check and sanitize cte
    for token in parsed_query.tokens:
        if token.is_whitespace:
            continue
        if name is None and not found_as and (token.is_group or token.ttype == Name):
            name = token
        elif token.is_keyword and token.normalized == 'AS':
            found_as = True
        elif query is None and found_as and token.is_group:
            query = token
        else:
            raise Errors.COULDNOTPARSE

    if name is None or query is None:
        raise Errors.COULDNOTPARSE

    return CommonTableExpression(name, query)


def parse_cte(parsed_query):
    # store ctes in the order they occur
    ctes = OrderedDict()

    found_with = False
    cte_tokens = None
    query_string = ''

    for token in parsed_query[0].tokens:
        # Look for 'with' keyword
        if token.ttype == Keyword.CTE:
            found_with = True
        # First group after 'with' contains common table expressions
        elif found_with and token.is_group:
            cte_tokens = token
            found_with = False
        # Add rest of query to query_string
        elif not found_with or cte_tokens is not None:
            query_string = f'{query_string}{token.value}'

    # No common table expression in query
    if cte_tokens is None:
        return []

    multiple_cte = True

    # Check if only one common table expression exists
    for token in cte_tokens.tokens:
        if token.is_keyword and token.normalized == 'AS':
            multiple_cte = False

    if multiple_cte:
        for token in cte_tokens:
            # Filter whitespaces and commas
            if token.is_group:
                cte = _construct_cte(token)
                ctes[cte.pipeline_name()] = cte
    else:
        cte = _construct_cte(cte_tokens)
        ctes[cte.pipeline_name()] = cte

    # identify pipeline type for all common table expressions
    for cte_name, cte in ctes.items():
        for table in cte.tables_in_from():
            # test if one table starts with 'stream'
            if is_stream(table):
                cte.pipeline_type = PipelineType.STREAM
                break
            elif table in ctes:
                # check if table is upper, i.e., depends on a stream or another upper pipeline
                if ctes[table].pipeline_type == PipelineType.STREAM or ctes[table].pipeline_type == PipelineType.UPPER:
                    cte.pipeline_type = PipelineType.UPPER

        if cte.pipeline_type == PipelineType.STREAM or cte.pipeline_type == PipelineType.UPPER:
            continue

        # handle subqueries
        for table in cte.tables_in_rest():
            if table in ctes and \
                    (ctes[table].pipeline_type == PipelineType.STREAM or
                     ctes[table].pipeline_type == PipelineType.UPPER):
                cte.pipeline_type = PipelineType.UPPER

    return query_string, ctes
