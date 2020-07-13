from sqlparse import sql
from sqlparse.tokens import Name, Punctuation, Text, Keyword

from parsesql_helper import find_first_instance, find_first_ttype, find_last_ttype


# get all table names in the from statement
def get_tables_in_from(parsed_query):
    tables = []
    found_from = False
    found_table_name = False

    table_group = None

    for token in parsed_query.tokens:
        # search until 'from' is found
        if token.is_keyword and token.normalized == 'FROM':
            found_from = True

        # First group after 'FROM' contains the tables
        elif found_from and token.is_group:
            table_group = token
            break

    # return empty list if no 'from' is found
    if table_group is None:
        return tables

    # Find all table names after 'FROM'
    for subtoken in table_group.tokens:
        # Table name in from statement (ignore it after AS)
        if subtoken.ttype == Name and not found_table_name:
            tables.append(subtoken.normalized)
            found_table_name = True
        elif find_first_ttype(subtoken, Name) is not None and not found_table_name:
            tables.append(find_first_ttype(subtoken, Name).normalized)
            found_table_name = True
        # Another stream might be referenced in a join
        elif subtoken.ttype == Punctuation and subtoken.normalized == ',':
            found_table_name = False

    return tables


# search all tokens in query for subqueries
def get_tables_in_rest(parsed_query):
    tables = []

    for token in parsed_query.tokens:
        if token.is_group:
            tables += get_tables_in_from(token)
            tables += get_tables_in_rest(token)

    return tables


# get names of attributes from query
# either extract from list after cte name
# or from the select statement in the query
def get_attributes(name_token, query_token):
    attributes = []
    identifier_list = None

    # search for attribute names after cte name
    parenthesis = find_first_instance(name_token, sql.Parenthesis)
    if parenthesis is not None:
        identifier_list = find_first_instance(parenthesis, (sql.Identifier, sql.IdentifierList))

    # if found something split it into a list and return
    if isinstance(identifier_list, sql.IdentifierList):
        for identifier in identifier_list:
            if identifier.ttype != Punctuation:
                attributes.append(identifier.value)
        return attributes

    elif isinstance(identifier_list, sql.Identifier):
        attributes.append(identifier_list.value)
        return attributes

    # search for attributes names in the query
    # find select statement
    identifier_list = find_first_instance(query_token, (sql.Identifier, sql.IdentifierList))

    # extract attribute names from select statement
    if isinstance(identifier_list, sql.IdentifierList):
        for identifier in identifier_list:
            if isinstance(identifier, sql.Identifier):
                name = find_last_ttype(identifier, Name)
                if name is not None:
                    attributes.append(name.normalized)

    elif isinstance(identifier_list, sql.Identifier):
        name = find_last_ttype(identifier_list, Name)
        if name is not None:
            attributes.append(name.normalized)

    return attributes


# return a list of statements in the select part of the query
def get_select_expressions(query_token):
    expressions = []

    identifier_list = find_first_instance(query_token, (sql.Identifier, sql.IdentifierList))

    if isinstance(identifier_list, sql.IdentifierList):
        for identifier in identifier_list:
            if identifier.ttype == Punctuation or identifier.ttype == Text.Whitespace:
                continue
            elif isinstance(identifier, sql.Identifier):
                tokens = []
                for token in identifier.tokens:
                    if token.ttype == Text.Whitespace and tokens == []:
                        continue
                    elif token.ttype != Text.Whitespace:
                        tokens.append(token)
                    elif token.ttype == Text.Whitespace:
                        break

                expressions.append(sql.Identifier(tokens))
            else:
                expressions.append(identifier)
    elif isinstance(identifier_list, sql.Identifier):
        expressions.append(identifier_list.tokens[0])

    return expressions


# get expressions in the group by statement of the query
def get_group_by_expressions(query_token):
    expressions = []
    found_group_by = False
    items = None

    for subtoken in query_token.tokens:
        if subtoken.ttype == Keyword and subtoken.normalized == 'GROUP BY':
            found_group_by = True
        elif found_group_by and not subtoken.ttype == Text.Whitespace:
            items = subtoken
            break

    if isinstance(items, sql.IdentifierList):
        for identifier in items:
            if identifier.ttype == Punctuation or identifier.ttype == Text.Whitespace:
                continue
            else:
                expressions.append(identifier)
    elif items is not None:
        expressions.append(items)

    return expressions
