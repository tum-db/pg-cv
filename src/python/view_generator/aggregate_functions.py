from sqlparse import sql

from parsesql_helper import find_first_instance


class AggregateFunction:
    # create expression for joining new aggregate value with an existing value
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        pass


class MaxFunction(AggregateFunction):
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        return f'greatest({new_attribute_name}, {old_attribute_name})'


class MinFunction(AggregateFunction):
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        return f'least({new_attribute_name}, {old_attribute_name})'


class CountFunction(AggregateFunction):
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        return f'{new_attribute_name} + {old_attribute_name}'


class SumFunction(AggregateFunction):
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        # if we aggregate the table without grouping for an attribute the sum of an empty table is not 0 but NULL
        # when we join the computed value with the existing result, we have to handle this case:
        # 1. if both are null: the result must also be null
        # 2. if both are not null: the result must be the sum of the two values
        # 3. if one is null: return the other one (unchanged)
        if exists_group:
            return f'{new_attribute_name} + {old_attribute_name}'
        else:
            return f'coalesce(coalesce({new_attribute_name}, 0) + {old_attribute_name}, ' \
                   f'{new_attribute_name} + coalesce({old_attribute_name}, 0))'


class DefaultFunction(AggregateFunction):
    def evaluate(self, new_attribute_name: str, old_attribute_name: str, exists_group: bool) -> str:
        return f'{new_attribute_name}'


# map for assigning AggregateFunctions to names
function_map = {'max': MaxFunction(), 'min': MinFunction(), 'count': CountFunction(), 'sum': SumFunction()}


def get_aggregate_function(select_expression: sql.Token) -> AggregateFunction:
    # find sql function (i.e. max, min, count, sum)
    function = select_expression if isinstance(select_expression, sql.Function) else find_first_instance(
        select_expression, sql.Function)

    if function is not None:
        identifier = find_first_instance(function, sql.Identifier)
        if identifier is not None:
            identifier_name = identifier.normalized.lower()
            if identifier_name in function_map:
                return function_map[identifier_name]

    return DefaultFunction()
