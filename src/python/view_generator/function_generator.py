from aggregate_functions import get_aggregate_function
from parse_cte import CommonTableExpression


class FunctionGenerator:

    # generate an insert function for a stream pipeline
    def generate_stream_insert_function(self, cv_name: str, stream_name: str, pipeline_name: str,
                                        cte: CommonTableExpression):
        # name of the input array, containing the tuples to insert
        entries = f'entries'

        # tuple is the aggregate on the new tuple (i.e., in entries)
        tuple = f'{pipeline_name.replace(".", "_")}_tuple'
        # found_tuple is the existing aggregate in the auxiliary table
        found_tuple = f'{pipeline_name.replace(".", "_")}_found_tuple'

        declarations = f'{tuple} {pipeline_name};\n' \
                       f'{found_tuple} {pipeline_name};'

        attributes = cte.attributes()
        select_expressions = cte.select_expressions()
        primary_keys = cte.primary_keys()

        # we find/update an existing aggregate using the primary keys
        where = ''
        for index, primary_key in enumerate(primary_keys):
            where = f'{where}{" and" if where != "" else ""} {pipeline_name}.{primary_key} = {tuple}.{primary_key}'
        # handle the case that we do not group for an attribute
        where = f' where{where}' if where != '' else f''

        # statement to update the existing aggregate
        update = ''
        for index, attribute in enumerate(attributes):
            if attribute not in primary_keys:
                aggregate_function = get_aggregate_function(select_expressions[index]) \
                    .evaluate(f'{tuple}.{attribute}', f'{found_tuple}.{attribute}', where != '')
                update = f'{update}{"," if update != "" else ""} {attribute} = {aggregate_function}'
        update = f'update {pipeline_name} set {update}{where};' if update != '' else f''

        # 1. update the search_path (no table renaming needed)
        # 2. compute stream query for new tuples
        # 3. find existing aggregate
        # 4. a) if not found: insert new aggregate
        # 5. b) otherwise: update existing aggregate value
        # 6. reset search_path
        body = f'perform set_config(\'search_path\', \'{cv_name}, \' || search_path, true);\n' \
               f'for {tuple} in with {stream_name} as (select * from unnest({entries})) {cte.query.normalized}\n' \
               f'loop\n' \
               f'   select * into {found_tuple} from {pipeline_name}{where};\n' \
               f'\n' \
               f'   if not found then\n' \
               f'       insert into {pipeline_name} select {tuple}.*;\n' \
               f'   else\n' \
               f'       {update}\n' \
               f'   end if;\n' \
               f'end loop;\n' \
               f'perform set_config(\'search_path\', search_path, true);'

        return declarations, body
