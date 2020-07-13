# check if table is a stream based on the name
def is_stream(table_name: str) -> bool:
    return table_name.startswith('stream')


# construct name for the insert function of a stream
def construct_insert_function_name(stream_name: str) -> str:
    return f'cv.{stream_name}_insert'
