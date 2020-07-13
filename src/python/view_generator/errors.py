# Some errors that can occur
class Errors:
    MULTIPLESTREAMS = RuntimeError("Join between multiple ungrouped streams not supported!")
    NOGROUPBY = RuntimeError("Streams must be grouped!")
    COULDNOTPARSE = RuntimeError("Could not parse the given query")
