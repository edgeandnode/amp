from pyarrow import flight
from google.protobuf.any_pb2 import Any

from . import FlightSql_pb2

class Client:
    def __init__(self, url):
        self.conn =  flight.connect(url)

    # If `read_all` is `True`, returns a `pyarrow.Table` with the complete result set. This is a
    # convenience for small result sets that don't need to be streamed.
    # 
    # If `read_all` is `False`, returns a generator of `pyarrow.RecordBatch`. This is suitable for
    # streaming larger result sets.
    def get_sql(self, query, read_all=False):
        # Create a CommandStatementQuery message
        command_query = FlightSql_pb2.CommandStatementQuery()
        command_query.query = query


        # Wrap the CommandStatementQuery in an Any type
        any_command = Any()
        any_command.Pack(command_query)
        cmd = any_command.SerializeToString()


        flight_descriptor = flight.FlightDescriptor.for_command(cmd)
        info = self.conn.get_flight_info(flight_descriptor)
        reader = self.conn.do_get(info.endpoints[0].ticket)

        if read_all:
            return reader.read_all()
        else:
            return self._batch_generator(reader)

    def _batch_generator(self, reader):
        while True:
            try:
                chunk = reader.read_chunk()
                yield chunk.data
            except StopIteration:
                break
