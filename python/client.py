from pyarrow import flight
from google.protobuf.any_pb2 import Any

import FlightSql_pb2

class Client:
    def __init__(self, url):
        self.conn =  flight.connect(url)


    def get_sql(self, query):
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
        return reader.read_pandas()
