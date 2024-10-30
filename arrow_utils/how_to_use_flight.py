import pyarrow.flight as fl
import pyarrow as pa

# Sample data to use as an Arrow table
data = {
    "column1": pa.array([1, 2, 3, 4, 5]),
    "column2": pa.array(["a", "b", "c", "d", "e"])
}
table = pa.Table.from_pydict(data)

# Define a Flight server
class MyFlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_get(self, context, ticket):
        return pa.RecordBatchStream(table)

# Initialize and run the server
if __name__ == "__main__":
    location = "grpc://0.0.0.0:8815"
    server = MyFlightServer(location)
    print(f"Flight server listening on {location}")
    server.serve()