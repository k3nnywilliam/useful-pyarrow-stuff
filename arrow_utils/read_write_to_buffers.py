import io
import pyarrow as pa
import pyarrow.parquet as pq

# Write a table to an in-memory buffer
file_path = "your_large_dataset.parquet"
table = pq.read_table(file_path, use_threads=True)

buffer = io.BytesIO()
pq.write_table(table, buffer)

# Read a table from an in-memory buffer
buffer.seek(0)
table_from_buffer = pq.read_table(buffer)
