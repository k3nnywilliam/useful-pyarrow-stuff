import pyarrow as pa
import pyarrow.parquet as pq

# Define the file path for the Parquet file
file_path = "large_dataset.parquet"

# Define the schema for the dataset
schema = pa.schema([
    ('column1', pa.int64()),
    ('column2', pa.float64()),
    ('column3', pa.string())
])

# Initialize the ParquetWriter with the desired row group size
# Row group size here is in number of rows (e.g., 1,000,000 rows per row group)
row_group_size = 1_000_000
with pq.ParquetWriter(file_path, schema, compression="ZSTD", use_dictionary=True) as writer:
    
    # Simulate batch generation (replace with actual data loading/generation)
    for batch_num in range(10):  # Assuming 10 batches here for example
        # Generate a batch of data (use your actual data source here)
        batch_data = {
            'column1': range(batch_num * 100_000, (batch_num + 1) * 100_000),
            'column2': [x * 0.1 for x in range(100_000)],
            'column3': ["text"] * 100_000
        }
        # Convert to a pyarrow Table
        batch_table = pa.Table.from_pydict(batch_data, schema=schema)
        
        # Write the batch to the Parquet file
        writer.write_table(batch_table, row_group_size=row_group_size)

# Once done, the Parquet file is finalized, and data is stored in larger row groups.
print(f"Data written to {file_path} with row groups of size {row_group_size} rows.")
