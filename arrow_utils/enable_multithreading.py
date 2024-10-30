import pyarrow.parquet as pq

table = pq.read_table("large_dataset.parquet", use_threads=True)