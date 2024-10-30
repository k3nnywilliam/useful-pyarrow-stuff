import pyarrow.parquet as pq

dataset = pq.ParquetFile("large_dataset.parquet")
for batch in dataset.iter_batches(batch_size=10000):
    # Process each batch (a RecordBatch)
    print(batch)