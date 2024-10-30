'''
You can apply different compression settings to achieve a balance between file size and read/write performance. 
For example, ZSTD is often a good balance between speed and compression ratio,
while Snappy is faster for lighter compression.
'''

import pyarrow.parquet as pq

file_path = "your_large_dataset.parquet"
table = pq.read_table(file_path, use_threads=True)
pq.write_table(table, "output.parquet", compression="ZSTD", compression_level=10)
