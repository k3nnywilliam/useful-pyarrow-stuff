'''
The TaskGroup API enables parallel processing in pyarrow, which is particularly useful when reading or writing Parquet files in parallel. 
This allows you to take advantage of multi-core CPUs for faster processing.
'''

import pyarrow.parquet as pq

from pyarrow import fs
from pyarrow.dataset import TaskGroup

filesystem = fs.LocalFileSystem()
group = TaskGroup()

# Define tasks (e.g., reading multiple files in parallel)
for path in ["file1.parquet", "file2.parquet", "file3.parquet"]:
    group.append(lambda path=path: pq.read_table(path, filesystem=filesystem))

# Execute tasks in parallel
results = group.finish()
