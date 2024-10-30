import os
import pyarrow as pa
import pyarrow.parquet as pq
import psutil

def can_use_mmap(file_path):
    # Get file size and available memory
    file_size = os.path.getsize(file_path)
    available_memory = psutil.virtual_memory().available

    # Ensure file fits within available memory
    if file_size > available_memory * 0.8:  # Keeping a buffer
        print("Warning: File size exceeds 80% of available memory. Using fallback read.")
        return False
    return True

def safe_read_parquet(file_path):
    try:
        # Check if file size is suitable for memory mapping
        if can_use_mmap(file_path):
            with pa.memory_map(file_path, "r") as mmap_file:
                table = pq.read_table(mmap_file)
        else:
            # Fallback: Use non-mmap read if the file is too large
            table = pq.read_table(file_path, use_threads=True)
    except Exception as e:
        print(f"Error occurred with memory mapping: {e}. Falling back to non-mmap read.")
        table = pq.read_table(file_path)  # Fallback read

    return table

# Usage
file_path = "your_large_dataset.parquet"
table = safe_read_parquet(file_path)
print(table)
