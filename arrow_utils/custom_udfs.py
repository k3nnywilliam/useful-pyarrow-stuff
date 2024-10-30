'''
You can define custom UDFs with pyarrow’s compute module for operations that are not natively supported. 
This is a powerful feature for complex data processing, especially when paired with vectorized operations.
'''

import pyarrow.compute as pc
import pyarrow as pa

# Define a custom UDF to square numbers
def square_udf(array):
    return pc.call_function("multiply", [array, array])

# Apply UDF on an Arrow array
array = pa.array([1, 2, 3, 4, 5])
squared_array = square_udf(array)
print(squared_array)
