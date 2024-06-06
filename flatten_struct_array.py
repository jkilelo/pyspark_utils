from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

def flatten_df(df: DataFrame, include_parents: bool = True, separator: str = '_') -> DataFrame:
    """
    Flattens a nested DataFrame, including any nested structs or arrays.
    
    Args:
        df (DataFrame): Input DataFrame to flatten.
        include_parents (bool): Whether to include parent names in the flattened keys.
        separator (str): Custom key name separator to avoid key collision.
        
    Returns:
        DataFrame: Flattened DataFrame.
    """
    # Stack to keep track of DataFrame and prefix
    stack = [(df, None)]
    flattened_cols = []
    
    while stack:
        current_df, prefix = stack.pop()
        
        for field in current_df.schema.fields:
            name = field.name
            dtype = field.dataType
            full_name = f"{prefix}{separator}{name}" if prefix else name

            if isinstance(dtype, StructType):
                stack.append((current_df.select(col(f"{prefix}.{name}.*" if prefix else f"{name}.*")), full_name))
            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                array_col_name = f"{prefix}.{name}" if prefix else name
                exploded_df = current_df.withColumn(array_col_name, explode(col(array_col_name)))
                stack.append((exploded_df, full_name))
            else:
                flattened_cols.append(col(f"{prefix}.{name}" if prefix else name).alias(full_name))
    
    return df.select(flattened_cols)

# Example usage:
# flattened_df = flatten_df(nested_df, include_parents=True, separator='_')
