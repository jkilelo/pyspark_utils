from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, lit
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
    def flatten(schema, prefix=None):
        fields = []
        for field in schema.fields:
            name = field.name
            dtype = field.dataType
            new_name = f"{prefix}{separator}{name}" if prefix else name

            if isinstance(dtype, StructType):
                fields += flatten(dtype, prefix=new_name)
            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                fields.append(col(f"{prefix}.{name}" if prefix else name).alias(new_name))
                fields.append(explode(col(f"{prefix}.{name}" if prefix else name)).alias(f"{new_name}_exploded"))
            else:
                fields.append(col(f"{prefix}.{name}" if prefix else name).alias(new_name))
        return fields

    def flatten_df_recursively(df, prefix=None):
        schema = df.schema
        columns = flatten(schema, prefix)
        df = df.select(*columns)
        
        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            if isinstance(dtype, StructType):
                df = df.withColumn(name, col(name))
                df = flatten_df_recursively(df, prefix=name)
            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                df = df.withColumn(name, explode(col(name)))
                df = flatten_df_recursively(df, prefix=name)
                
        return df

    return flatten_df_recursively(df)

# Example usage:
# flattened_df = flatten_df(nested_df, include_parents=True, separator='_')
