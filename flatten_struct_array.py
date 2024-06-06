from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize Spark session
spark = SparkSession.builder.appName("FlattenNestedDF").getOrCreate()

# Define schema with multiple nested levels
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True),
    StructField("contacts", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("detail", StringType(), True)
    ])), True),
    StructField("job", StructType([
        StructField("title", StringType(), True),
        StructField("department", StructType([
            StructField("name", StringType(), True),
            StructField("location", StringType(), True)
        ]), True)
    ]), True)
])

# Create sample data
data = [
    ("John Doe", 30, ("San Francisco", "CA", "94107"), 
     [("email", "john.doe@example.com"), ("phone", "123-456-7890")], 
     ("Software Engineer", ("Engineering", "Building 1"))),
    ("Jane Smith", 25, ("New York", "NY", "10001"), 
     [("email", "jane.smith@example.com")], 
     ("Data Scientist", ("Data", "Building 2")))
]

# Create DataFrame
nested_df = spark.createDataFrame(data, schema)

# Show the DataFrame
nested_df.show(truncate=False)
nested_df.printSchema()

from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame
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
                fields += flatten(dtype, new_name)
            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                fields.append((f"{new_name}", col(f"{prefix}.{name}" if prefix else name)))
                fields.append((f"{new_name}_exploded", explode(col(f"{prefix}.{name}" if prefix else name))))
            else:
                fields.append((new_name, col(f"{prefix}.{name}" if prefix else name)))
        return fields

    flattened_fields = flatten(df.schema)

    select_expr = [field.alias(alias) for alias, field in flattened_fields]
    flat_df = df.select(*select_expr)

    for alias, field in flattened_fields:
        if "_exploded" in alias:
            exploded_alias = alias.replace("_exploded", "")
            nested_cols = flat_df.schema[alias].dataType.elementType
            nested_select = [col(f"{alias}.{nested_col.name}").alias(f"{exploded_alias}{separator}{nested_col.name}")
                             for nested_col in nested_cols.fields]
            flat_df = flat_df.select("*", *nested_select).drop(alias)

    return flat_df

# Example usage:
flattened_df = flatten_df(nested_df, include_parents=True, separator='_')
flattened_df.show(truncate=False)
flattened_df.printSchema()
