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
    stack = [(df, None)]
    flattened_cols = []

    while stack:
        current_df, prefix = stack.pop()
        for field in current_df.schema.fields:
            name = field.name
            dtype = field.dataType
            full_name = f"{prefix}{separator}{name}" if prefix else name

            if isinstance(dtype, StructType):
                nested_cols = [col(f"{name}.{nested_field.name}") for nested_field in dtype.fields]
                for nested_col in nested_cols:
                    nested_name = nested_col._jc.toString().split('`')[1]
                    stack.append((current_df.select(col("*"), nested_col.alias(f"{full_name}{separator}{nested_name}")), full_name))
            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                array_col_name = f"{prefix}.{name}" if prefix else name
                exploded_df = current_df.withColumn(array_col_name, explode(col(array_col_name)))
                stack.append((exploded_df, full_name))
            else:
                flattened_cols.append(col(f"{prefix}.{name}" if prefix else name).alias(full_name))

    return df.select(flattened_cols)

# Example usage:
flattened_df = flatten_df(nested_df, include_parents=True, separator='_')
flattened_df.show(truncate=False)
flattened_df.printSchema()
