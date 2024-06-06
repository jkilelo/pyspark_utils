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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, array
from pyspark.sql.types import StructType, ArrayType

def flatten_dataframe(df: DataFrame, include_parents: bool = True, separator: str = "_") -> DataFrame:
    """
    Flattens nested structs and arrays within a PySpark DataFrame.

    Args:
        df: The input DataFrame to flatten.
        include_parents: If True, include parent names in the flattened column names.
        separator: The character used to separate parent and child names.

    Returns:
        The flattened DataFrame.
    """
    # Initialize a stack to track columns to process
    stack = [(df, [])]

    while stack:
        df, parents = stack.pop()

        for field in df.schema.fields:
            field_name = field.name
            field_type = field.dataType

            # Base Case 1: if the field is a primitive data type, then do nothing.
            if not isinstance(field_type, (StructType, ArrayType)):
                continue

            # Base Case 2: if the field is an array but its element type is primitive, then explode it directly.
            elif isinstance(field_type, ArrayType) and not isinstance(field_type.elementType, (StructType, ArrayType)):
                df = df.withColumn(field_name, explode_outer(col(field_name)))

            # Recursive Case 1: if the field is an array of structs, we first explode it.
            elif isinstance(field_type, ArrayType):
                # Explode the field as it is an array and continue further.
                df = df.withColumn(field_name, explode_outer(col(field_name)))

            # Recursive Case 2: if the field is a struct, we push it into stack along with its parent.
            else:
                # Push the struct field to the stack along with its parents for later processing
                stack.append((df.select(field_name + ".*"), parents + [field_name]))

        # If include_parents flag is true then we flatten the dataframe by joining parent and children names.
        if include_parents:
            flattened_cols = [
                col(".".join(parents + [c.name])).alias(separator.join(parents + [c.name]))
                for c in df.schema.fields
            ]
        else:
            # Otherwise, just use the children names
            flattened_cols = [col(c.name) for c in df.schema.fields]

        # Select flattened columns from current dataframe
        df = df.select(*flattened_cols)

    return df
