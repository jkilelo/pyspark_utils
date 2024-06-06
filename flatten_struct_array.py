from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, explode_outer
from pyspark.sql.types import ArrayType

class NestedDF:
    """A class for flattening nested dataframes in PySpark."""

    def __init__(self, nested_df: DataFrame, separator: str = '_', include_parents: bool = True):
        """
        Initialize the NestedDF class.

        Args:
            nested_df (pyspark.sql.dataframe.DataFrame): Nested dataframe.
            separator (str): Separator to use in flattened column names.
            include_parents (bool): Whether to include parent column names in the flattened column names.
        """
        self.nested_df = nested_df
        self.separator = separator
        self.include_parents = include_parents
        self.flattened_struct_df = self.flatten_struct_df()
        self.flattened_df = self.flatten_array_df()

    def flatten_array_df(self) -> DataFrame:
        """
        Flatten a nested array dataframe into a single level dataframe.

        Returns:
            pyspark.sql.dataframe.DataFrame: Flattened dataframe.
        """
        # Get columns of the DataFrame
        cols = self.flattened_struct_df.columns
        for col_name in cols:
            # Check if the column is an array
            if isinstance(self.flattened_struct_df.schema[col_name].dataType, ArrayType):
                # Get the length of the array
                array_len = self.flattened_struct_df.select(size(col(col_name)).alias("array_len")).collect()[0]["array_len"]
                for i in range(array_len):
                    # Flatten the array elements into separate columns
                    self.flattened_struct_df = self.flattened_struct_df.withColumn(f"{col_name}{self.separator}{i}", self.flattened_struct_df[col_name].getItem(i))
                # Drop the original array column
                self.flattened_struct_df = self.flattened_struct_df.drop(col_name)
        return self.flattened_struct_df

    def flatten_struct_df(self) -> DataFrame:
        """
        Flatten a nested dataframe into a single level dataframe.

        Returns:
            pyspark.sql.dataframe.DataFrame: Flattened dataframe.
        """
        stack = [((), self.nested_df)]  # Stack to keep track of the current level of nesting
        columns = []  # List to store column expressions
        while stack:
            parents, df = stack.pop()
            for col_name, col_type in df.dtypes:
                if col_type.startswith('struct'):
                    # If the column is a struct, add it to the stack for further flattening
                    new_parents = parents + (col_name,)
                    stack.append((new_parents, df.select(f"{col_name}.*")))
                else:
                    # Generate the new column name based on the separator and include_parents flag
                    new_col_name = self.separator.join(parents + (col_name,)) if self.include_parents else col_name
                    # Add the column expression to the list
                    columns.append(col(".".join(parents + (col_name,))).alias(new_col_name))
        # Select the flattened columns
        return self.nested_df.select(columns)

# Example usage
df_to_flatten = NestedDF(nested_df, separator='_', include_parents=True)
flattened_df = df_to_flatten.flattened_df
flattened_df.show(truncate=False)
flattened_df.printSchema()
