import subprocess
import re
from pyspark.sql import SparkSession

def is_table_partitioned(beeline_url, db_name, table_name):
    """
    Checks if a Hive table is partitioned.

    Args:
        beeline_url: Beeline connection URL (e.g., "jdbc:hive2://localhost:10000").
        db_name: The name of the database containing the table.
        table_name: The name of the table to check.

    Returns:
        True if the table is partitioned, False otherwise.
    """

    command = ["beeline", "-u", beeline_url, "-e", f"SHOW CREATE TABLE {db_name}.{table_name};"]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = result.stdout
        
        # Use regular expression to search for PARTITIONED BY clause
        if re.search("PARTITIONED BY", output, re.IGNORECASE):
            return True
        else:
            return False
    
    except subprocess.CalledProcessError as e:
        print(f"Error executing Beeline: {e}")
        return None




def is_table_partitioned_sql(db_name, table_name):
    """
    Checks if a Hive table is partitioned using a direct SQL query.

    Args:
        db_name: The name of the database containing the table.
        table_name: The name of the table to check.

    Returns:
        True if the table is partitioned, False otherwise.
    """
    spark = SparkSession.builder.getOrCreate()

    # Execute a SQL query to check for partition information
    query = f"SHOW PARTITIONS {db_name}.{table_name};"
    result = spark.sql(query)

    # If there are any rows in the result, the table is partitioned
    return result.count() > 0 

