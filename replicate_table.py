import subprocess

def replicate_hive_table_beeline(
    beeline_url,
    source_db,
    source_table,
    target_db,
    target_table,
    log_file="beeline_output.log",
):
    """
    Creates a new Hive table (using Beeline) that replicates the structure and data of an existing source table.

    Args:
        beeline_url: Beeline connection URL (e.g., "jdbc:hive2://localhost:10000").
        source_db: Name of the source database.
        source_table: Name of the source table.
        target_db: Name of the target database.
        target_table: Name of the new table to create.
        log_file: Path to the log file (default: "beeline_output.log").
    """

    create_table_query = f"CREATE TABLE {target_db}.{target_table} LIKE {source_db}.{source_table};"
    insert_data_query = f"INSERT OVERWRITE TABLE {target_db}.{target_table} SELECT * FROM {source_db}.{source_table};"

    beeline_command = f"""
    !connect {beeline_url}
    {create_table_query}
    {insert_data_query}
    """

    with open(log_file, "a") as f:
        process = subprocess.Popen(
            ["beeline", "-f", "-"], stdin=subprocess.PIPE, stdout=f, stderr=subprocess.STDOUT, text=True
        )
        process.communicate(input=beeline_command)

        if process.returncode == 0:
            print("Table replication successful.")
        else:
            print(f"Error replicating table. Check {log_file} for details.")
          
