import subprocess

def transfer_and_rename_table(
    beeline_url,
    source_db,
    source_table,
    target_db,
    target_table,
    column_mapping,
):
    """
    Transfers data from a source Hive table to a target table, renaming columns as specified.

    Args:
        beeline_url: Beeline connection URL (e.g., "jdbc:hive2://localhost:10000").
        source_db: Source database name.
        source_table: Source table name.
        target_db: Target database name.
        target_table: Target table name.
        column_mapping: A dictionary mapping old column names to new column names.
    """

    select_clause = ", ".join(
        f"{old_col} AS {new_col}" if new_col else old_col
        for old_col, new_col in column_mapping.items()
    )
    query = f"""
    CREATE TABLE {target_db}.{target_table} AS
    SELECT {select_clause} 
    FROM {source_db}.{source_table};
    """

    command = ["beeline", "-u", beeline_url, "-e", query]
    try:
        subprocess.run(command, check=True)
        print("Table transfer and renaming successful.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing Beeline: {e}")
