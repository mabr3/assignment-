
import logging

def get_create_statement(table):
    """ Genereates a create table statement from a dictionary
    Args:
        table (dict): _description_

    Returns:
        str : The string with the create statement
    """

    columns = table['columns']
    table_format = table['format']
    partitions = table['partition']
    
    create_table_statement = '('
    # Columns
    for column in columns:
        name = column['name']
        data_type = column['type']
        create_table_statement += f"\n{name} {data_type},"
    create_table_statement = create_table_statement.rstrip(',') + ')'
    
    # Format
    create_table_statement += f"\n USING {table_format}"
    
    # Partitions
    if partitions:
        create_table_statement += "\nPARTITIONED BY ("
        for partition in partitions:
            create_table_statement += f"{partition}, "
        create_table_statement = create_table_statement.rstrip(', ') + ")"

    return create_table_statement


def check_tables(spark, tables_list, first=False):
    """Checks if the tables and schemas exist. If not, creates them and asserts if they were correctly created
    Args:
        spark (sparkContext): sparkContext instance being used
        tables_list (list): list of tables read from the yaml files
        first (bool): Flag to signal initial runs
    """
    for table in tables_list:
        schema_name = table['table']['schema']
        table_name = table['table']['name']

        if first:
            spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")


        # Create Schema if it doesn't exist and ensure it worked
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        assert any([i.namespace == schema_name for i in spark.sql("SHOW DATABASES;").collect()])
        
        create_statement = get_create_statement(table['table'])
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} {create_statement}")
        assert any([i.namespace == schema_name for i in spark.sql("SHOW DATABASES;").collect()])

        
