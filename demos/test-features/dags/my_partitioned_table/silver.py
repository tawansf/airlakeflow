from my_partitioned_table.transformations.my_partitioned_table import (
    get_jdbc_url_and_properties,
    run_silver_my_partitioned_table_transformation,
)


def silver_transformation_data_my_partitioned_table():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_my_partitioned_table_transformation(jdbc_url, db_properties)
