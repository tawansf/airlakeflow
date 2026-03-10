from my_snapshot.transformations.my_snapshot import (
    get_jdbc_url_and_properties,
    run_silver_my_snapshot_transformation,
)


def silver_transformation_data_my_snapshot():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_my_snapshot_transformation(jdbc_url, db_properties)
