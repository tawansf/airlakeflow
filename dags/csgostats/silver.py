from csgostats.transformations.csgostats import (
    get_jdbc_url_and_properties,
    run_silver_csgostats_transformation,
)


def silver_transformation_data_csgostats():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_csgostats_transformation(jdbc_url, db_properties)
