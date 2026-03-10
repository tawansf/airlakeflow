from my_kafka.transformations.my_kafka import (
    get_jdbc_url_and_properties,
    run_silver_my_kafka_transformation,
)


def silver_transformation_data_my_kafka():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_my_kafka_transformation(jdbc_url, db_properties)
