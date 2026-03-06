from crypto.transformations.crypto import (
    get_jdbc_url_and_properties,
    run_silver_crypto_transformation,
)


def silver_transformation_data_crypto():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_crypto_transformation(jdbc_url, db_properties)
