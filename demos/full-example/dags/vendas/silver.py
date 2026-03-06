from vendas.transformations.vendas import (
    get_jdbc_url_and_properties,
    run_silver_vendas_transformation,
)


def silver_transformation_data_vendas():
    jdbc_url, db_properties = get_jdbc_url_and_properties()
    run_silver_vendas_transformation(jdbc_url, db_properties)
