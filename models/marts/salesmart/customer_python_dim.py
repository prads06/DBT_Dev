def model(dbt, session):
    dbt.config(materialized= 'table', schema= 'salesmart')
    dim_customer_df = dbt.ref("trf_customer")
    return dim_customer_df