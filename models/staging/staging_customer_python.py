def model(dbt, session):
    staging_customer_df = dbt.source("qwt_raw", "customer")
    return staging_customer_df

