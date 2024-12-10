import snowflake.snowpark.functions as f 


def model (dbt,session):
    dbt.config(materialized='incremental', unique_key= 'orderid')
    df = dbt.source('qwt_raw', 'raw_order')

    if dbt.is_incremental:
        max_orderdate = f"select max(orderdate) from {dbt.this}"
        df = df.filter (df.orderdate > session.sql(max_orderdate).collect()[0][0])

    return df