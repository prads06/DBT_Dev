import snowflake.snowpark.functions as F 
import pandas as pd
import holidays
def avgsale(x,y):
    return y/x


def is_holiday(date_col):
    french_holiday = holidays.France()
    is_holiday = (date_col in french_holiday)
    return is_holiday


def model(dbt,session):
    dbt.config(materialized= 'table', schema= 'reportings', packages= 'holidays')
    dim_customer_df = dbt.ref('customer_dim')
    fct_order_df = dbt.ref('fact_order')

    customer_order_df = (
                            fct_order_df
                            .group_by('customerid')
                            .agg 
                            (
                            F.min(F.col('orderdate')).alias('first_orderdate'),
                            F.max(F.col('orderdate')).alias('recent_orderdate'),
                            F.count(F.col('orderid')).alias('total_order'),
                            F.sum(F.col('linesaleamount')).alias('total_sales'),
                            F.avg(F.col('margin')).alias('avg_margin')
                            ) 
                        )           

    final_df =  (
                    dim_customer_df
                    .join(customer_order_df,customer_order_df.customerid == dim_customer_df.customerid, 'left')
                    .select
                      (
                        dim_customer_df.companyname.alias('companyname'),
                        dim_customer_df.contactname.alias('contactname'),      
                        customer_order_df.first_orderdate.alias('first_orderdate'),
                        customer_order_df.recent_orderdate.alias('recent_orderdate'),
                        customer_order_df.total_order.alias('total_order'),
                        customer_order_df.total_sales.alias('total_sales'),
                        customer_order_df.avg_margin.alias('avg_margin')
                        )
                )
    final_df = final_df.withColumn('avg_salevalue', avgsale(final_df['total_order'], final_df['total_sales']))
    final_df = final_df.filter(F.col('first_orderdate').isNotNull())

    final_df = final_df.to_pandas()

    final_df["IS_FIRST_ORDERDATE_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)

    return final_df
  
