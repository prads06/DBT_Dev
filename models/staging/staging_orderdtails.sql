{{config (materialized= 'table', unique_key= ['orderid','lineno'],  schema=env_var('DBT_STAGESCHEMA', 'STAGING_DEV'))}}


select od.*,o.orderdate from 
{{source ('qwt_raw', 'order_dtails')}} as od
inner join
{{source ('qwt_raw', 'raw_order')}} as o
on od.orderid=o.orderid

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where orderdate > (select max(orderdate) from {{ this }})

{% endif %}