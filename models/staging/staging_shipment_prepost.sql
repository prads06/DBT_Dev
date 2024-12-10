{{
    config(
        materialized='table',transient= false,
        pre_hook= "select current_date;",
        post_hook = "create or replace database QWT_ANALYTICS_DEV_clone clone QWT_ANALYTICS_DEV;" )
}}
 
select
ORDERID,
LINENO,
SHIPPERID,
CUSTOMERID,
PRODUCTID,
EMPLOYEEID,
split_part(SHIPMENTDATE,' ',0)::date as SHIPMENTDATE,
STATUS,
from
{{ source('qwt_raw', 'shipment') }}