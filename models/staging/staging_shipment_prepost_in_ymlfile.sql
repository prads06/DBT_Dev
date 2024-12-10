{{
    config(
        materialized='table')
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