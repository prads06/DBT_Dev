{{config(materialized= 'table', schema= 'transformings')}}


select s.orderid, sh.companyname, s.shipmentdate,s.status, s.dbt_valid_from as valid_from, s.dbt_valid_to as valid_to from
{{ref('shipments_snapshot')}} as s inner join {{ref('lookup_s')}} as sh
on sh.shipperid= s.shipperid