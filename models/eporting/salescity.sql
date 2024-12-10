{{config(materialized= 'view', schema= 'reportings')}}


select c.customerid, c.contactname, c.city, sum(o.linesaleamount) as sales, sum(o.quantity) as quantity
, avg(o.margin) as margin
from
{{ref("customer_dim")}} as c inner join {{ref('fact_order')}} as o
on o.customerid=c.customerid
where c.city= {{var('vcity', "'London'")}}
group by c.customerid, c.contactname, c.city 