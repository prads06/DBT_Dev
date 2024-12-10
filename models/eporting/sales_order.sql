{{ config(materialized="view", schema="reportings") }}

select
    c.companyname,
    c.contactname,
    count(f.orderid) as total_order,
    max(f.orderdate) as recent_order_date,
    sum(f.linesaleamount) as total_sales,
    avg(f.margin) as avg_margin
from {{ ref("fact_order") }} f
inner join {{ ref("customer_dim") }} c on f.customerid = c.customerid
group by c.companyname, c.contactname
order by total_sales desc
