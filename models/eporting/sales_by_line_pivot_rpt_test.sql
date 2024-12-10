{{ config(materialized= 'view', schema= 'reportings')}}

{% set lineno = [1,2,3] %}

select
orderid,
{% for lnos in lineno %}
sum(case when lineno = {{lnos}} then linesaleamount end) as lineno{{lnos}}_sales,
{% endfor %}

sum(linesaleamount) as total_sales
from {{ ref('fact_order') }}
group by 1


