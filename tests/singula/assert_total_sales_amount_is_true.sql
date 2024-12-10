select
orderid,
sum(linesaleamount) as sales
from
{{ref("fact_order")}}
group by 1
having sales< 0
