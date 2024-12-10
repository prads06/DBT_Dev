{{config (materialized= 'table', schema='transformings')}}


select 
c.customerid,
c.companyname,
c.contactname,
c.city,
c.country,
l.divisionname,
c.address,
c.fax,
c.phone,
c.postalcode,
IFF (c.stateprovince= '', 'NA', c.stateprovince) as state
from 
{{ref('staging_customer')}} as c
inner join
{{ref ('lookup_division')}} as l
on c.divisionid=l.divisionid

