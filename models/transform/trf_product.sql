{{config(materialized= 'table', schema = 'transformings')}}


select 
p.productid,
p.productname,
c.categoryname,
s.CompanyName,
s.ContactName,
s.City,
s.Country,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
iff (p.unitsonorder > p.unitsinstock, 'yes', 'no') as productstaus
from
{{ref('staging_products')}} as p inner join {{ref('trf_supplier')}} as s 
on s.supplier_id= p.supplierid
inner join {{ref('lookup_categories')}} as c 
on p.categoryid= c.categoryid