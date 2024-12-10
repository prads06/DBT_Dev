{{config(materialized= 'table', schema= 'transformings')}}


select

GET(XMLGET (supplierinfo, 'SupplierID'), '$') as supplier_id,
GET(XMLGET (supplierinfo, 'CompanyName'), '$')::varchar as CompanyName,
GET(XMLGET (supplierinfo, 'ContactName'), '$')::varchar as ContactName,
GET(XMLGET (supplierinfo, 'Address'), '$')::varchar as Address,
GET(XMLGET (supplierinfo, 'City'), '$')::varchar as City,
GET(XMLGET (supplierinfo, 'Country'), '$')::varchar as Country,
GET(XMLGET (supplierinfo, 'PostalCode'), '$')::varchar as PostalCode,
GET(XMLGET (supplierinfo, 'Phone'), '$')::varchar as Phone,
GET(XMLGET (supplierinfo, 'Fax'), '$')::varchar as Fax,
from

{{ref('staging_supplier')}}