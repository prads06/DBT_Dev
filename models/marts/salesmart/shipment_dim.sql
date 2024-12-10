{{ config(materialized= 'view', schema= 'salesmart')}}


select * from
{{ref('trf_shipment')}}
