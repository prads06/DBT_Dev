{{config(materialized= 'table', schema= 'salesmart')}}

select * from
{{(ref('trf_customer'))}}