
{{
    config(
        materialized="table",
        sql_header= "alter session set timezone = 'America/Chicago';",
        schema= env_var('DBT_STAGESCHEMA','STAGING_DEV')
    )
}}
select *
from {{ source("qwt_raw", "customer") }}
