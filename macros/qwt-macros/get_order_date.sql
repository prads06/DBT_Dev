{% macro get_min_orderdate() %}
 
{% set get_minorderdate_query %}
 
select
min(orderdate)
from {{ ref('fact_order') }}
 
{% endset %}
 
{% set results = run_query(get_minorderdate_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{ return(results_list) }}

{% endmacro %}