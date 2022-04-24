{#
--to macro
select * from {{ source('snowplow', 'events') }}

{% if target.name == 'dev'%}
where collector_tstamp >= dateadd('day', -3, current_timestamp)
{% endif %}

--the macro

{% macro limit_data_in_dev() %}
{% if target.name == 'dev'%}
where collector_tstamp >= dateadd('day', -3, current_timestamp)
{% endif %}
{% endmacro %}

-- call the macro

{{ limit_data_in_dev() }}


----- check snowflake info

{% set database=target.database %}
{% set schema=target.schema %}

select 
    table_type,
    table_schema,
    table_name,
    last_altered
from {{ database }}.INFORMATION_SCHEMA.TABLES
where table_schema = upper('{{ schema }}')
order by last_altered desc

#}