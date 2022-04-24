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
#}