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

{#


{{config (
    materialized='incremental'
    unique_key = 'page_view_id'
) }}

{% if is_incremental() %}
where collector_tstamp >= (select dateadd('day', -3, max(max_collector_tstamp)) from {{ this }})
{% endif %}

--this = this model

--- is_incremental does
-- 1.does this model already existes as an object in the database?
-- 2.is that databse object a table?
-- 3. is this mdoel configured woth materialized  = 'incremental'
-- 4 was the --full-refresh flag passe don this dbt run
-- yes yes yes no == incremental run


#}

{# --- stg-page_views
{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id'
) }}
with events as (
  --  select * from {{ source('snowplow', 'events') }}
   -- {% if is_incremental() %}
    where collector_tstamp >= (select max(max_collector_tstamp) from {{ this }})
    {% endif %}
),
page_views as (
    select * from events
    where event = 'page_view'
),
aggregated_page_events as (
    select
        page_view_id,
        count(*) * 10 as approx_time_on_page,
        min(derived_tstamp) as page_view_start,
        max(collector_tstamp) as max_collector_tstamp
    from events
    group by 1
),
joined as (
    select
        *
    from page_views
    left join aggregated_page_events using (page_view_id)
)
select * from joined
#}