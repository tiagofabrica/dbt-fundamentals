{% snapshot snap_products %}
    {{
        config(
            target_database = 'analytics',
            target_schema = 'dbt_tfabrica',
            unique_key='CUSTOMER_ID',

            strategy='timestamp',
            updated_at='ORDER_DATE',
            
            
        )
    }}

    select * from {{ ref('stg_orders') }}
         --  from {{ source('jaffle_shop', 'orders') }}
    
 {% endsnapshot %}

 {#
 strategy='check',
 check_cols= ['status'],
 --select order_id, status from {{ source('jaffle_shop', 'orders') }}
 #}