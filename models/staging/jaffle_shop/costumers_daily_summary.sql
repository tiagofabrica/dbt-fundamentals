select 
   
    {{ dbt_utils.surrogate_key(['customer_id', 'order_date']) }} as id,
    customer_id as cust_id,
    order_date as order_date,
    count(*) as max_amount

from {{ ref('stg_orders') }}
group by 1,2,3