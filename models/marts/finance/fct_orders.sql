{{ config(materialized='table') }}

with payments as (
    select
        order_id,
        amount
    from {{ ref('stg_stripe__payments') }}
),
orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
)
select
    orders.order_id,
    orders.customer_id,
    coalesce(payments.amount, 0) as amount
from orders
left join payments using (order_id)
