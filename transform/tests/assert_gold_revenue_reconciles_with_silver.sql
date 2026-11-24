-- Gold daily revenue must reconcile with silver trips to the cent.
with silver as (
    select round(sum(total_amount), 2) as revenue from {{ ref('trips') }}
),

gold as (
    select round(sum(revenue), 2) as revenue from {{ ref('daily_trip_metrics') }}
)

select
    silver.revenue as silver_revenue,
    gold.revenue as gold_revenue
from silver cross join gold
where abs(silver.revenue - gold.revenue) > 0.01
