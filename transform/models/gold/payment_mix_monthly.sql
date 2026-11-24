{{ config(properties=iceberg_properties()) }}

with monthly as (
    select
        pickup_month,
        payment_method,
        count(*) as trips,
        sum(total_amount) as revenue
    from {{ ref('trips') }}
    group by pickup_month, payment_method
)

select
    pickup_month,
    payment_method,
    trips,
    revenue,
    round(100.0 * trips / sum(trips) over (partition by pickup_month), 2) as share_of_trips_pct
from monthly
