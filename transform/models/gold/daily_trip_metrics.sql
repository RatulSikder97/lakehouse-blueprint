{{ config(properties=iceberg_properties()) }}

select
    pickup_date,
    count(*) as trips,
    sum(total_amount) as revenue,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_miles), 2) as avg_trip_miles,
    round(avg(trip_seconds) / 60.0, 1) as avg_trip_minutes,
    round(
        100.0 * sum(case when payment_method = 'credit_card' then tip_amount else 0 end)
        / nullif(sum(case when payment_method = 'credit_card' then fare_amount else 0 end), 0),
        2
    ) as credit_card_tip_pct,
    sum(case when airport_fee > 0 then 1 else 0 end) as airport_pickups
from {{ ref('trips') }}
group by pickup_date
