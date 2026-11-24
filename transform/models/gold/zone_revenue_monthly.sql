{{ config(properties=iceberg_properties(['pickup_month'])) }}

select
    t.pickup_month,
    t.pu_location_id as location_id,
    z.borough,
    z.zone_name,
    count(*) as trips,
    sum(t.total_amount) as revenue,
    round(avg(t.trip_miles), 2) as avg_trip_miles
from {{ ref('trips') }} as t
left join {{ ref('taxi_zones') }} as z
    on t.pu_location_id = z.location_id
group by t.pickup_month, t.pu_location_id, z.borough, z.zone_name
