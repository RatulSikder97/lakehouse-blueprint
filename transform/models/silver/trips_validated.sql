{{ config(materialized='ephemeral') }}

with source as (
    select * from {{ source('bronze', 'yellow_trips') }}
),

typed as (
    select
        trip_id,
        source_month,
        vendor_id,
        pickup_at,
        dropoff_at,
        cast(pickup_at as date) as pickup_date,
        cast(date_trunc('month', pickup_at) as date) as pickup_month,
        extract(hour from pickup_at) as pickup_hour,
        {{ dbt.datediff('pickup_at', 'dropoff_at', 'second') }} as trip_seconds,
        passenger_count,
        trip_distance as trip_miles,
        ratecode_id,
        coalesce(store_and_fwd_flag = 'Y', false) as is_store_and_forward,
        pu_location_id,
        do_location_id,
        payment_type,
        case payment_type
            when 0 then 'flex_fare'
            when 1 then 'credit_card'
            when 2 then 'cash'
            when 3 then 'no_charge'
            when 4 then 'dispute'
            when 6 then 'voided'
            else 'unknown'
        end as payment_method,
        cast(fare_amount as decimal(12, 2)) as fare_amount,
        cast(extra as decimal(12, 2)) as extra,
        cast(mta_tax as decimal(12, 2)) as mta_tax,
        cast(tip_amount as decimal(12, 2)) as tip_amount,
        cast(tolls_amount as decimal(12, 2)) as tolls_amount,
        cast(improvement_surcharge as decimal(12, 2)) as improvement_surcharge,
        cast(coalesce(congestion_surcharge, 0) as decimal(12, 2)) as congestion_surcharge,
        cast(coalesce(airport_fee, 0) as decimal(12, 2)) as airport_fee,
        cast(coalesce(cbd_congestion_fee, 0) as decimal(12, 2)) as cbd_congestion_fee,
        cast(total_amount as decimal(12, 2)) as total_amount,
        _ingested_at as ingested_at
    from source
)

select
    *,
    case
        when pickup_month != cast({{ dbt.concat(["source_month", "'-01'"]) }} as date)
            then 'pickup_outside_source_month'
        when dropoff_at <= pickup_at then 'non_positive_duration'
        when trip_seconds > 6 * 3600 then 'duration_over_6h'
        when trip_miles <= 0 or trip_miles > 200 then 'implausible_distance'
        when fare_amount < 0 or total_amount < 0 then 'negative_amount'
        when
            pu_location_id not between 1 and 265
            or do_location_id not between 1 and 265
            then 'unknown_location'
    end as rejection_reason
from typed
