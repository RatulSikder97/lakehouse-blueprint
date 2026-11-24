{#- Column contract shared by silver.trips and silver.rejected_trips. -#}
{% macro trip_columns() -%}
    {{ return([
        "trip_id", "source_month", "vendor_id", "pickup_at", "dropoff_at", "pickup_date",
        "pickup_month", "pickup_hour", "trip_seconds", "passenger_count", "trip_miles",
        "ratecode_id", "is_store_and_forward", "pu_location_id", "do_location_id",
        "payment_type", "payment_method", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee",
        "cbd_congestion_fee", "total_amount", "ingested_at"
    ]) }}
{%- endmacro %}
