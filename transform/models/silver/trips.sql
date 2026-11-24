{{ config(properties=iceberg_properties(['pickup_month'])) }}

-- Cleaned, typed trips that passed every validation rule.
select
    {{ trip_columns() | join(",\n    ") }}
from {{ ref('trips_validated') }}
where rejection_reason is null
