{{ config(properties=iceberg_properties()) }}

-- Quarantine: rows that failed validation, with the first rule they broke.
select
    {{ trip_columns() | join(",\n    ") }},
    rejection_reason
from {{ ref('trips_validated') }}
where rejection_reason is not null
