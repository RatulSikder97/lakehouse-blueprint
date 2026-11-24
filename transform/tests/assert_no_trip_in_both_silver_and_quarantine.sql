-- Every bronze row lands in exactly one of silver.trips / silver.rejected_trips.
select t.trip_id
from {{ ref('trips') }} as t
inner join {{ ref('rejected_trips') }} as r on t.trip_id = r.trip_id
