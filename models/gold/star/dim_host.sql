{{ config(
    materialized='table',
    alias='dim_host'
) }}

select distinct
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood
from {{ ref('snap_host') }}
where host_id is not null
