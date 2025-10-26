{{ config(
    materialized='table',
    schema='gold',
    alias='dim_host'
) }}

select
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_host') }}
where host_id is not null
  and dbt_valid_to is null
