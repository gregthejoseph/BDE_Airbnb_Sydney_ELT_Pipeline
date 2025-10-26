{{ config(
    materialized='table',
    schema='gold',
    alias='dim_listing'
) }}

select
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_listing') }}
where listing_id is not null
  and dbt_valid_to is null  -- get only the latest active listings
