{{ config(
    materialized='table',
    alias='dim_listing'
) }}

select
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_listing') }}
where listing_id is not null
