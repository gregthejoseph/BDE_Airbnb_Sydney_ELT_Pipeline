{{ config(
    materialized='table',
    alias='dim_listing'
) }}


select distinct
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30
    
from {{ ref('snap_listing') }}
where listing_id is not null
