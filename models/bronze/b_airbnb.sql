{{
    config(
        unique_key='listing_id',
        alias='bronze_airbnb'
    )
}}

select *
from {{ source('raw', 'airbnb_raw') }}