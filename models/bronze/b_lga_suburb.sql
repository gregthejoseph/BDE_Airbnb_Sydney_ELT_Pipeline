{{
    config(
        unique_key='suburb_name',
        alias='bronze_lga_suburb'
    )
}}

select *
from {{ source('raw', 'lga_suburb_raw') }}