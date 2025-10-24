{{
    config(
        unique_key='lga_code',
        alias='bronze_lga_code'
    )
}}

select *
from {{ source('raw', 'lga_code_raw') }}