{{
    config(
        unique_key='lga_code_2016',
        alias='bronze_lga_g01'
    )
}}

select *
from {{ source('raw', 'lga_g01_raw') }}