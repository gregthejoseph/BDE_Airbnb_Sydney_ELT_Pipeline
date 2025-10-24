{{
    config(
        unique_key='lga_code_2016',
        alias='bronze_lga_g02'
    )
}}

select *
from {{ source('raw', 'lga_g02_raw') }}