{{ config(
    materialized = 'table',
    alias = 'dim_lga_g02'
) }}


select distinct *
from {{ ref('stg_lga_g02') }}
where lga_code_2016 is not null
