{{ config(
    materialized = 'table',
    alias = 'dim_lga_g01'
) }}


select distinct *
from {{ ref('stg_lga_g01') }}
where lga_code_2016 is not null
