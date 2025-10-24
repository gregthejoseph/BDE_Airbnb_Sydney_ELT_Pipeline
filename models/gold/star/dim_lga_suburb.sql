{{ config(
    materialized='table',
    alias='dim_lga_suburb'
) }}

select distinct
  suburb_name,
  lga_name
from {{ ref('stg_lga_suburb') }}
where suburb_name is not null
