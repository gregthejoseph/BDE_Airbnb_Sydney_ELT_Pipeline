{{ config(
    materialized='table',
    schema='gold',
    alias='dim_lga_suburb'
) }}

select
    suburb_name,
    lga_name,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_lga_suburb') }}
where suburb_name is not null
  and dbt_valid_to is null
