{{ config(
    materialized='table',
    schema='gold',
    alias='dim_lga_code'
) }}

select
    lga_code,
    lga_name,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_lga_code') }}
where dbt_valid_to is null  -- keep only current active LGAs
