{{ config(alias='dim_lga_code') }}

select
  lga_code,
  lga_name
from {{ ref('stg_lga_code') }}
