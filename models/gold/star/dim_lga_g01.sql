{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'dim_lga_g01'
) }}

select
    lga_code_2016,
    tot_p_m,
    tot_p_f,
    tot_p_p,
    age_0_4_yr_p,
    age_5_14_yr_p,
    age_65_74_yr_p,
    age_85ov_p,
    indigenous_p_tot_p,
    birthplace_australia_p,
    birthplace_elsewhere_p,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_lga_g01') }}
where dbt_valid_to is null
