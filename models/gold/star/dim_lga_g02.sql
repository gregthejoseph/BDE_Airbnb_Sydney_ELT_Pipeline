{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'dim_lga_g02'
) }}

select
    lga_code_2016,
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('snap_lga_g02') }}
where dbt_valid_to is null
