{{
    config(
        alias='silver_lga_g02',
        unique_key='lga_code_2016'
    )
}}

-- ============================================================
-- SILVER LAYER: LGA G02 CENSUS TRANSFORMATION
-- ============================================================
-- Purpose:
--   - Standardize and clean the Census LGA G02 dataset
--   - Convert text-based LGA code to integer
--   - Maintain naming consistency and add metadata
-- ============================================================

with source as (

    -- Read data from Bronze layer
    select * from {{ source('raw', 'lga_g02_raw') }}

),

cleaned as (

    select
        -- 🧩 Identifier (remove 'LGA' prefix and cast to integer)
        cast(regexp_replace(lga_code_2016, '[^0-9]', '', 'g') as integer) as lga_code_2016,

        -- 💵 Economic and Housing Statistics
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size,

        -- 🕒 Metadata
        current_timestamp as record_loaded_at

    from source
    where lga_code_2016 is not null
)

select * from cleaned
