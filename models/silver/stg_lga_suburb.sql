{{
    config(
        alias='silver_lga_suburb',
        unique_key='suburb_name'
    )
}}

-- ============================================================
-- SILVER LAYER: LGA SUBURB TRANSFORMATION
-- ============================================================
-- Purpose:
--   - Clean and standardize suburb and LGA mapping information
--   - Ensure consistent data types, casing, and naming conventions
--   - Prepare for joins with Census and Airbnb data in the Gold layer
-- ============================================================

with source as (

    -- Read from the Bronze layer
    select * from {{ source('raw', 'lga_suburb_raw') }}

),

cleaned as (

    select
        -- 📍 Suburb Name (Title case for readability)
        trim(initcap(suburb_name)) as suburb_name,

        -- 🏛️ LGA Name (uppercase for consistent joins)
        trim(upper(lga_name)) as lga_name,

        -- 🕒 Metadata
        current_timestamp as record_loaded_at

    from source
)

select * from cleaned
