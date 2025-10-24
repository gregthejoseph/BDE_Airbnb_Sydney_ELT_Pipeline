{{ 
    config(
        alias='silver_lga_code',
        unique_key='lga_code'
    ) 
}}

-- ============================================================
-- SILVER LAYER: LGA CODE TRANSFORMATION
-- ============================================================

with source as (

    -- Read data from the Bronze layer
    select * from {{ source('raw', 'lga_code_raw') }}

),

cleaned as (

    select
        -- 🧩 Primary Identifier
        cast(lga_code as integer) as lga_code,

        -- 🏙️ LGA Name (remove extra spaces or casing inconsistencies)
        trim(upper(lga_name)) as lga_name

    from source
    where lga_code is not null
)

select * from cleaned
