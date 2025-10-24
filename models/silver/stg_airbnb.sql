{{
    config(
        unique_key='listing_id',
        alias='silver_airbnb'
    )
}}

-- ============================================================
-- SILVER LAYER: Airbnb Listings (Cleaned & Typed)
-- ============================================================
-- Purpose:
--   This model transforms the raw Airbnb data from the Bronze layer
--   into a clean, standardized, and analysis-ready Silver table.
--   - Converts textual booleans ("t"/"f") into true/false
--   - Handles multiple date formats safely
--   - Fills missing review score fields with 0
-- ============================================================

with source as (

    -- Read from Bronze layer
    select * from {{ source('raw', 'airbnb_raw') }}

),

renamed as (

    select
        -- 🧩 Identifiers
        listing_id,
        scrape_id,

        -- 🕓 Scrape Date (handles both possible formats)
        case
            when scraped_date::text ~ '^\d{4}-\d{2}-\d{2}$' then to_date(scraped_date::text, 'YYYY-MM-DD')
            when scraped_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_date(scraped_date::text, 'DD/MM/YYYY')
            else null
        end as scraped_date,

        -- 👤 Host Information
        host_id,
        trim(host_name) as host_name,

        case
            when host_since ~ '^\d{4}-\d{2}-\d{2}$' then to_date(host_since, 'YYYY-MM-DD')
            when host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_date(host_since, 'DD/MM/YYYY')
            else null
        end as host_since,

        -- ✅ Keep same name but cast to boolean
        case 
            when lower(host_is_superhost) in ('t','true','yes','y') then true
            when lower(host_is_superhost) in ('f','false','no','n') then false
            else null
        end as host_is_superhost,

        trim(host_neighbourhood) as host_neighbourhood,

        -- 🏠 Listing Details
        trim(listing_neighbourhood) as listing_neighbourhood,
        trim(property_type) as property_type,
        trim(room_type) as room_type,
        accommodates,

        -- 💰 Pricing (remove $ or commas safely)
        cast(
            nullif(regexp_replace(price::text, '[^0-9\.]', '', 'g'), '') as numeric
        ) as price,

        -- ✅ Keep same name but cast to boolean
        case 
            when lower(has_availability) in ('t','true','yes','y') then true
            when lower(has_availability) in ('f','false','no','n') then false
            else null
        end as has_availability,
        availability_30,

        -- ⭐ Reviews (fill NaN/null with 0)
        number_of_reviews,
        coalesce(review_scores_rating, 0)           as review_scores_rating,
        coalesce(review_scores_accuracy, 0)         as review_scores_accuracy,
        coalesce(review_scores_cleanliness, 0)      as review_scores_cleanliness,
        coalesce(review_scores_checkin, 0)          as review_scores_checkin,
        coalesce(review_scores_communication, 0)    as review_scores_communication,
        coalesce(review_scores_value, 0)            as review_scores_value,

        -- 🕒 Metadata
        current_timestamp as record_loaded_at

    from source
)

select * from renamed
