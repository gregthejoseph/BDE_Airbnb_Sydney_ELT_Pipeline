{{
    config(
        materialized='incremental',
        unique_key='listing_id',
        incremental_strategy='delete+insert',
        alias='silver_airbnb'
    )
}}

with source as (
    select * from {{ source('raw', 'airbnb_raw') }}
),

cleaned as (
    select
        listing_id,
        scrape_id,
        case
            when scraped_date::text ~ '^\d{4}-\d{2}-\d{2}$' then to_date(scraped_date::text, 'YYYY-MM-DD')
            when scraped_date::text ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_date(scraped_date::text, 'DD/MM/YYYY')
            else null
        end as scraped_date,
        host_id,
        trim(host_name) as host_name,
        case
            when host_since ~ '^\d{4}-\d{2}-\d{2}$' then to_date(host_since, 'YYYY-MM-DD')
            when host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_date(host_since, 'DD/MM/YYYY')
            else null
        end as host_since,
        case 
            when lower(host_is_superhost) in ('t','true','yes','y') then true
            when lower(host_is_superhost) in ('f','false','no','n') then false
            else null
        end as host_is_superhost,
        trim(host_neighbourhood) as host_neighbourhood,
        trim(listing_neighbourhood) as listing_neighbourhood,
        trim(property_type) as property_type,
        trim(room_type) as room_type,
        accommodates,
        cast(nullif(regexp_replace(price::text, '[^0-9\.]', '', 'g'), '') as numeric) as price,
        case 
            when lower(has_availability) in ('t','true','yes','y') then true
            when lower(has_availability) in ('f','false','no','n') then false
            else null
        end as has_availability,
        availability_30,
        number_of_reviews,
        coalesce(nullif(review_scores_rating, 'NaN')::numeric, 0) as review_scores_rating,
        coalesce(nullif(review_scores_accuracy, 'NaN')::numeric, 0) as review_scores_accuracy,
        coalesce(nullif(review_scores_cleanliness, 'NaN')::numeric, 0) as review_scores_cleanliness,
        coalesce(nullif(review_scores_checkin, 'NaN')::numeric, 0) as review_scores_checkin,
        coalesce(nullif(review_scores_communication, 'NaN')::numeric, 0) as review_scores_communication,
        coalesce(nullif(review_scores_value, 'NaN')::numeric, 0) as review_scores_value,
        current_timestamp as record_loaded_at,
        coalesce(
            to_timestamp(scraped_date::text, 'YYYY-MM-DD'),
            to_timestamp(scraped_date::text, 'DD/MM/YYYY'),
            current_timestamp - interval '1 day'
        ) as updated_at
    from source
)

{% if is_incremental() %}

, max_existing as (
    select max(updated_at) as max_updated_at
    from {{ this }}
)

select c.*
from cleaned c
left join max_existing m on true
where c.updated_at > coalesce(m.max_updated_at, '1900-01-01')

{% else %}

select * from cleaned

{% endif %}
