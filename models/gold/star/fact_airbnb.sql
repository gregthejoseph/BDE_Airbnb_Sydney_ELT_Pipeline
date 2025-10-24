{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'fact_airbnb'
) }}


with src as (

    select
        listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value
    from {{ ref('stg_airbnb') }}
    where listing_id is not null

),

fact as (

    select
        -- Foreign keys / identifiers
        listing_id,
        host_id,

        -- Time context
        scraped_date,

        -- Base numeric metrics
        price::numeric                                   as price,
        number_of_reviews::int                           as number_of_reviews,
        review_scores_rating::numeric                    as review_scores_rating,
        review_scores_accuracy::numeric                  as review_scores_accuracy,
        review_scores_cleanliness::numeric               as review_scores_cleanliness,
        review_scores_checkin::numeric                   as review_scores_checkin,
        review_scores_communication::numeric             as review_scores_communication,
        review_scores_value::numeric                     as review_scores_value,
        availability_30::int                             as availability_30,

        -- Derived metrics
        case 
            when has_availability = true and coalesce(availability_30, 0) > 0 
                then 1 
            else 0 
        end as is_active,

        greatest(0, 30 - coalesce(availability_30, 0)) as est_stays,

        (coalesce(price::numeric, 0) * 
         greatest(0, 30 - coalesce(availability_30, 0)))::numeric as est_revenue

    from src
)

select * from fact
