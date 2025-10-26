{% snapshot snap_listing %}
{{
    config(
        target_schema='snapshots',
        unique_key='listing_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    listing_id,
    host_id,
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
    review_scores_value,
    scraped_date,

    updated_at::timestamp as updated_at

from {{ ref('stg_airbnb') }}
where listing_id is not null

{% endsnapshot %}
