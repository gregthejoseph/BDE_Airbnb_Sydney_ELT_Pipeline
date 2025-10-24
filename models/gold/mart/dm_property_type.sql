{{ config(
    materialized='view',
    schema='gold',
    alias='dm_property_type'
) }}

with base as (
    select
        f.listing_id,
        f.host_id,
        f.scraped_date,
        date_trunc('month', f.scraped_date)::date as month_year,
        f.price,
        l.has_availability,
        l.availability_30,
        f.review_scores_rating,
        l.property_type,
        l.room_type,
        l.accommodates,
        h.host_is_superhost
    from {{ ref('fact_airbnb') }} f
    left join {{ ref('dim_listing') }} l using (listing_id)
    left join {{ ref('dim_host') }} h using (host_id)
),

agg as (
    select
        property_type,
        room_type,
        accommodates,
        month_year,

        -- total and active listings
        count(distinct listing_id) as total_listings,
        count(distinct case when has_availability = true then listing_id end) as active_listings,

        -- Active listing rate (%)
        (count(distinct case when has_availability = true then listing_id end)::numeric /
         nullif(count(distinct listing_id), 0)) * 100 as active_listing_rate,

        -- Price metrics for active listings
        min(case when has_availability = true then price end) as min_price_active,
        max(case when has_availability = true then price end) as max_price_active,
        percentile_cont(0.5) within group (order by price)
            filter (where has_availability = true) as median_price_active,
        avg(case when has_availability = true then price end) as avg_price_active,

        -- Distinct hosts & superhost rate (%)
        count(distinct host_id) as no_of_distinct_hosts,
        count(distinct case when host_is_superhost = 't' then host_id end) as superhosts,
        (count(distinct case when host_is_superhost = 't' then host_id end)::numeric /
         nullif(count(distinct host_id), 0)) * 100 as superhost_rate,

        -- Ratings for active listings
        avg(case when has_availability = true then review_scores_rating end) as avg_review_score_active,

        -- Total stays (for active listings)
        sum(case when has_availability = true then greatest(0, 30 - availability_30) end) as total_stays,

        -- Average estimated revenue per active listing
        avg(
            case when has_availability = true
                 then (greatest(0, 30 - availability_30) * price)::numeric end
        ) as avg_est_revenue_active

    from base
    group by property_type, room_type, accommodates, month_year
),

with_pct as (
    select
        a.*,

        -- % change for active listings month-to-month
        ((a.active_listings - lag(a.active_listings)
            over (partition by property_type, room_type, accommodates order by month_year))
         / nullif(lag(a.active_listings)
            over (partition by property_type, room_type, accommodates order by month_year), 0)
        ) * 100 as pct_change_active_listings,

        -- % change for inactive listings month-to-month
        (((a.total_listings - a.active_listings)
            - lag(a.total_listings - a.active_listings)
            over (partition by property_type, room_type, accommodates order by month_year))
         / nullif(lag(a.total_listings - a.active_listings)
            over (partition by property_type, room_type, accommodates order by month_year), 0)
        ) * 100 as pct_change_inactive_listings

    from agg a
)

select
    property_type,
    room_type,
    accommodates,
    month_year,
    active_listing_rate,
    min_price_active,
    max_price_active,
    median_price_active,
    avg_price_active,
    no_of_distinct_hosts,
    superhost_rate,
    avg_review_score_active,
    pct_change_active_listings,
    pct_change_inactive_listings,
    total_stays,
    avg_est_revenue_active
from with_pct
order by property_type, room_type, accommodates, month_year
