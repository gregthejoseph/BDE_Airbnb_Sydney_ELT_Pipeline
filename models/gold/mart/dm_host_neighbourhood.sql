{{ config(
    materialized='view',
    schema='gold',
    alias='dm_host_neighbourhood'
) }}

with base as (
    select
        f.listing_id,
        f.host_id,
        f.scraped_date,
        date_trunc('month', f.scraped_date)::date as month_year,
        l.has_availability,
        l.availability_30,
        f.price,
        -- deriving the LGA name (for simplicity just aliasing host_neighbourhood)
        h.host_neighbourhood as host_neighbourhood_lga
    from {{ ref('fact_airbnb') }} f
    left join {{ ref('dim_listing') }} l using (listing_id)
    left join {{ ref('dim_host') }} h using (host_id)
),

agg as (
    select
        host_neighbourhood_lga,
        month_year,

        -- number of distinct hosts
        count(distinct host_id) as num_distinct_hosts,

        -- average estimated revenue per active listing
        avg(
            case when has_availability = true
                 then (greatest(0, 30 - availability_30) * price)::numeric end
        ) as avg_est_revenue_per_active_listing,

        -- estimated revenue per host (distinct)
        (sum(
            case when has_availability = true
                 then (greatest(0, 30 - availability_30) * price)::numeric end
        ) / nullif(count(distinct host_id), 0)) as est_revenue_per_host

    from base
    group by host_neighbourhood_lga, month_year
)

select *
from agg
order by host_neighbourhood_lga, month_year
