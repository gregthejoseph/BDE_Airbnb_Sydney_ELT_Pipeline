{{ config(
    materialized = 'view',
    schema       = 'gold',
    alias        = 'dm_listing_neighbourhood'
) }}

with base as (
    select
        f.listing_id,
        f.host_id,
        f.scraped_date,
        date_trunc('month', f.scraped_date)::date as month_year,
        f.price,
        f.availability_30,
        f.review_scores_rating,
        f.is_active,
        f.est_revenue,

        l.listing_neighbourhood,
        h.host_is_superhost
    from {{ ref('fact_airbnb') }} f
    left join {{ ref('dim_listing') }} l
      on  f.listing_id = l.listing_id
      and f.scraped_date >= l.dbt_valid_from
      and (f.scraped_date < l.dbt_valid_to or l.dbt_valid_to is null)
    left join {{ ref('dim_host') }} h
      on  f.host_id = h.host_id
      and f.scraped_date >= h.dbt_valid_from
      and (f.scraped_date < h.dbt_valid_to or h.dbt_valid_to is null)
),

agg as (
    select
        listing_neighbourhood,
        month_year,
        count(distinct listing_id)                                         as total_listings,
        count(distinct case when is_active = 1 then listing_id end)        as active_listings,
        (count(distinct case when is_active = 1 then listing_id end)::numeric /
         nullif(count(distinct listing_id), 0)) * 100                      as active_listing_rate,

        min(case when is_active = 1 then price end)                        as min_price_active,
        max(case when is_active = 1 then price end)                        as max_price_active,
        percentile_cont(0.5) within group (order by price)
            filter (where is_active = 1)                                   as median_price_active,
        avg(case when is_active = 1 then price end)                        as avg_price_active,

        count(distinct host_id)                                            as total_hosts,
        count(distinct case when host_is_superhost = true then host_id end) as superhosts,
        (count(distinct case when host_is_superhost = true then host_id end)::numeric /
         nullif(count(distinct host_id), 0)) * 100                         as superhost_rate,

        avg(case when is_active = 1 then review_scores_rating end)         as avg_review_score_active,
        sum(case when is_active = 1 then greatest(0, 30 - availability_30) end) as total_stays,
        avg(case when is_active = 1 then est_revenue end)                  as avg_est_revenue_active
    from base
    group by listing_neighbourhood, month_year
),

with_pct as (
    select
        a.*,
        ((a.active_listings - lag(a.active_listings)
           over (partition by listing_neighbourhood order by month_year))
         / nullif(lag(a.active_listings)
           over (partition by listing_neighbourhood order by month_year), 0)::numeric) * 100
           as pct_change_active_listings,
        (((a.total_listings - a.active_listings)
           - lag(a.total_listings - a.active_listings)
           over (partition by listing_neighbourhood order by month_year))
         / nullif(lag(a.total_listings - a.active_listings)
           over (partition by listing_neighbourhood order by month_year), 0)::numeric) * 100
           as pct_change_inactive_listings
    from agg a
)

select
    listing_neighbourhood,
    month_year,
    active_listing_rate,
    min_price_active,
    max_price_active,
    median_price_active,
    avg_price_active,
    total_hosts,
    superhost_rate,
    avg_review_score_active,
    pct_change_active_listings,
    pct_change_inactive_listings,
    total_stays,
    avg_est_revenue_active
from with_pct
order by listing_neighbourhood, month_year
