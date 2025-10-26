{{ config(
    materialized = 'view',
    schema       = 'gold',
    alias        = 'dm_host_neighbourhood'
) }}

with base as (
    select
        f.listing_id,
        f.host_id,
        f.scraped_date,
        date_trunc('month', f.scraped_date)::date as month_year,
        f.price,
        f.availability_30,
        f.is_active,
        f.est_revenue,
        h.host_neighbourhood,
        h.host_is_superhost
    from {{ ref('fact_airbnb') }} f
    left join {{ ref('dim_host') }} h
      on  f.host_id = h.host_id
      and f.scraped_date >= h.dbt_valid_from
      and (f.scraped_date < h.dbt_valid_to or h.dbt_valid_to is null)
),

-- 🗺️ map host suburb → LGA
host_lga as (
    select
        b.*,
        ls.lga_name as host_neighbourhood_lga
    from base b
    left join {{ ref('dim_lga_suburb') }} ls
      on upper(trim(b.host_neighbourhood)) = upper(trim(ls.suburb_name))
),

agg as (
    select
        host_neighbourhood_lga,
        month_year,
        count(distinct host_id)                                         as num_distinct_hosts,

        -- 💵 average estimated revenue per active listing
        avg(case when is_active = 1 then est_revenue end)               as avg_est_revenue_per_active_listing,

        -- 💵 estimated revenue per host
        (sum(case when is_active = 1 then est_revenue end)::numeric
         / nullif(count(distinct host_id),0))                           as est_revenue_per_host
    from host_lga
    group by host_neighbourhood_lga, month_year
)

select *
from agg
order by host_neighbourhood_lga, month_year
