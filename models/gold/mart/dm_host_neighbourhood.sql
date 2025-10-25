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
        h.host_neighbourhood as host_neighbourhood_lga
    from {{ ref('fact_airbnb') }} f
    left join {{ ref('dim_listing') }} l
      on f.listing_id = l.listing_id
      and f.scraped_date >= l.dbt_valid_from
      and (f.scraped_date < l.dbt_valid_to or l.dbt_valid_to is null)
    left join {{ ref('dim_host') }} h
      on f.host_id = h.host_id
      and f.scraped_date >= h.dbt_valid_from
      and (f.scraped_date < h.dbt_valid_to or h.dbt_valid_to is null)
),
agg as (
    select
        host_neighbourhood_lga,
        month_year,
        count(distinct host_id) as num_distinct_hosts,
        avg(
            case 
                when has_availability = true
                then (greatest(0, 30 - availability_30) * price)::numeric
            end
        ) as avg_est_revenue_per_active_listing,
        (
            sum(
                case 
                    when has_availability = true
                    then (greatest(0, 30 - availability_30) * price)::numeric
                end
            ) / nullif(count(distinct host_id), 0)
        ) as est_revenue_per_host
    from base
    group by host_neighbourhood_lga, month_year
)
select *
from agg
order by host_neighbourhood_lga, month_year
