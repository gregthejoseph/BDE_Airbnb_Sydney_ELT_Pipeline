{% snapshot snap_host %}
{{
    config(
        target_schema='snapshots',
        unique_key='host_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    host_id,
    host_name,
    scraped_date,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    record_loaded_at,
    
    updated_at::timestamp as updated_at

from {{ ref('stg_airbnb') }}
where host_id is not null

{% endsnapshot %}
