{% snapshot snap_lga_suburb %}
{{
    config(
        target_schema='snapshots',
        unique_key='suburb_name',
        strategy='timestamp',
        updated_at='fake_updated_at'
    )
}}

select
    lga_name,
    suburb_name,
    current_timestamp as fake_updated_at
from {{ ref('stg_lga_suburb') }}
where suburb_name is not null

{% endsnapshot %}
