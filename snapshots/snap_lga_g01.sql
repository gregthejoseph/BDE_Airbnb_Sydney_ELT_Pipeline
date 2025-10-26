{% snapshot snap_lga_g01 %}
{{
    config(
        target_schema='snapshots',
        unique_key='lga_code_2016',
        strategy='timestamp',
        updated_at='fake_updated_at'
    )
}}

select
    *,
    current_timestamp as fake_updated_at
from {{ ref('stg_lga_g01') }}
where lga_code_2016 is not null

{% endsnapshot %}
