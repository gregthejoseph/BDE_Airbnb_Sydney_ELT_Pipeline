{% snapshot snap_lga_code %}
{{
    config(
        target_schema='snapshots',
        unique_key='lga_code',
        strategy='timestamp',
        updated_at='fake_updated_at'
    )
}}

select
    lga_code,
    lga_name,
    current_timestamp as fake_updated_at
from {{ ref('stg_lga_code') }}
where lga_code is not null

{% endsnapshot %}
