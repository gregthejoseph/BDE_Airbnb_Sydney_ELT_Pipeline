{% snapshot snap_lga_g02 %}
{{
    config(
        target_schema='snapshots',
        unique_key='lga_code_2016',
        strategy='timestamp',
        updated_at='fake_updated_at'
    )
}}

select
    lga_code_2016,
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size,
    current_timestamp as fake_updated_at
from {{ ref('stg_lga_g02') }}
where lga_code_2016 is not null

{% endsnapshot %}
