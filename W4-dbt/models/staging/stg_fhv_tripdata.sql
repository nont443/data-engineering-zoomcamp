with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select 
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        sr_flag as store_and_fwd_flag,
        Affiliated_base_number
    from source
    where dispatching_base_num IS NOT NULL
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
