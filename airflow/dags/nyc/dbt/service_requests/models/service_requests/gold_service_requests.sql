{{
  config(
    materialized='incremental',
    unique_key='dt_reference',
    incremental_strategy='merge'
  )
}}
{% set ref_date = var('reference_date', 'YYYY-MM-DD') %}

select
    date(dt_created) dt_reference,
    nm_agency,
    nm_complaint_type,
    nm_city,
    cd_borough,
    nm_open_data_channel_type,
    nm_status,
    avg(
        EXTRACT(
            EPOCH
            FROM
                (dt_closed - dt_created)
        ) / 3600
    ) AS vl_hours_difference_create_close_mean,
    count(distinct id_service_request) qt_service_requests
from
    {{ ref('silver_service_requests') }}
where
    date(dt_created) = '{{ ref_date }}'
group by
    dt_reference,
    nm_agency,
    nm_complaint_type,
    nm_city,
    cd_borough,
    nm_open_data_channel_type,
    nm_status