{{
  config(
    materialized='incremental',
    unique_key='id_service_request',
    incremental_strategy='merge'
  )
}}
{% set ref_date = var('reference_date', 'YYYY-MM-DD') %}

WITH query_row_number AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY unique_key
            ORDER BY
                created_date DESC
        ) row_number
    FROM
        {{ source('service_requests', 'service_requests') }}
    WHERE
        created_date >= '{{ ref_date }} 00:00:00'
        AND created_date <= '{{ ref_date }} 23:59:59'
)
SELECT
    unique_key AS id_service_request,
    created_date AS dt_created,
    closed_date AS dt_closed,
    agency AS cd_agency,
    agency_name AS nm_agency,
    complaint_type AS nm_complaint_type,
    descriptor AS nm_descriptor,
    location_type AS nm_location_type,
    incident_zip AS cd_incident_zip,
    incident_address AS nm_incident_address,
    street_name AS nm_street,
    cross_street_1 AS nm_cross_street_1,
    cross_street_2 AS nm_cross_street_2,
    intersection_street_1 AS nm_intersection_street_1,
    intersection_street_2 AS nm_intersection_street_2,
    address_type AS nm_address_type,
    city AS nm_city,
    landmark AS nm_landmark,
    status AS nm_status,
    resolution_description AS nm_resolution_description,
    resolution_action_updated_date AS dt_resolution_action_updated,
    community_board AS cd_community_board,
    bbl AS cd_bbl,
    borough AS cd_borough,
    x_coordinate_state_plane AS vl_x_coordinate_state_plane,
    y_coordinate_state_plane AS vl_y_coordinate_state_plane,
    open_data_channel_type AS nm_open_data_channel_type,
    park_facility_name AS nm_park_facility,
    park_borough AS nm_park_borough,
    latitude AS vl_latitude,
    longitude AS vl_longitude,
    address AS nm_address,
    addr_city AS nm_addr_city,
    addr_state AS nm_addr_state,
    addr_zip AS cd_addr_zip,
    bridge_highway_name AS nm_bridge_highway,
    bridge_highway_direction AS nm_bridge_highway_direction,
    bridge_highway_segment AS nm_bridge_highway_segment,
    CASE
        WHEN facility_type = 'N/A' THEN NULL
        ELSE facility_type
    END AS nm_facility_type,
    vehicle_type AS nm_vehicle_type,
    due_date AS dt_due,
    taxi_pick_up_location AS nm_taxi_pick_up_location,
    road_ramp AS nm_road_ramp,
    taxi_company_borough AS nm_taxi_company_borough,
    NOW() AS dt_process
FROM
    query_row_number
WHERE
    row_number = 1