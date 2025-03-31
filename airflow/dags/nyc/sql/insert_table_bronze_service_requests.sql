INSERT INTO bronze_service_requests (
    unique_key, created_date, closed_date, agency, agency_name, 
    complaint_type, descriptor, location_type, incident_zip, 
    incident_address, street_name, cross_street_1, cross_street_2, 
    intersection_street_1, intersection_street_2, address_type, city, 
    landmark, status, resolution_description, resolution_action_updated_date, 
    community_board, bbl, borough, x_coordinate_state_plane, 
    y_coordinate_state_plane, open_data_channel_type, park_facility_name, 
    park_borough, latitude, longitude, address, addr_city, addr_state, 
    addr_zip, bridge_highway_name, bridge_highway_direction, 
    bridge_highway_segment, facility_type, vehicle_type, due_date, 
    taxi_pick_up_location, road_ramp, taxi_company_borough
)
VALUES %s;