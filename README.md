# NYC Service Requests

A data engineering project for ingesting, processing, and analyzing NYC 311 service requests data.

## Project Overview

This project creates an end-to-end data pipeline that:
1. Extracts service request data from the NYC Open Data API
2. Loads the raw data into a PostgreSQL database
3. Transforms the data using dbt
4. Creates gold-level analytics tables for insights and metrics

## System Architecture

![System Architecture](docs/images/system_architecture.png)

## Components

### Data Source
- NYC Open Data API: https://data.cityofnewyork.us/resource/erm2-nwe9.json
- Provides 311 service request data across NYC

### Data Infrastructure
- **PostgreSQL**: Storage for raw and transformed data
- **Docker Compose**: Local deployment of PostgreSQL instance
- **Environment Variables**: Credentials stored in `.env` file

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.x
- dbt

### Setup
1. Clone the repository
```bash
git clone https://github.com/pdrals16/nyc-service-requests.git
cd nyc-service-requests
```

2. Set up environment variables
```bash
cd .infra
cp .env.example .env
# Edit .env with your preferred credentials
```

2. 1. Set up dbt profiles.yml

3. Start PostgreSQL using Docker Compose
```bash
make up
```

# DBT Models Explained

## Overview

This project uses dbt (data build tool) to transform NYC service request data through a medallion architecture. The data flows through three layers:

1. **Bronze**: Raw data exactly as received from the source
2. **Silver**: Cleaned, deduplicated, and standardized data
3. **Gold**: Aggregated analytics-ready data

## Model Details

### Bronze Layer

**Table: bronze_service_requests**

This table serves as the landing zone for raw data from the NYC Open Data API. It contains all original columns with minimal transformation. The schema is defined in `create_table_bronze_service_requests.sql`.

Key characteristics:
- Preserves original data structure
- Supports JSON parsing for nested fields (like location data)
- All incoming fields are captured and typed appropriately
- No business logic applied at this stage

### Silver Layer

**Model: silver_service_requests**

This model represents the first transformation layer where we apply data cleaning techniques and standardize naming conventions.

Key transformations:
- Column names standardized with prefixes:
  - `id_` for identifiers
  - `dt_` for dates
  - `nm_` for names/descriptive fields
  - `cd_` for codes
  - `vl_` for numerical values
- Deduplication using row_number() window function on unique_key
- Date filtering based on the reference date
- Null handling (e.g., 'N/A' values converted to NULL)
- Timestamp tracking with process date

Implementation details:
- Materialized as an incremental model
- Uses merge strategy for efficient updates
- Unique key constraints on id_service_request
- Configured to process one day's worth of data per run

### Gold Layer

**Model: gold_service_requests**

This model serves as the analytics layer, providing aggregated metrics for NYC service requests. It's designed for business intelligence and reporting use cases.

Aggregation dimensions:
- Date reference (dt_reference)
- Agency (nm_agency)
- Complaint type (nm_complaint_type)
- City (nm_city)
- Borough (cd_borough)
- Channel type (nm_open_data_channel_type)
- Status (nm_status)

Key metrics:
- Average resolution time in hours (vl_hours_difference_create_close_mean)
  - Calculated as the time difference between creation and closing dates
- Service request count (qt_service_requests)
  - Distinct count of service request IDs

Implementation details:
- Materialized as an incremental model
- Unique key on dt_reference for incremental processing
- Parameterized with reference_date variable
- Aggregates data for a specific date
- Designed for daily batch processing