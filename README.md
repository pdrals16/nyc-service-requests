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

3. Start PostgreSQL using Docker Compose
```bash
make up
```
