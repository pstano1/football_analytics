# Football Analytics Data Warehouse

Football Analytics Data Warehouse documentation. This is a comprehensive football analytics data 
warehouse that combines modern data engineering tools to provide insights into match performance, 
player statistics, and team standings.

## Key Features

- **Core Data Layer**: Comprehensive football data including players, teams, matches, leagues and 
  associations
- **Data Marts**: Pre-built analytical models for match performance, player performance and team 
  standings
- **Automated Pipelines**: ETL jobs for data ingestion and transformation
- **Metadata Management**: Amundsen integration for data discovery and lineage
- **Containerized**: Full Docker Compose setup for easy deployment

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | PostgreSQL 13 | Data storage |
| Transformation | dbt | SQL transformations |
| Orchestration | Dagster | Pipeline management |
| Metadata | Amundsen | Data discovery |
| Container | Docker Compose | Deployment |

## Project Structure

```
project/
├── dagster/             # Orchestration code
│   ├── dags/            # Pipeline definitions
│   └── jobs/            # ETL jobs
├── dbt/                 # Transformation models
|   |__ clickhouse/      # dbt clickhouse models
│   └── postgres/        # dbt postgres models
├── migrations/          # Database schemas
├── docker-compose.yml   # Service definitions
└── .env.example         # Configuration template
```

## Getting Started

1. Clone the repository
2. Copy `.env.example` to `.env` and configure
3. Run `docker-compose up -d`

