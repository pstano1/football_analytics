# Football Analytics Data Platform

> **Note**: This project was developed as part of a bachelor's thesis on modern data warehouse architectures. Full thesis available [here](#) (link to be added).

> **Note**: The predefined ETL pipelines use preprocessed data (from a project implemented by a fellow student) rather than retrieving it directly from [https://footystats.org/api/](https://footystats.org/api/).

A comprehensive football analytics data platform combining modern data engineering tools to provide insights into match performance, player statistics, and team standings across 50+ leagues.

## Project Overview

This platform:
- Ingests football data from multiple sources
- Transforms raw data into analytical models
- Provides interactive dashboards and data discovery
- Supports both OLTP (PostgreSQL) and OLAP (ClickHouse) workloads

### Key Features

- **Multi-layered Architecture**: Core normalized layer + dimensional data marts
- **Dual Database Strategy**: PostgreSQL for integrity, ClickHouse for analytics
- **Automated Pipelines**: Dagster-orchestrated ETL with dbt transformations
- **Data Discovery**: Amundsen integration for metadata management
- **Interactive Analytics**: Power BI dashboards with real-time insights

### Architecture

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Dagster | Pipeline scheduling and monitoring |
| **Transformation** | dbt | SQL-based data modeling |
| **OLTP Database** | PostgreSQL 13 | Normalized data storage |
| **OLAP Database** | ClickHouse | High-performance analytics |
| **Data Catalog** | Amundsen + Neo4j | Metadata management |
| **Search** | Elasticsearch | Metadata indexing |
| **Visualization** | Power BI | Interactive dashboards |
| **Deployment** | Docker Compose | Containerized environment |

## Data Model

### Core Schema (PostgreSQL)
Normalized 3NF design with:
- **Teams & Players**: Player profiles, team rosters, statistics
- **Competitions**: Leagues, seasons, team participation
- **Matches**: Detailed match statistics (goals, xG, possession, cards)
- **Infrastructure**: Stadiums, referees, associations

### Data Marts (PostgreSQL)
Dimensional star schemas for:
- **Match Performance**: Team-level match analytics
- **Player Performance**: Individual player statistics
- **Team Standings**: League tables and rankings

### OLAP Cubes (ClickHouse)
Denormalized cubes for:
- **Team Standings Cube**: Real-time standings with match-level detail
- Pre-aggregated metrics for fast querying

## Quick Start

### Prerequisites

- Docker Desktop (v20.10+)
- Docker Compose (v1.29+)
- 8GB+ RAM
- 20GB free disk space

### Installation

1. **Clone the repository**
```bash
git clone 
cd 
```

2. **Configure environment**
```bash
cp .env.example .env
```

3. **Start services**
```bash
docker-compose up -d --build
```

4. **Verify installation**
```bash
docker-compose ps
```

### Access Services

| Service | URL | Description |
|---------|-----|-------------|
| Dagster UI | [http://localhost:3000](http://localhost:3000) | Pipeline orchestration |
| Amundsen | [http://localhost:5003](http://localhost:5003) | Data catalog |

## Documentation

To run a browser based documentation please use the following command and navigate to [http://127.0.0.1:8000/](http://127.0.0.1:8000/).

```bash
LANG=en envsubst < mkdocs.yml > mkdocs.build.yml && mkdocs serve -f mkdocs.build.yml
```

## License

Project is licensed under MIT License.

---

> **Note** This is a thesis project and is not actively maintained.


