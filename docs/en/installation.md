# Installation Guide

This guide will walk you through setting up the Football Analytics Data Warehouse on your local 
machine.

## Prerequisites

Before installing, ensure you have:

- Docker Desktop (v20.10+)
- Docker Compose (v1.29+)
- At least 8GB of available RAM
- 20GB of free disk space

## Installation Steps

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Configure Environment Variables

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your preferred values:

```bash
# Project Configuration
PROJECT_NAME=minerva

# Amundsen Ports
AMUNDSEN_FRONTEND_PORT=5000
AMUNDSEN_METADATA_PORT=5002
AMUNDSEN_SEARCH_PORT=5001
AMUNDSEN_NEO4J_HTTP_PORT=7474
AMUNDSEN_NEO4J_BOLT_PORT=7687

# PostgreSQL Configuration
POSTGRES_HOST=minerva_postgres
POSTGRES_PORT=5432
POSTGRES_DB=minerva
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_secure_password

# Neo4j Configuration
NEO4J_USER=neo4j
NEO4J_PASSWORD=test
```

### 3. Start Services

Launch all services using Docker Compose:

```bash
docker-compose up -d
```

### 4. Verify Installation

Check that all containers are running:

```bash
docker-compose ps
```

You should see all services in "Up" state.

### 5. Initialize Database

The database schema will be automatically created from the migration files in `migrations/`. Wait 
a few moments for PostgreSQL to initialize.

Verify the database is ready:

```bash
docker-compose exec minerva_postgres psql -U <your_username> -d minerva -c "SELECT schema_name 
FROM information_schema.schemata;"
```

You should see schemas: `core`, `metadata`, `mart_match_performance`, `mart_player_performance`, 
`mart_team_standings`.


## Access the Services

Once installation is complete, access the following services:

| Service | URL | Description |
|---------|-----|-------------|
| Dagster UI | http://localhost:3000 | Pipeline orchestration and monitoring |
| Amundsen | http://localhost:5000 | Data discovery and catalog |

