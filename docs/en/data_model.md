# Data Model

This document describes the data modeling approach and design patterns used in the application.

For detailed table and column information, use Amundsen. This documentation focuses on design 
patterns, modeling decisions, and conceptual understanding.

## Modeling Approach

The warehouse uses a **hybrid architecture**:

- **Core Schema** (`core`): Normalized 3NF design for operational data integrity
- **Data Marts** (`mart_*`): Dimensional star schema for analytical performance

This approach provides:
- Strong data integrity through normalization
- Fast query performance through dimensional modeling
- Clear separation between operational and analytical concerns

## Core Schema

**Purpose**: Store normalized operational data with referential integrity.

**Design Pattern**: Third Normal Form (3NF)

### Entity Groups

#### Teams & Players
- Player profiles and demographics
- Team information and home stadiums
- Player-team relationships over time (which players played for which teams in which seasons)
- Player statistics aggregated by season

#### Competitions
- Leagues and their governing associations
- Seasons (time periods for competitions)
- League-season combinations (which leagues ran in which seasons)
- Team participation in leagues

#### Matches
- Match events with home/away teams
- Detailed match statistics (goals, possession, xG, cards, shots, fouls)
- Venue and referee assignments
- Date and attendance information

#### Infrastructure
- Stadiums with capacity and location
- Referees and their associations
- Association hierarchies (FIFA → Continental → National)

#### Geography
- Countries for location references
- Used by players (nationality), teams, stadiums, associations

### Design Decisions

**Why UUIDs?**
- Support for distributed data collection
- No collision risk across different sources
- Future-proof for system integration

**Why Normalization?**
- Eliminates data redundancy
- Ensures referential integrity
- Makes updates consistent
- Provides single source of truth

**Junction Tables**:
- `players_teams`: Many-to-many (players can play for multiple teams over time)
- `teams_leagues_seasons`: Tracks which teams participated in which leagues per season
- `leagues_seasons`: Defines which leagues operated in which seasons
- `associations_self_relations`: Hierarchical relationships (e.g., UEFA under FIFA)

## Data Marts

**Purpose**: Provide optimized analytical models for specific use cases.

**Design Pattern**: Dimensional modeling (star schema)

### Dimensional Modeling Principles

Each mart follows the star schema pattern:

```
         Dimension
              |
Dimension -- FACT -- Dimension
              |
         Dimension
```

**Benefits**:
- Intuitive for business users
- Optimized for analytical queries (fewer joins)
- Easy to aggregate and filter
- Simple to extend with new dimensions

**Grain**: Each fact table has a clearly defined grain (level of detail per row)

### Match Performance Mart

**Schema**: `mart_match_performance`

**Purpose**: Analyze team performance in individual matches.

**Grain**: One row per team per match

**Use Cases**:
- Compare team statistics across matches
- Analyze home vs away performance
- Track possession and xG trends
- Identify patterns in cards and fouls

**Dimensions**:
- Team (who played)
- Match (venue, attendance, referee)
- Season (when)
- League (competition context)

**Measures**:
- Goals and timing (full-time, half-time)
- Possession percentage
- Expected goals (xG)
- Disciplinary (cards)
- Shot accuracy (on/off target)
- Fouls and corners

**Design Note**: Currently stores home team perspective. Can be extended to include away team data or use UNION to create symmetric rows.

### Player Performance Mart

**Schema**: `mart_player_performance`

**Purpose**: Analyze individual player performance across seasons and teams.

**Grain**: One row per player per team per season per league

**Use Cases**:
- Scout players by statistics
- Track player development over time
- Compare players across leagues
- Identify goal scorers and assist leaders

**Dimensions**:
- Player (demographics, position, nationality)
- Team (which team they played for)
- Season (time period)
- League (competition level and context)

**Measures**:
- Playing time (minutes, appearances)
- Goals and assists
- Penalties (scored and missed)
- Defensive (clean sheets, goals conceded)
- Disciplinary (yellow/red cards)

**Design Note**: Supports tracking players who transferred between teams within a season through the composite grain.

### Team Standings Mart

**Schema**: `mart_team_standings`

**Purpose**: Calculate league standings and rankings.

**Grain**: One row per team per season per league

**Use Cases**:
- Generate league tables
- Calculate goal difference
- Track win/loss/draw records
- Compare team performance across seasons

**Dimensions**:
- Team (team identity)
- Season (time period)
- League (competition context and tier)

**Measures**:
- Match results (wins, losses, draws)
- Points (3 for win, 1 for draw)
- Goals scored and conceded
- Matches played

**Calculation Logic**: Derived from match results in core schema, handling both home and away 
matches to compute complete standings.

## Metadata Schema

**Schema**: `metadata`

**Purpose**: Track ETL execution and data lineage.

**Key Table**: `etl_logs`

Stores:
- Job execution history
- Success/failure status
- Rows affected (loaded, updated, deleted)
- Timing information
- Error messages

**Use Cases**:
- Troubleshooting failed jobs
- Monitoring data freshness
- Auditing data changes
- Performance analysis

