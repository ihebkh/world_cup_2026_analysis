# World Cup 2026 — Data Pipeline

End-to-end data pipeline for the FIFA World Cup 2026, built with **PySpark** and orchestrated with **Prefect**.  
Implements a **Medallion Architecture** (Bronze → Silver → Gold) with a star-schema data warehouse loaded into **PostgreSQL**.

---

## Architecture

```
API / CSV files
      │
      ▼
┌─────────────┐
│  EXTRACTION │  Fetch data from API → CSV files in DATA/
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   BRONZE    │  CSV → Parquet (raw, no transformation)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Parquet → Parquet (cleaned, typed, deduplicated)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Parquet → PostgreSQL (star schema / data warehouse)
└─────────────┘
```

### Gold Layer — Star Schema

**Dimension tables**
| Table | Description |
|---|---|
| `dim_equipe` | Teams (id, name, country, coach…) |
| `dim_joueur` | Players (id, name, position, nationality…) |
| `dim_stade` | Stadiums (id, name, city) |
| `dim_date` | Date calendar (id, day, month, year, week…) |

**Fact tables**
| Table | Description |
|---|---|
| `fact_match` | One row per match (scores, result, FKs to all dims) |
| `fact_buteur` | Scorer stats per player (goals, assists, penalties…) |
| `fact_classement` | Group stage standings per team |

---

## Project Structure

```
world cup 2026/
├── DATA/                          # Raw CSV source files
│   ├── equipes.csv
│   ├── joueurs.csv
│   ├── buteurs.csv
│   ├── matchs/
│   └── classements/
│
├── data extraction/               # API extraction scripts
│   ├── extract_teams.py
│   ├── extarct_players.py
│   ├── extract_scores.py
│   └── extract_standings.py
│
├── bronze/                        # Bronze scripts + Parquet output
│   ├── equipe_bronze.py
│   ├── joueurs_bronze.py
│   ├── buteurs_bronze.py
│   ├── matches_joues_bronze.py
│   ├── macthes_non_joues_bronze.py
│   ├── classement_group_stage_bronze.py
│   └── equipes/  joueurs/  buteurs/  matchs_joues/  matchs_non_joues/  classement_group_stage/
│
├── silver/                        # Silver scripts + Parquet output
│   ├── silver_equipes.py
│   ├── silver_joueurs.py
│   ├── silver_buteurs.py
│   ├── silver_matchs_joues.py
│   ├── silver_matchs_non_joues.py
│   ├── silver_classement_group_stage.py
│   └── equipes/  joueurs/  buteurs/  matchs_joues/  matchs_non_joues/  classement_group_stage/
│
├── gold/                          # Gold scripts (output → PostgreSQL)
│   ├── config.py                  # DB connection settings
│   ├── gold_dim_equipe.py
│   ├── gold_dim_joueur.py
│   ├── gold_dim_stade.py
│   ├── gold_dim_date.py
│   ├── gold_fact_match.py
│   ├── gold_fact_buteur.py
│   ├── gold_fact_classement.py
│   └── run_gold.py                # Run gold layer standalone
│
└── orchestrator/
    └── pipeline.py                # Prefect flow — runs every 24 hours
```

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.9+ |
| Java (JDK) | 11 or 17 (required by Spark) |
| Apache Spark | 3.4+ |
| PostgreSQL | 13+ |

**Python packages**
```
pip install pyspark prefect
```

---

## PostgreSQL Setup

1. Create the database:
```sql
CREATE DATABASE worldcup2026;
```

2. Edit credentials in [gold/config.py](gold/config.py) if needed:
```python
PG_HOST     = "localhost"
PG_PORT     = "5432"
PG_DATABASE = "worldcup2026"
PG_USER     = "postgres"
PG_PASSWORD = "admin"
```

The JDBC driver (`org.postgresql:postgresql:42.7.3`) is downloaded automatically by Spark on first run — no manual installation needed.

---

## Running the Pipeline

### Option 1 — Full automated pipeline (recommended)

Open **two terminals**:

**Terminal 1 — Start the Prefect server**
```powershell
prefect server start
```
Wait until you see `Prefect UI available at http://127.0.0.1:4200`.

**Terminal 2 — Start the pipeline (polls every 24 hours)**
```powershell
cd "c:\Users\khmir\Desktop\world cup 2026"
python orchestrator/pipeline.py
```

**Terminal 3 — Trigger an immediate run**
```powershell
prefect deployment run 'WorldCup2026 Pipeline/worldcup2026-daily'
```

Monitor runs in the Prefect UI: **http://127.0.0.1:4200**

---

### Option 2 — Run each layer manually

Each script must be run **from its own folder** (relative paths depend on cwd).

```powershell
# Bronze
cd "c:\Users\khmir\Desktop\world cup 2026\bronze"
python equipe_bronze.py
python joueurs_bronze.py
# ... (other bronze scripts)

# Silver
cd "c:\Users\khmir\Desktop\world cup 2026\silver"
python silver_equipes.py
python silver_joueurs.py
# ... (other silver scripts)

# Gold (all dims then facts)
cd "c:\Users\khmir\Desktop\world cup 2026\gold"
python run_gold.py
```

---

## Disable Prefect Telemetry (optional)

If you see `getaddrinfo failed` warnings in the logs:
```powershell
prefect config set PREFECT_SEND_ANONYMOUS_USAGE_STATS=false
```

---

## Data Flow Details

```
extraction  →  DATA/*.csv
bronze      →  reads DATA/  writes bronze/<table>/ (Parquet)
silver      →  reads bronze/<table>/  writes silver/<table>/ (Parquet + cleaning)
gold dims   →  reads silver/  writes PostgreSQL dim_*
gold facts  →  reads silver/  writes PostgreSQL fact_*
```

Execution order enforced by orchestrator:
1. Extraction (parallel, 2 retries)
2. Bronze (parallel, 1 retry)
3. Silver (parallel, 1 retry)
4. Gold Dimensions (parallel, 1 retry)
5. Gold Facts (parallel, 1 retry — after dims complete)
