# Architecture

End-to-end **Postgres-style CDC → Databricks Lakehouse → Analytics** for healthcare claims data.

## Overview

```mermaid
flowchart TB
  subgraph Source["Source (simulated Postgres CDC)"]
    PG["PostgreSQL / OLTP<br/>(patients, providers, claims)"]
    CDC["CDC events<br/>op: c / u / d + event_ts"]
    PG --> CDC
  end

  subgraph Landing["Landing"]
    VOL["UC Volume<br/>/Volumes/workspace/hc_cdc_lakehouse/cdc_landing/"]
  end

  subgraph Bronze["Bronze — raw CDC"]
    BP["bronze_patients_cdc"]
    BPR["bronze_providers_cdc"]
    BC["bronze_claims_cdc"]
  end

  subgraph Silver["Silver — curated"]
    DP["dim_patients<br/>SCD Type 2"]
    DPR["dim_providers<br/>SCD Type 2"]
    FC["fact_claims<br/>SCD Type 1"]
  end

  subgraph Gold["Gold — analytics"]
    G1["gold_claim_kpis"]
    G2["gold_claims_by_provider"]
    G3["gold_patients_current"]
    G4["gold_patient_insurance_changes"]
  end

  subgraph Consume["Consume"]
    SQL["SQL Warehouse / AI/BI"]
  end

  CDC -->|"seed notebook (JSON)"| VOL
  VOL -->|"Auto Loader"| BP & BPR & BC
  BP -->|"Auto CDC"| DP
  BPR -->|"Auto CDC"| DPR
  BC -->|"Auto CDC"| FC
  DP --> G3 & G4
  DPR --> G2
  FC --> G1 & G2
  G1 & G2 & G3 & G4 --> SQL
```

## Layers

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| **Source** | OLTP / Postgres-style tables | Operational patients, providers, claims |
| **Landing** | Unity Catalog volume (JSON) | CDC change feed on disk (Debezium-style `c`/`u`/`d`) |
| **Bronze** | Auto Loader streaming tables | Append-only raw CDC events |
| **Silver** | Auto CDC | SCD2 dimensions + SCD1 claim facts |
| **Gold** | Materialized views | KPIs and history ready for SQL / AI/BI |
| **Orchestration** | Databricks Job | Seed landing → refresh Lakeflow pipeline |

## SCD semantics

- **SCD Type 2** (`dim_patients`, `dim_providers`): full history with `__START_AT` / `__END_AT`. Current rows: `WHERE __END_AT IS NULL`.
- **SCD Type 1** (`fact_claims`): latest claim status and amounts only (upsert / delete by `claim_id`).

## Catalog objects

| Layer | Objects |
|-------|---------|
| Landing | `/Volumes/workspace/hc_cdc_lakehouse/cdc_landing/{patients,providers,claims}` |
| Bronze | `bronze_patients_cdc`, `bronze_providers_cdc`, `bronze_claims_cdc` |
| Silver | `dim_patients`, `dim_providers`, `fact_claims` |
| Gold | `gold_claim_kpis`, `gold_claims_by_provider`, `gold_patients_current`, `gold_patient_insurance_changes` |

Schema: `workspace.hc_cdc_lakehouse`

## Code map

| Path | Role |
|------|------|
| `src/seed/seed_postgres_cdc.py` | Writes simulated CDC JSON to the volume |
| `src/.../transformations/bronze_*.py` | Auto Loader → bronze CDC tables |
| `src/.../transformations/silver_*.py` | Auto CDC → SCD2 dims / SCD1 facts |
| `src/.../transformations/gold_*.py` | Gold materialized views |
| `resources/cdc_lakehouse.job.yml` | Job: seed → SDP pipeline |
| `resources/healthcare_cdc_scd_etl.pipeline.yml` | Lakeflow Declarative Pipeline |

## Related: standalone dbt project

dbt lives in a **separate** folder/repo: `healthcare-cdc-dbt` (not nested here).

## Production path

Replace the seed notebook with **Lakeflow Connect**, **JDBC**, or real Postgres/Lakebase CDC into the same volume (or bronze Delta). Silver Auto CDC and Gold views stay the same.
