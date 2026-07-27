# Healthcare CDC / SCD Lakehouse (Lakeflow SDP)

End-to-end **Postgres-style CDC → Databricks Lakehouse → Analytics** using **Lakeflow Auto Loader + Auto CDC** (not dbt).

For the **standalone dbt** pipeline, see: https://github.com/Tech-hub-bytes/healthcare-cdc-dbt

## Architecture

Full diagram: **[ARCHITECTURE.md](./ARCHITECTURE.md)**

```mermaid
flowchart LR
  PG[Postgres CDC] --> VOL[UC Volume]
  VOL -->|Auto Loader| BR[Bronze]
  BR -->|Auto CDC| SV[Silver SCD2/SCD1]
  SV --> GD[Gold KPIs]
  GD --> BI[SQL / AI/BI]
```

| Layer | Tables |
|-------|--------|
| Landing | `/Volumes/workspace/hc_cdc_lakehouse/cdc_landing/{patients,providers,claims}` |
| Bronze | `bronze_patients_cdc`, `bronze_providers_cdc`, `bronze_claims_cdc` |
| Silver | `dim_patients` (SCD2), `dim_providers` (SCD2), `fact_claims` (SCD1) |
| Gold | `gold_claim_kpis`, `gold_claims_by_provider`, `gold_patients_current`, `gold_patient_insurance_changes` |

SCD2 current rows: `WHERE __END_AT IS NULL`.

## Deploy & run

```bash
databricks bundle validate -t dev --profile dbc-7c3eed4c
databricks bundle deploy -t dev --profile dbc-7c3eed4c
databricks bundle run cdc_lakehouse_job -t dev --profile dbc-7c3eed4c
```

## Databricks AI/BI report

[Healthcare CDC Lakehouse Claims Report](https://dbc-7c3eed4c-66bb.cloud.databricks.com/dashboardsv3/01f189cf7cd21b2eaca0833f798882fa/published)

## Related project

- **dbt e2e (separate repo/folder):** https://github.com/Tech-hub-bytes/healthcare-cdc-dbt
