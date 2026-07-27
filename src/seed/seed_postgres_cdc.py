# Databricks notebook source
# MAGIC %md
# MAGIC # Seed simulated PostgreSQL CDC
# MAGIC Writes Debezium-style CDC JSON into a Unity Catalog volume (stands in for
# MAGIC Postgres → CDC → landing). First run = wave 1; later runs append wave 2+ for SCD2.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "hc_cdc_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`cdc_landing`")

base = f"/Volumes/{catalog}/{schema}/cdc_landing"
for entity in ("patients", "providers", "claims"):
    dbutils.fs.mkdirs(f"{base}/{entity}")

# COMMAND ----------

import json
from datetime import datetime, timezone

existing = [f.name for f in dbutils.fs.ls(f"{base}/patients")]
if not any(n.startswith("wave1_") for n in existing):
    wave_n = 1
elif not any(n.startswith("wave2_") for n in existing):
    wave_n = 2
else:
    wave_n = 3

ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def put_jsonl(path: str, rows: list) -> None:
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows) + "\n", True)

# COMMAND ----------

if wave_n == 1:
    patients = [
        {"op": "c", "event_ts": "2024-01-10T08:00:00.000Z", "patient_id": "P001", "first_name": "Ava", "last_name": "Nguyen", "gender": "F", "zip_code": "94105", "insurance_plan": "PPO Gold", "status": "active"},
        {"op": "c", "event_ts": "2024-01-10T08:01:00.000Z", "patient_id": "P002", "first_name": "Marcus", "last_name": "Lee", "gender": "M", "zip_code": "10001", "insurance_plan": "HMO Basic", "status": "active"},
        {"op": "c", "event_ts": "2024-01-11T09:00:00.000Z", "patient_id": "P003", "first_name": "Sofia", "last_name": "Patel", "gender": "F", "zip_code": "60601", "insurance_plan": "Medicare Advantage", "status": "active"},
        {"op": "u", "event_ts": "2024-03-15T12:00:00.000Z", "patient_id": "P001", "first_name": "Ava", "last_name": "Nguyen", "gender": "F", "zip_code": "94107", "insurance_plan": "PPO Gold", "status": "active"},
        {"op": "u", "event_ts": "2024-06-01T10:00:00.000Z", "patient_id": "P002", "first_name": "Marcus", "last_name": "Lee", "gender": "M", "zip_code": "10001", "insurance_plan": "PPO Silver", "status": "active"},
    ]
    providers = [
        {"op": "c", "event_ts": "2024-01-05T08:00:00.000Z", "provider_id": "DR100", "npi": "1999999999", "full_name": "Dr. Elena Ruiz", "specialty": "Family Medicine", "facility": "Bay Clinic", "status": "active"},
        {"op": "c", "event_ts": "2024-01-05T08:01:00.000Z", "provider_id": "DR200", "npi": "1888888888", "full_name": "Dr. Jamal Okonkwo", "specialty": "Cardiology", "facility": "Metro Heart", "status": "active"},
        {"op": "u", "event_ts": "2024-04-20T11:00:00.000Z", "provider_id": "DR100", "npi": "1999999999", "full_name": "Dr. Elena Ruiz", "specialty": "Family Medicine", "facility": "Bay Clinic - Mission", "status": "active"},
    ]
    claims = [
        {"op": "c", "event_ts": "2024-02-01T14:00:00.000Z", "claim_id": "CLM1001", "patient_id": "P001", "provider_id": "DR100", "service_date": "2024-01-28", "icd10_primary": "J06.9", "cpt_primary": "99213", "billed_amount": 185.0, "paid_amount": 140.0, "claim_status": "paid"},
        {"op": "c", "event_ts": "2024-02-10T15:00:00.000Z", "claim_id": "CLM1002", "patient_id": "P002", "provider_id": "DR200", "service_date": "2024-02-05", "icd10_primary": "I10", "cpt_primary": "93000", "billed_amount": 420.0, "paid_amount": 0.0, "claim_status": "denied"},
        {"op": "c", "event_ts": "2024-03-01T16:00:00.000Z", "claim_id": "CLM1003", "patient_id": "P003", "provider_id": "DR100", "service_date": "2024-02-20", "icd10_primary": "E11.9", "cpt_primary": "99214", "billed_amount": 265.0, "paid_amount": 210.0, "claim_status": "paid"},
        {"op": "u", "event_ts": "2024-03-05T09:00:00.000Z", "claim_id": "CLM1002", "patient_id": "P002", "provider_id": "DR200", "service_date": "2024-02-05", "icd10_primary": "I10", "cpt_primary": "93000", "billed_amount": 420.0, "paid_amount": 315.0, "claim_status": "paid"},
    ]
else:
    patients = [
        {"op": "u", "event_ts": "2024-09-01T08:00:00.000Z", "patient_id": "P001", "first_name": "Ava", "last_name": "Nguyen-Smith", "gender": "F", "zip_code": "94107", "insurance_plan": "EPO Plus", "status": "active"},
        {"op": "c", "event_ts": "2024-09-02T08:00:00.000Z", "patient_id": "P004", "first_name": "Noah", "last_name": "Garcia", "gender": "M", "zip_code": "75201", "insurance_plan": "Medicaid", "status": "active"},
        {"op": "d", "event_ts": "2024-09-03T08:00:00.000Z", "patient_id": "P003", "first_name": "Sofia", "last_name": "Patel", "gender": "F", "zip_code": "60601", "insurance_plan": "Medicare Advantage", "status": "inactive"},
    ]
    providers = [
        {"op": "u", "event_ts": "2024-09-01T09:00:00.000Z", "provider_id": "DR200", "npi": "1888888888", "full_name": "Dr. Jamal Okonkwo", "specialty": "Interventional Cardiology", "facility": "Metro Heart", "status": "active"},
    ]
    claims = [
        {"op": "c", "event_ts": "2024-09-10T14:00:00.000Z", "claim_id": "CLM2001", "patient_id": "P001", "provider_id": "DR100", "service_date": "2024-09-05", "icd10_primary": "M54.5", "cpt_primary": "97110", "billed_amount": 310.0, "paid_amount": 250.0, "claim_status": "paid"},
        {"op": "u", "event_ts": "2024-09-12T10:00:00.000Z", "claim_id": "CLM1003", "patient_id": "P003", "provider_id": "DR100", "service_date": "2024-02-20", "icd10_primary": "E11.9", "cpt_primary": "99214", "billed_amount": 265.0, "paid_amount": 210.0, "claim_status": "adjusted"},
    ]

put_jsonl(f"{base}/patients/wave{wave_n}_{ts}.json", patients)
put_jsonl(f"{base}/providers/wave{wave_n}_{ts}.json", providers)
put_jsonl(f"{base}/claims/wave{wave_n}_{ts}.json", claims)

print(f"Seeded wave {wave_n} into {base}")
display(spark.createDataFrame([{"wave": wave_n, "patients": len(patients), "providers": len(providers), "claims": len(claims)}]))
