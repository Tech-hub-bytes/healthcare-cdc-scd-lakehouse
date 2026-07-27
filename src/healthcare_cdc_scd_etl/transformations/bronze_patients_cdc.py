"""Bronze: ingest patient CDC JSON (simulated Postgres CDC landing)."""
from pyspark import pipelines as dp


@dp.table(
    name="bronze_patients_cdc",
    comment="Raw patient CDC events from Postgres-style landing volume",
)
def bronze_patients_cdc():
    root = spark.conf.get("cdc.landing_root", "/Volumes/workspace/hc_cdc_lakehouse/cdc_landing")
    path = f"{root}/patients"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaHints",
            "op STRING, event_ts TIMESTAMP, patient_id STRING, first_name STRING, "
            "last_name STRING, gender STRING, zip_code STRING, insurance_plan STRING, status STRING",
        )
        .option("cloudFiles.includeExistingFiles", "true")
        .load(path)
    )
