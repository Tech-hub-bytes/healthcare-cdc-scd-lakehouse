"""Bronze: ingest claims CDC JSON."""
from pyspark import pipelines as dp


@dp.table(
    name="bronze_claims_cdc",
    comment="Raw claim CDC events from Postgres-style landing volume",
)
def bronze_claims_cdc():
    root = spark.conf.get("cdc.landing_root", "/Volumes/workspace/hc_cdc_lakehouse/cdc_landing")
    path = f"{root}/claims"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaHints",
            "op STRING, event_ts TIMESTAMP, claim_id STRING, patient_id STRING, provider_id STRING, "
            "service_date DATE, icd10_primary STRING, cpt_primary STRING, "
            "billed_amount DOUBLE, paid_amount DOUBLE, claim_status STRING",
        )
        .option("cloudFiles.includeExistingFiles", "true")
        .load(path)
    )
