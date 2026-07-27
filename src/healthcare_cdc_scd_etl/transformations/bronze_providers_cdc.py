"""Bronze: ingest provider CDC JSON."""
from pyspark import pipelines as dp


@dp.table(
    name="bronze_providers_cdc",
    comment="Raw provider CDC events from Postgres-style landing volume",
)
def bronze_providers_cdc():
    root = spark.conf.get("cdc.landing_root", "/Volumes/workspace/hc_cdc_lakehouse/cdc_landing")
    path = f"{root}/providers"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaHints",
            "op STRING, event_ts TIMESTAMP, provider_id STRING, npi STRING, full_name STRING, "
            "specialty STRING, facility STRING, status STRING",
        )
        .option("cloudFiles.includeExistingFiles", "true")
        .load(path)
    )
