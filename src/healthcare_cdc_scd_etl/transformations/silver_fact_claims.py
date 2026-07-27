"""Silver: claims fact table SCD Type 1 (latest claim state) via Auto CDC."""
from pyspark import pipelines as dp
from pyspark.sql.functions import expr


@dp.temporary_view(name="claims_cdc_events")
def claims_cdc_events():
    return (
        spark.readStream.table("bronze_claims_cdc")
        .filter("claim_id IS NOT NULL AND event_ts IS NOT NULL")
    )


dp.create_streaming_table(
    name="fact_claims",
    comment="Claims fact SCD Type 1 — latest status/amounts per claim_id",
)

dp.create_auto_cdc_flow(
    target="fact_claims",
    source="claims_cdc_events",
    keys=["claim_id"],
    sequence_by="event_ts",
    stored_as_scd_type=1,
    apply_as_deletes=expr("op = 'd' OR op = 'D'"),
    except_column_list=["op", "_rescued_data"],
)
