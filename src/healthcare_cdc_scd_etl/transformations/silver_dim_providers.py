"""Silver: provider dimension with SCD Type 2 history via Auto CDC."""
from pyspark import pipelines as dp
from pyspark.sql.functions import expr


@dp.temporary_view(name="providers_cdc_events")
def providers_cdc_events():
    return (
        spark.readStream.table("bronze_providers_cdc")
        .filter("provider_id IS NOT NULL AND event_ts IS NOT NULL")
    )


dp.create_streaming_table(
    name="dim_providers",
    comment="Provider dimension SCD Type 2. Current rows: __END_AT IS NULL",
)

dp.create_auto_cdc_flow(
    target="dim_providers",
    source="providers_cdc_events",
    keys=["provider_id"],
    sequence_by="event_ts",
    stored_as_scd_type=2,
    apply_as_deletes=expr("op = 'd' OR op = 'D'"),
    except_column_list=["op", "_rescued_data"],
    track_history_column_list=["full_name", "specialty", "facility", "status"],
)
