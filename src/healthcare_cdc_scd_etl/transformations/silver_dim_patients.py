"""Silver: patient dimension with SCD Type 2 history via Auto CDC."""
from pyspark import pipelines as dp
from pyspark.sql.functions import expr


@dp.temporary_view(name="patients_cdc_events")
def patients_cdc_events():
    return (
        spark.readStream.table("bronze_patients_cdc")
        .filter("patient_id IS NOT NULL AND event_ts IS NOT NULL")
    )


dp.create_streaming_table(
    name="dim_patients",
    comment="Patient dimension SCD Type 2 (__START_AT / __END_AT). Current rows: __END_AT IS NULL",
)

dp.create_auto_cdc_flow(
    target="dim_patients",
    source="patients_cdc_events",
    keys=["patient_id"],
    sequence_by="event_ts",
    stored_as_scd_type=2,
    apply_as_deletes=expr("op = 'd' OR op = 'D'"),
    except_column_list=["op", "_rescued_data"],
    track_history_column_list=[
        "first_name",
        "last_name",
        "zip_code",
        "insurance_plan",
        "status",
    ],
)
