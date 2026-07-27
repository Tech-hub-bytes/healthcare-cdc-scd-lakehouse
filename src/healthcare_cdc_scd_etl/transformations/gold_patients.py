"""Gold: current patients + SCD2 history view for point-in-time analytics."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_patients_current",
    comment="Current patient attributes (SCD2 where __END_AT IS NULL)",
)
def gold_patients_current():
    return (
        spark.read.table("dim_patients")
        .filter("__END_AT IS NULL")
        .select(
            "patient_id",
            "first_name",
            "last_name",
            "gender",
            "zip_code",
            "insurance_plan",
            "status",
            F.col("__START_AT").alias("valid_from"),
        )
    )


@dp.materialized_view(
    name="gold_patient_insurance_changes",
    comment="Patient insurance plan change history from SCD2",
)
def gold_patient_insurance_changes():
    return (
        spark.read.table("dim_patients")
        .select(
            "patient_id",
            "insurance_plan",
            "zip_code",
            F.col("__START_AT").alias("valid_from"),
            F.col("__END_AT").alias("valid_to"),
            F.when(F.col("__END_AT").isNull(), F.lit(True))
            .otherwise(F.lit(False))
            .alias("is_current"),
        )
    )
