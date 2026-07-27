"""Gold: claim KPIs for analytics / AI/BI."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_claim_kpis",
    comment="Claim volume, billed/paid totals, and denial rate by status",
)
def gold_claim_kpis():
    return (
        spark.read.table("fact_claims")
        .groupBy("claim_status")
        .agg(
            F.count("*").alias("claim_count"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid"),
            F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
        )
    )


@dp.materialized_view(
    name="gold_claims_by_provider",
    comment="Paid claims rolled up by current provider specialty/facility",
)
def gold_claims_by_provider():
    claims = spark.read.table("fact_claims")
    providers = (
        spark.read.table("dim_providers")
        .filter("__END_AT IS NULL")
        .select("provider_id", "full_name", "specialty", "facility")
    )
    return (
        claims.join(providers, "provider_id", "left")
        .groupBy("provider_id", "full_name", "specialty", "facility")
        .agg(
            F.count("*").alias("claim_count"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid"),
        )
    )
