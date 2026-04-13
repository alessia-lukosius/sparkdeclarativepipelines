# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG      = spark.conf.get("CATALOG")
VOL_AGENCIES = spark.conf.get("VOL_AGENCIES")

AGENCIES_PATH = f"/Volumes/{CATALOG}/landing/{VOL_AGENCIES}"

# COMMAND ----------

@dp.temporary_view(
    name="agencies_source_vw",
    comment="Streaming view de agencies com data_referencia derivada do nome do arquivo",
)
def agencies_source_vw():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", f"{AGENCIES_PATH}/_schema")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("header", "true")
             .load(AGENCIES_PATH)
             # Extrai YYYYMM do nome do arquivo e converte direto para o 1º dia do mês
             # Ex: agencies_202605.csv → 2026-05-01
             .withColumn(
                 "data_referencia",
                 F.to_date(
                     F.regexp_extract(F.col("_metadata.file_path"), r"agencies_(\d{6})\.csv", 1),
                     "yyyyMM",
                 ),
             )
    )

# COMMAND ----------

dp.create_streaming_table(
    name="bronze_agencies",
    comment="Histórico versionado de agências (SCD Type 2, chaveado por agency_id)",
    table_properties={
        "quality": "bronze",
    }
)

dp.create_auto_cdc_flow(
    target="bronze_agencies",
    source="agencies_source_vw",
    keys=["agency_id"],
    sequence_by=F.col("data_referencia"),
    stored_as_scd_type=2,
)

# COMMAND ----------

@dp.materialized_view(
    name="silver_agencies",
    comment=(
        "Agências versionadas (SCD Type 2). Contém histórico completo + flag "
        "is_current para consumo analítico."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect("valid_period",      "__START_AT IS NOT NULL")
@dp.expect("agency_name_not_null", "agency_name IS NOT NULL")
def silver_agencies():
    return (
        dp.read("bronze_agencies")
          .withColumn("is_current", F.col("__END_AT").isNull())
    )
