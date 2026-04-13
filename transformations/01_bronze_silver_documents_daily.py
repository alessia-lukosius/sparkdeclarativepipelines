# Databricks notebook source
# Documents Notebook

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Lê parâmetros do pipeline SDP (configurados em Pipeline settings → Advanced → Configuration)
CATALOG       = spark.conf.get("CATALOG")
VOL_DOCUMENTS = spark.conf.get("VOL_DOCUMENTS")

# Volume de landing: /Volumes/{catalog}/landing/{vol_documents}
LANDING_PATH = f"/Volumes/{CATALOG}/landing/{VOL_DOCUMENTS}"

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------

@dp.temporary_view(
    name="documents_source_vw",
    comment="Streaming view sobre a landing de documents (Auto Loader, append-only)",
)
def documents_source_vw():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", f"{LANDING_PATH}/_schema")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("header", "true")
             .load(LANDING_PATH)
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

dp.create_streaming_table(
    name="bronze_documents_daily",
    comment="Estado atual dos documentos (SCD Type 1, chaveado por doc_id)",
    table_properties={"quality": "bronze"},
)

dp.create_auto_cdc_flow(
    target="bronze_documents_daily",
    source="documents_source_vw",
    keys=["doc_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
    except_column_list=["_source_file", "_ingested_at"],
)

# COMMAND ----------

silver_rules = {
    "valid_doc_id":       "doc_id IS NOT NULL",
    "valid_updated_at":   "updated_at IS NOT NULL",
    "doc_type_valido":    "doc_type IN ('TKT', 'EMD')",
    "agency_id_not_null": "agency_id IS NOT NULL",
    "amount_positivo":    "amount_value > 0",
    "status_valido":      "status IN ('ISSUED', 'CANCELLED', 'REFUNDED', 'EXCHANGED')",
    "currency_valida":    "currency IN ('BRL', 'USD', 'EUR')",
}

@dp.materialized_view(
    name="silver_documents",
    comment="Documents limpos e padronizados, prontos para consumo analítico",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(silver_rules)
def silver_documents():
    return (
        dp.read("bronze_documents_daily")
          .withColumn("doc_type",  F.trim(F.upper(F.col("doc_type"))))
          .withColumn("status",    F.trim(F.upper(F.col("status"))))
          .withColumn("currency",  F.trim(F.upper(F.col("currency"))))
          .withColumn("is_active", F.col("status") == F.lit("ISSUED"))
    )
