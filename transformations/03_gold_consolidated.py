# Databricks notebook source
# Gold Consolidated

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# --- Taxas de câmbio (PLACEHOLDER) ---
# Simplificação do MVP: taxas fixas. Num ambiente real, isto viria de uma
# tabela exchange_rates histórica, joinada pela data da venda (updated_at).
USD_TO_BRL = 6.00
EUR_TO_BRL = 6.50


@dp.materialized_view(
    name="gold_documents_consolidated",
    comment=(
        "Produto analítico final: vendas de passagem enriquecidas com a agência "
        "vigente. Inclui amount_value_brl normalizado para análise consolidada em reais."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("doc_id_nao_nulo",  "doc_id IS NOT NULL")
@dp.expect("agency_resolvida", "agency_id IS NOT NULL")
@dp.expect("amount_brl_positivo", "amount_value_brl > 0")


def gold_documents_consolidated():
    # --- Vendas de passagem (sistema atual) ---
    docs = (
        dp.read("silver_documents")
          .withColumn(
              "amount_value_brl",
              F.round(
                  F.when(F.col("currency") == "BRL", F.col("amount_value"))
                   .when(F.col("currency") == "USD", F.col("amount_value") * USD_TO_BRL)
                   .when(F.col("currency") == "EUR", F.col("amount_value") * EUR_TO_BRL)
                   .otherwise(None),
                  2,
              ),
          )
    )

    # --- Agências vigentes (snapshot atual do SCD2) ---
    agencies_atuais = (
        dp.read("silver_agencies")
          .where(F.col("is_current"))
          .select(
              F.col("agency_id"),
              F.col("agency_name"),
              F.col("city").alias("agency_city"),
              F.col("state").alias("agency_state"),
              F.col("data_referencia").alias("agency_ref_date"),
          )
    )

    # --- Join final ---
    return (
        docs.alias("d")
        .join(
            agencies_atuais.alias("a"),
            on=F.col("d.agency_id") == F.col("a.agency_id"),
            how="left",
        )
        .select(
            "d.doc_id",
            "d.doc_type",
            "d.amount_value",          # valor original na moeda de emissão
            "d.currency",              # moeda original
            "d.amount_value_brl",      # valor normalizado em BRL
            "d.status",
            "d.is_active",
            "d.updated_at",
            "a.agency_id",
            "a.agency_name",
            "a.agency_city",
            "a.agency_state",
            "a.agency_ref_date",
        )
    )
