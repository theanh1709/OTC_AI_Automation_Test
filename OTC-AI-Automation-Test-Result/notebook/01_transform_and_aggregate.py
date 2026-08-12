# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — OTC transformation and aggregation

# COMMAND ----------

CURRENT_CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "otc_ai_test"

OTC = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_raw`"
CUSTOMER = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`master_customer`"
TRANSFORMED = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_transformed`"
SUMMARY = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_summary`"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {TRANSFORMED}
USING DELTA
AS
SELECT
    o.*,

    CONCAT(
        CAST(o.BELNR AS STRING), '-',
        CAST(o.BUZEI AS STRING), '-',
        CAST(o.GJAHR AS STRING)
    ) AS source_key,

    CASE
        WHEN COALESCE(o.DAYS_OVERDUE, 0) > 0 THEN TRUE
        ELSE FALSE
    END AS is_overdue,

    CASE
        WHEN COALESCE(o.RISK_SCORE, 0) >= 70
          OR COALESCE(o.DAYS_OVERDUE, 0) > 60
          OR (
              o.EXCEPTION_REASON IS NOT NULL
              AND LOWER(TRIM(o.EXCEPTION_REASON)) <> 'none'
          )
        THEN TRUE
        ELSE FALSE
    END AS is_high_risk,

    CASE
        WHEN o.DISPUTE_STATUS IS NOT NULL
         AND LOWER(TRIM(o.DISPUTE_STATUS)) <> 'none'
        THEN TRUE
        ELSE FALSE
    END AS has_dispute,

    CASE
        WHEN COALESCE(TRIM(o.RECEIPT_MATCH_STATUS), '') <> 'Matched'
        THEN TRUE
        ELSE FALSE
    END AS has_receipt_exception,

    c.CREDIT_LIMIT_SGD,
    c.COLLECTION_OWNER,
    c.RISK_TIER

FROM {OTC} o
LEFT JOIN {CUSTOMER} c
    ON o.KUNNR = c.KUNNR

WHERE
    COALESCE(o.AR_STATUS, '') <> 'Closed'
    AND o.BLART IN ('DR', 'DG')
    AND COALESCE(o.DMBTR, 0) <> 0
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SUMMARY}
USING DELTA
AS
SELECT
    BUKRS,
    CUSTOMER_SEGMENT,
    REGION,
    AR_STATUS,
    RECEIPT_MATCH_STATUS,

    COUNT(*) AS invoice_count,
    ROUND(SUM(DMBTR), 2) AS open_amount_sgd,
    ROUND(AVG(DAYS_OVERDUE), 2) AS avg_days_overdue,

    SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) AS high_risk_count,
    SUM(CASE WHEN has_dispute THEN 1 ELSE 0 END) AS dispute_count,
    SUM(CASE WHEN has_receipt_exception THEN 1 ELSE 0 END) AS exception_count

FROM {TRANSFORMED}

GROUP BY
    BUKRS,
    CUSTOMER_SEGMENT,
    REGION,
    AR_STATUS,
    RECEIPT_MATCH_STATUS
""")

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) AS working_rows,
    SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) AS high_risk_rows,
    SUM(CASE WHEN has_dispute THEN 1 ELSE 0 END) AS dispute_rows,
    SUM(CASE WHEN has_receipt_exception THEN 1 ELSE 0 END) AS receipt_exception_rows,
    SUM(
      CASE WHEN is_high_risk OR has_dispute OR has_receipt_exception
           THEN 1 ELSE 0 END
    ) AS llm2_candidate_rows
FROM {TRANSFORMED}
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT *
FROM {SUMMARY}
ORDER BY open_amount_sgd DESC
LIMIT 30
"""))

# COMMAND ----------

# Data-quality checks useful for LLM #1 and interview discussion.
display(spark.sql(f"""
SELECT
    SUM(CASE WHEN KUNNR IS NULL THEN 1 ELSE 0 END) AS missing_customer_id,
    SUM(CASE WHEN COLLECTION_OWNER IS NULL THEN 1 ELSE 0 END) AS missing_collection_owner,
    SUM(CASE WHEN CREDIT_LIMIT_SGD IS NULL THEN 1 ELSE 0 END) AS unmatched_customer_master,
    SUM(CASE WHEN DAYS_OVERDUE IS NULL THEN 1 ELSE 0 END) AS missing_days_overdue,
    SUM(CASE WHEN RISK_SCORE IS NULL THEN 1 ELSE 0 END) AS missing_risk_score
FROM {TRANSFORMED}
"""))