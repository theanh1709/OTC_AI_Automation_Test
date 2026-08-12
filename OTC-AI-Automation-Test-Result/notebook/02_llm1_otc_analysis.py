# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — LLM #1: OTC analytic insight
# MAGIC Sends only the grouped OTC summary, not 10,000 raw rows.

# COMMAND ----------

import json
from datetime import datetime, timezone

CURRENT_CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "otc_ai_test"

SUMMARY = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_summary`"
ANALYSIS_TABLE = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_analysis`"

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

summary_rows = [
    row.asDict(recursive=True)
    for row in spark.sql(f"""
        SELECT *
        FROM {SUMMARY}
        ORDER BY open_amount_sgd DESC
    """).collect()
]

summary_json = json.dumps(summary_rows, default=str)

print("Summary groups:", len(summary_rows))
print("Prompt JSON characters:", len(summary_json))

# COMMAND ----------

SYSTEM_PROMPT = """You are an OTC finance analyst.
Review the JSON summary generated from SAP-like AR data.

Return:
1. Top 5 risk observations.
2. Top 3 customer / segment actions.
3. Any data quality issues.
4. One executive summary under 80 words.

Rules:
- Use only the provided data.
- Do not invent missing facts.
- Focus on overdue invoices, receipt matching exceptions, disputes, and high-risk exposure.
"""

request_text = SYSTEM_PROMPT + "\n\nOTC grouped summary JSON:\n" + summary_json

# Escape a Python string so it can safely be embedded as a SQL string literal.
request_sql = request_text.replace("'", "''")

# COMMAND ----------

# Limit input size by taking top N records sorted by risk
from pyspark.sql.functions import col

top_n = 500  # Adjust based on token limits
top_summary_rows = [
    row.asDict(recursive=True)
    for row in spark.sql(f"""
        SELECT *
        FROM {SUMMARY}
        ORDER BY open_amount_sgd DESC
        LIMIT {top_n}
    """).collect()
]

top_summary_json = json.dumps(top_summary_rows, default=str)
request_text_filtered = SYSTEM_PROMPT + "\n\nOTC grouped summary JSON (top " + str(top_n) + " by open amount):\n" + top_summary_json
request_sql_filtered = request_text_filtered.replace("'", "''")

print("Sending", len(top_summary_rows), "summary groups")
print("Prompt JSON characters:", len(top_summary_json))

llm1_result = spark.sql(f"""
SELECT ai_query(
    '{MODEL_ENDPOINT}',
    '{request_sql_filtered}',
    modelParameters => named_struct('temperature', 0.0)
) AS analysis
""").first()["analysis"]

print(llm1_result)

# COMMAND ----------

analysis_df = spark.createDataFrame([{
    "analysis_id": datetime.now(timezone.utc).strftime("ANL-%Y%m%d%H%M%S"),
    "created_timestamp": datetime.now(timezone.utc).isoformat(),
    "llm_model": MODEL_ENDPOINT,
    "group_count": len(summary_rows),
    "analysis_text": llm1_result
}])

(
    analysis_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(ANALYSIS_TABLE)
)

# COMMAND ----------

display(spark.sql(f"""
SELECT *
FROM {ANALYSIS_TABLE}
ORDER BY created_timestamp DESC
LIMIT 5
"""))