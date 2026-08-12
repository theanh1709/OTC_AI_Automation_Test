# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # 00 — Setup and ingest
# MAGIC Creates the schema/volume and loads the supplied Excel workbook into Delta tables.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd

CURRENT_CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "otc_ai_test"
VOLUME = "source_files"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CURRENT_CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CURRENT_CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

BASE_PATH = f"/Volumes/{CURRENT_CATALOG}/{SCHEMA}/{VOLUME}"
FILE_PATH = f"{BASE_PATH}/OTC_AI_Automation_Developer_Test_Dummy_Data.xlsx"

print("Current catalog:", CURRENT_CATALOG)
print("Upload the workbook to:")
print(FILE_PATH)

# COMMAND ----------

# Stop here the first time if the file has not been uploaded yet.
files = [x.name for x in dbutils.fs.ls(BASE_PATH)]
assert "OTC_AI_Automation_Developer_Test_Dummy_Data.xlsx" in files, (
    f"Workbook not found. Upload it to {FILE_PATH} and rerun this cell."
)

# COMMAND ----------

otc_pd = pd.read_excel(FILE_PATH, sheet_name="OTC_RAW", engine="openpyxl")
customer_pd = pd.read_excel(FILE_PATH, sheet_name="MASTER_CUSTOMER", engine="openpyxl")

print("OTC_RAW:", otc_pd.shape)
print("MASTER_CUSTOMER:", customer_pd.shape)

assert otc_pd.shape[0] == 10000, "Expected 10,000 OTC rows"
assert "KUNNR" in otc_pd.columns and "KUNNR" in customer_pd.columns

# COMMAND ----------

otc_df = spark.createDataFrame(otc_pd)
customer_df = spark.createDataFrame(customer_pd)

# Normalize Excel NaN values in string columns into NULLs.
for field in otc_df.schema.fields:
    if field.dataType.simpleString() == "string":
        otc_df = otc_df.withColumn(
            field.name,
            F.when(
                F.lower(F.trim(F.col(field.name))).isin("nan", ""),
                F.lit(None)
            ).otherwise(F.col(field.name))
        )

for field in customer_df.schema.fields:
    if field.dataType.simpleString() == "string":
        customer_df = customer_df.withColumn(
            field.name,
            F.when(
                F.lower(F.trim(F.col(field.name))).isin("nan", ""),
                F.lit(None)
            ).otherwise(F.col(field.name))
        )

# COMMAND ----------

OTC_TABLE = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_raw`"
CUSTOMER_TABLE = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`master_customer`"

(
    otc_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OTC_TABLE)
)

(
    customer_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CUSTOMER_TABLE)
)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {OTC_TABLE}) AS otc_rows,
  (SELECT COUNT(*) FROM {CUSTOMER_TABLE}) AS customer_rows
"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {OTC_TABLE} LIMIT 10"))