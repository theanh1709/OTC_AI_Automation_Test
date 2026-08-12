# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 — LLM #2: Decision signals
# MAGIC Select exception records, send a controlled demo subset to LLM #2,
# MAGIC validate the JSON, then MERGE into `decision_signal`.

# COMMAND ----------

import json
from datetime import datetime, timezone
from decimal import Decimal

CURRENT_CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "otc_ai_test"

TRANSFORMED = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`otc_transformed`"
DECISION_TABLE = f"`{CURRENT_CATALOG}`.`{SCHEMA}`.`decision_signal`"

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Technical test asks for at least 20 generated signals or a clear demo subset.
DEMO_LIMIT = 20
BATCH_SIZE = 10

ALLOWED_RISK = {"Low", "Medium", "High", "Critical"}
ALLOWED_ACTIONS = {
    "No Action",
    "Monitor",
    "Collection Reminder",
    "Priority Collection Follow-up",
    "Request Remittance Review",
    "Escalate Dispute"
}

# COMMAND ----------

candidate_df = spark.sql(f"""
SELECT
    source_key,
    CAST(BUKRS AS STRING) AS company_code,
    CAST(KUNNR AS STRING) AS customer_id,
    CAST(DMBTR AS DOUBLE) AS amount_sgd,
    CAST(DAYS_OVERDUE AS INT) AS days_overdue,
    CAST(RISK_SCORE AS INT) AS risk_score,
    EXCEPTION_REASON AS exception_reason,
    DISPUTE_STATUS AS dispute_status,
    RECEIPT_MATCH_STATUS AS receipt_match_status,
    COLLECTION_OWNER AS owner_team,
    is_high_risk,
    has_dispute,
    has_receipt_exception
FROM {TRANSFORMED}
WHERE
       is_high_risk = TRUE
    OR has_dispute = TRUE
    OR has_receipt_exception = TRUE
ORDER BY
    CASE WHEN DAYS_OVERDUE > 90 OR RISK_SCORE >= 85 THEN 0 ELSE 1 END,
    DAYS_OVERDUE DESC,
    RISK_SCORE DESC
LIMIT {DEMO_LIMIT}
""")

display(candidate_df)

# COMMAND ----------

rows = [r.asDict(recursive=True) for r in candidate_df.collect()]

run_timestamp = datetime.now(timezone.utc).isoformat()

# Give deterministic IDs/timestamps to the model and require exact copying.
for idx, row in enumerate(rows, start=1):
    row["signal_id"] = f"SIG-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{idx:06d}"
    row["created_timestamp"] = run_timestamp
    row["llm_model"] = MODEL_ENDPOINT

# COMMAND ----------

SYSTEM_PROMPT = """You are a decision-signal generator for OTC exception handling.

For every input record, output exactly one JSON object.
Return a valid JSON array only.
No markdown.
No explanation.

Required fields in every object:
- signal_id
- source_key
- company_code
- customer_id
- risk_level
- recommended_action
- reason_code
- amount_sgd
- days_overdue
- confidence
- owner_team
- created_timestamp
- llm_model
- json_payload

Copy these input fields exactly:
signal_id, source_key, company_code, customer_id, amount_sgd,
days_overdue, owner_team, created_timestamp, llm_model.

Risk logic:
- Critical if days_overdue > 90 or risk_score >= 85
- High if days_overdue > 60 or exception exists
- Medium if days_overdue > 30
- Low otherwise

recommended_action must be exactly one of:
- No Action
- Monitor
- Collection Reminder
- Priority Collection Follow-up
- Request Remittance Review
- Escalate Dispute

Decision guidance:
- If a dispute exists, prefer "Escalate Dispute".
- Else if receipt_match_status is not "Matched", prefer "Request Remittance Review".
- Else if Critical or High, prefer "Priority Collection Follow-up".
- Else if Medium, prefer "Collection Reminder".
- Else use "Monitor" or "No Action".

reason_code must be a short uppercase underscore code based only on supplied fields,
for example OVERDUE_GT_90, RISK_SCORE_GE_85, RECEIPT_MATCH_EXCEPTION,
DISPUTE, EXCEPTION, OVERDUE_GT_60, OVERDUE_GT_30.

confidence must be a number between 0 and 1.

For json_payload:
Return a compact JSON string containing the decision fields for that object
EXCLUDING json_payload itself. Do not create recursive JSON.

Do not invent customer, amount, dates, owner, risk score, status, or exception facts.
"""

# COMMAND ----------

def call_llm2(batch):
    payload = json.dumps(batch, default=str, ensure_ascii=False)
    prompt = SYSTEM_PROMPT + "\n\nINPUT RECORDS:\n" + payload
    prompt_sql = prompt.replace("'", "''")

    return spark.sql(f"""
        SELECT ai_query(
            '{MODEL_ENDPOINT}',
            '{prompt_sql}',
            modelParameters => named_struct('temperature', 0.0)
        ) AS result
    """).first()["result"]

# COMMAND ----------

REQUIRED_FIELDS = [
    "signal_id",
    "source_key",
    "company_code",
    "customer_id",
    "risk_level",
    "recommended_action",
    "reason_code",
    "amount_sgd",
    "days_overdue",
    "confidence",
    "owner_team",
    "created_timestamp",
    "llm_model",
    "json_payload"
]

def validate_batch(raw_text, input_batch):
    try:
        parsed = json.loads(raw_text)
    except Exception as exc:
        raise ValueError(f"LLM output is not valid JSON: {exc}")

    if not isinstance(parsed, list):
        raise ValueError("LLM output must be a JSON array")

    if len(parsed) != len(input_batch):
        raise ValueError(
            f"Expected {len(input_batch)} output objects, received {len(parsed)}"
        )

    input_by_key = {str(x["source_key"]): x for x in input_batch}
    output_keys = set()

    validated = []

    for obj in parsed:
        if not isinstance(obj, dict):
            raise ValueError("Every array element must be a JSON object")

        missing = [f for f in REQUIRED_FIELDS if f not in obj]
        if missing:
            raise ValueError(f"Missing fields for {obj.get('source_key')}: {missing}")

        key = str(obj["source_key"])
        if key not in input_by_key:
            raise ValueError(f"Unknown source_key returned by model: {key}")

        if key in output_keys:
            raise ValueError(f"Duplicate source_key returned by model: {key}")
        output_keys.add(key)

        source = input_by_key[key]

        if obj["risk_level"] not in ALLOWED_RISK:
            raise ValueError(f"Invalid risk_level for {key}: {obj['risk_level']}")

        if obj["recommended_action"] not in ALLOWED_ACTIONS:
            raise ValueError(
                f"Invalid recommended_action for {key}: {obj['recommended_action']}"
            )

        try:
            confidence = float(obj["confidence"])
            amount_sgd = float(obj["amount_sgd"])
            days_overdue = int(obj["days_overdue"])
        except Exception:
            raise ValueError(f"Numeric conversion failed for {key}")

        if not 0 <= confidence <= 1:
            raise ValueError(f"Confidence out of range for {key}")

        # Traceability / anti-hallucination checks.
        exact_fields = [
            "signal_id",
            "company_code",
            "customer_id",
            "owner_team",
            "created_timestamp",
            "llm_model"
        ]
        for field in exact_fields:
            source_val = source.get(field)
            out_val = obj.get(field)
            if (source_val is None and out_val not in (None, "")) or (
                source_val is not None and str(source_val) != str(out_val)
            ):
                raise ValueError(
                    f"Model changed source field {field} for {key}: "
                    f"{source_val!r} -> {out_val!r}"
                )

        if abs(amount_sgd - float(source["amount_sgd"])) > 0.01:
            raise ValueError(f"Model changed amount_sgd for {key}")

        if days_overdue != int(source["days_overdue"]):
            raise ValueError(f"Model changed days_overdue for {key}")

        # Recalculate risk level deterministically and reject contradictions.
        exception_exists = (
            source.get("exception_reason") is not None
            and str(source.get("exception_reason")).strip().lower() != "none"
        )

        if int(source.get("days_overdue") or 0) > 90 or int(source.get("risk_score") or 0) >= 85:
            expected_risk = "Critical"
        elif int(source.get("days_overdue") or 0) > 60 or exception_exists:
            expected_risk = "High"
        elif int(source.get("days_overdue") or 0) > 30:
            expected_risk = "Medium"
        else:
            expected_risk = "Low"

        if obj["risk_level"] != expected_risk:
            raise ValueError(
                f"Risk rule mismatch for {key}: expected {expected_risk}, "
                f"received {obj['risk_level']}"
            )

        # Ensure json_payload itself is valid serialized JSON.
        try:
            payload_obj = json.loads(obj["json_payload"])
            if not isinstance(payload_obj, dict):
                raise ValueError
        except Exception:
            raise ValueError(f"json_payload is not a valid JSON object string for {key}")

        obj["amount_sgd"] = amount_sgd
        obj["days_overdue"] = days_overdue
        obj["confidence"] = confidence

        validated.append(obj)

    return validated

# COMMAND ----------

all_signals = []
raw_outputs = []

for start in range(0, len(rows), BATCH_SIZE):
    batch = rows[start:start + BATCH_SIZE]

    raw = call_llm2(batch)
    raw_outputs.append(raw)

    validated = validate_batch(raw, batch)
    all_signals.extend(validated)

print("Validated signals:", len(all_signals))
assert len(all_signals) >= 20, "Technical test expects at least 20 demo signals"

# COMMAND ----------

signals_df = spark.createDataFrame(all_signals)

signals_df.createOrReplaceTempView("decision_signal_stage")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DECISION_TABLE} (
    signal_id STRING,
    source_key STRING,
    company_code STRING,
    customer_id STRING,
    risk_level STRING,
    recommended_action STRING,
    reason_code STRING,
    amount_sgd DOUBLE,
    days_overdue INT,
    confidence DOUBLE,
    owner_team STRING,
    created_timestamp STRING,
    llm_model STRING,
    json_payload STRING
)
USING DELTA
""")

# COMMAND ----------

# Idempotent write-back: one current signal per source_key.
spark.sql(f"""
MERGE INTO {DECISION_TABLE} AS target
USING decision_signal_stage AS source
ON target.source_key = source.source_key

WHEN MATCHED THEN UPDATE SET
    target.signal_id = source.signal_id,
    target.company_code = source.company_code,
    target.customer_id = source.customer_id,
    target.risk_level = source.risk_level,
    target.recommended_action = source.recommended_action,
    target.reason_code = source.reason_code,
    target.amount_sgd = source.amount_sgd,
    target.days_overdue = source.days_overdue,
    target.confidence = source.confidence,
    target.owner_team = source.owner_team,
    target.created_timestamp = source.created_timestamp,
    target.llm_model = source.llm_model,
    target.json_payload = source.json_payload

WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(spark.sql(f"""
SELECT *
FROM {DECISION_TABLE}
ORDER BY
    CASE risk_level
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        ELSE 4
    END,
    days_overdue DESC
"""))