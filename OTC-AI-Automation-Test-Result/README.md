## 1. Overview

This project is a simple end-to-end OTC (Order-to-Cash) / Accounts
Receivable prototype built with Databricks Free Edition.

It loads the provided Excel data, transforms and aggregates OTC records,
calls two LLM steps, validates the decision JSON, and writes valid
results back to a Delta table named `decision_signal`.

## 2. Architecture

``` text
Excel Workbook
      |
      v
Databricks Volume
      |
      v
Raw Delta Tables
- otc_raw
- master_customer
      |
      v
SQL Transformation
Filter + Join + Risk/Exception Flags
      |
      v
otc_transformed
      |
      +--------------------------+
      |                          |
      v                          v
OTC Aggregation          High-risk/Exception Records
      |                          |
      v                          v
otc_summary                   LLM #2
      |                          |
      v                          v
LLM #1                  Decision Signal JSON
      |                          |
      v                          v
OTC Analysis              JSON Validation
                                 |
                                 v
                          decision_signal
```

### Notebooks

1.  `00_setup_and_ingest` -- Create the schema/volume and load Excel
    data.
2.  `01_transform_and_aggregate` -- Filter, join, create flags, and
    aggregate OTC data.
3.  `02_llm1_otc_analysis` -- Generate OTC insights from the grouped
    summary.
4.  `03_llm2_decision_signals` -- Generate, validate, and write decision
    signals.
5.  `04_validation` -- Validate the final data and output.

## 3. Setup

### Requirements

-   Databricks Free Edition
-   `OTC_AI_Automation_Developer_Test_Dummy_Data.xlsx`
-   A Foundation Model endpoint available in the Databricks workspace

### Steps

1.  Create a Databricks Free Edition workspace.
2.  Import all notebooks into one workspace folder.
3.  Run `00_setup_and_ingest`.
4.  Upload the Excel workbook to the Unity Catalog Volume path printed
    by the notebook.
5.  Rerun the remaining cells in notebook 00.
6.  Confirm `otc_raw` and `master_customer` exist.
7.  Run `01_transform_and_aggregate`.
8.  Confirm `otc_transformed` and `otc_summary` exist.
9.  Check an available Foundation Model and update `MODEL_ENDPOINT` in
    notebooks 02 and 03 if needed.
10. Run notebooks 02, 03, and 04.

## 4. Transformation

The working dataset applies the required rules:

-   `AR_STATUS <> Closed`
-   `BLART IN (DR, DG)`
-   `DMBTR <> 0`
-   `source_key = BELNR-BUZEI-GJAHR`
-   `is_overdue = DAYS_OVERDUE > 0`
-   High risk when `RISK_SCORE >= 70`, `DAYS_OVERDUE > 60`, or an
    exception exists
-   Dispute when `DISPUTE_STATUS` contains a dispute value
-   Receipt exception when `RECEIPT_MATCH_STATUS <> Matched`
-   Join `MASTER_CUSTOMER` using `KUNNR`

The summary is grouped by `BUKRS`, `CUSTOMER_SEGMENT`, `REGION`,
`AR_STATUS`, and `RECEIPT_MATCH_STATUS`.

## 5. LLM Processing

### LLM #1 -- OTC Analysis

LLM #1 receives the aggregated `otc_summary` instead of the raw 10,000
records.

It returns:

-   Top 5 risk observations
-   Top 3 customer/segment actions
-   Data quality issues
-   Executive summary under 80 words

### LLM #2 -- Decision Signal

LLM #2 receives records where `is_high_risk`, `has_dispute`, or
`has_receipt_exception` is true.

For this prototype, a small subset is processed to generate at least 20
decision signals.

Before write-back, the JSON is validated for required fields, allowed
values, numeric values, confidence range, source keys, and risk rules.

Valid results are written to `decision_signal`.

## 6. Assumptions

-   Blank dispute/exception values are treated as no dispute/no
    exception where applicable.
-   `source_key` is used as the operational traceability key.
-   LLM #2 processes a demo subset of at least 20 records instead of
    every exception record.
-   `json_payload` stores the decision JSON without recursively
    including itself.
-   Foundation Model availability can differ between Databricks
    workspaces.
-   This solution is a technical-test prototype, not a production
    system.

## 7. Limitations

-   Databricks Free Edition has compute and model usage limits.
-   Model availability depends on the workspace.
-   LLM responses can vary.
-   Human approval is not implemented.
-   Production retry, monitoring, alerting, and dead-letter handling are
    not implemented.
-   LLM #2 processes a demo subset to reduce model usage.
-   Notebooks are run manually instead of through a production
    Databricks Job.

## 8. Rerun Steps

Run:

``` text
00_setup_and_ingest
        ↓
01_transform_and_aggregate
        ↓
02_llm1_otc_analysis
        ↓
03_llm2_decision_signals
        ↓
04_validation
```

Then verify:

-   `otc_raw` contains the source data.
-   `master_customer` contains customer master data.
-   `otc_transformed` contains the filtered/enriched records.
-   `otc_summary` contains the grouped summary.
-   LLM #1 returns OTC analysis.
-   `decision_signal` contains at least 20 valid signals.
-   Validation shows no invalid or duplicate signals.

The write-back uses `source_key` so rerunning the prototype updates existing signals instead of creating duplicates.

## 9. Optional Stretch Questions

### How would you prevent the LLM call from freezing a workflow while waiting for missing information?

I would validate required data before calling the LLM. Incomplete records would be skipped or stored for later review. For production, I would also add a timeout and limited retry so other records can continue processing.

### How would you validate that the JSON output is safe before writing back?

I would parse the JSON and validate required fields, data types, allowed values, confidence range, and `source_key`. Important business rules such as risk level should also be recalculated in code before write-back.

### How would you design human approval for Critical signals?

I would save Critical signals with a `Pending Approval` status. An AR manager could approve or reject the recommendation before a downstream action is executed. The decision and reviewer comments should be stored for audit.

### How would you secure API credentials in a high cyber-security enterprise environment?

I would not put API keys directly in notebook code. I would use Databricks secret management or an approved enterprise secret store, apply least-privilege access, and restrict access to the model/API endpoint.

### How would this design change if the source were SAP BTP / SAP HANA instead of Excel?

I would mainly replace the ingestion layer. Instead of uploading Excel, Databricks would read SAP BTP / SAP HANA through an approved connection or ingestion method. The transformation, aggregation, LLM, validation, and `decision_signal` layers could remain mostly the same.
