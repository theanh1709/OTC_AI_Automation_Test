-- OTC AI Automation Developer Test
-- Replace <catalog> with the catalog printed by notebook 00.

USE CATALOG <catalog>;
USE SCHEMA otc_ai_test;

-- 1. Raw ingestion
SELECT COUNT(*) AS otc_raw_count FROM otc_raw;
SELECT COUNT(*) AS master_customer_count FROM master_customer;

-- 2. Working dataset
SELECT COUNT(*) AS transformed_count
FROM otc_transformed;

-- 3. Validate the mandatory filter
SELECT COUNT(*) AS invalid_filtered_rows
FROM otc_transformed
WHERE AR_STATUS = 'Closed'
   OR BLART NOT IN ('DR','DG')
   OR DMBTR = 0;

-- Expected: 0

-- 4. source_key uniqueness
SELECT source_key, COUNT(*) AS n
FROM otc_transformed
GROUP BY source_key
HAVING COUNT(*) > 1;

-- Expected: no rows

-- 5. Aggregation preview
SELECT *
FROM otc_summary
ORDER BY open_amount_sgd DESC
LIMIT 25;

-- 6. LLM #2 candidate population
SELECT
    COUNT(*) AS llm2_candidate_rows
FROM otc_transformed
WHERE is_high_risk
   OR has_dispute
   OR has_receipt_exception;

-- 7. decision_signal required fields
SELECT COUNT(*) AS invalid_signal_rows
FROM decision_signal
WHERE signal_id IS NULL
   OR source_key IS NULL
   OR company_code IS NULL
   OR customer_id IS NULL
   OR risk_level IS NULL
   OR recommended_action IS NULL
   OR reason_code IS NULL
   OR amount_sgd IS NULL
   OR days_overdue IS NULL
   OR confidence IS NULL
   OR created_timestamp IS NULL
   OR llm_model IS NULL
   OR json_payload IS NULL;

-- Expected: 0

-- 8. enum validation
SELECT *
FROM decision_signal
WHERE risk_level NOT IN ('Low','Medium','High','Critical')
   OR recommended_action NOT IN (
       'No Action',
       'Monitor',
       'Collection Reminder',
       'Priority Collection Follow-up',
       'Request Remittance Review',
       'Escalate Dispute'
   );

-- Expected: no rows

-- 9. confidence validation
SELECT *
FROM decision_signal
WHERE confidence < 0 OR confidence > 1;

-- Expected: no rows

-- 10. traceability back to source
SELECT d.source_key
FROM decision_signal d
LEFT ANTI JOIN otc_transformed o
    ON d.source_key = o.source_key;

-- Expected: no rows

-- 11. duplicates after rerun
SELECT source_key, COUNT(*) AS n
FROM decision_signal
GROUP BY source_key
HAVING COUNT(*) > 1;

-- Expected: no rows
