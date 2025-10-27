-- Build contracts table with certification attached per business
-- Priority: tax_id -> b2g_id -> name

DROP TABLE IF EXISTS ppt.contract_powerbi_cert;

CREATE TABLE ppt.contract_powerbi_cert AS
WITH
-- 1) One best (Active + hierarchy) cert row per vendor in vendor_search
vendor_dim AS (
  SELECT *
  FROM (
    SELECT
      NULLIF(TRIM(v.tax_id::text), '')                  AS tax_id,          -- vendor tax id (text)
      NULLIF(TRIM(v.b2gnow_vendor_number::text), '')    AS b2g_id,          -- vendor b2g id (text to avoid cast issues)
      REGEXP_REPLACE(                                   -- canonical vendor name
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(TRIM(v.business_name)), '[^A-Z0-9 ]', '', 'g'),
          '\s+', ' ', 'g'
        ),
        '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
      )                                                AS canon_name,
      v.certification_type,
      v.certification_status,
      ROW_NUMBER() OVER (
        PARTITION BY
          COALESCE(NULLIF(TRIM(v.tax_id::text), ''), 'NO_TAX'),
          COALESCE(NULLIF(TRIM(v.b2gnow_vendor_number::text), ''), 'NO_B2G'),
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(UPPER(TRIM(v.business_name)), '[^A-Z0-9 ]', '', 'g'),
              '\s+', ' ', 'g'
            ),
            '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
          )
        ORDER BY
          CASE WHEN v.certification_status = 'Active' THEN 0 ELSE 1 END,
          CASE v.certification_type                          -- your hierarchy
            WHEN 'MBE' THEN 1
            WHEN 'MWBE' THEN 2
            WHEN 'WBE' THEN 3
            WHEN 'CBE' THEN 4
            WHEN 'SEDBE' THEN 5
            WHEN 'VOB' THEN 6
            ELSE 999
          END
      ) AS rn
    FROM ppt.vendor_search_results_taxid v
  ) s
  WHERE rn = 1
    AND certification_status = 'Active'
),

-- 2) Normalize contracts rows (keep the columns you requested)
contracts_norm AS (
  SELECT
    NULLIF(TRIM(c.b2gnow_id::text), '')                 AS b2g_id,          -- keep as text; safe for join
    c.prime_vendor,
    c.contract_holder,
    c.business_name,
    UPPER(TRIM(c.vendor_type))                          AS vendor_type,     -- normalized for consistency
    c.amount_paid,
    c.audit_period,
    NULLIF(TRIM(c.taxidnumber::text), '')               AS taxidnumber_txt, -- contract-side tax id
    REGEXP_REPLACE(                                     -- canonicalized business name
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    )                                                   AS canon_name
  FROM ppt.contract_powerbi c
),

-- 3) tax-id matches (highest confidence)
tax_matches AS (
  SELECT
    c.*,
    v.certification_type,
    v.tax_id       AS matched_tax_id,
    v.b2g_id       AS matched_b2g_id,
    'tax_id'::text AS match_method_cert
  FROM contracts_norm c
  JOIN vendor_dim v
    ON c.taxidnumber_txt IS NOT NULL
   AND v.tax_id = c.taxidnumber_txt
),

-- 4) b2g-id matches (only for rows not matched by tax-id)
b2g_matches AS (
  SELECT
    c.*,
    v.certification_type,
    v.tax_id       AS matched_tax_id,
    v.b2g_id       AS matched_b2g_id,
    'b2g_id'::text AS match_method_cert
  FROM contracts_norm c
  LEFT JOIN tax_matches t
    ON t.b2g_id = c.b2g_id
   AND t.contract_holder = c.contract_holder
   AND t.business_name = c.business_name
   AND t.amount_paid = c.amount_paid
   AND t.audit_period = c.audit_period
  JOIN vendor_dim v
    ON t.b2g_id IS NULL
   AND c.b2g_id IS NOT NULL
   AND v.b2g_id = c.b2g_id
),

-- 5) name matches (only for rows not matched by tax-id or b2g-id)
name_matches AS (
  SELECT
    c.*,
    v.certification_type,
    v.tax_id       AS matched_tax_id,
    v.b2g_id       AS matched_b2g_id,
    'name'::text   AS match_method_cert
  FROM contracts_norm c
  LEFT JOIN tax_matches t
    ON t.business_name = c.business_name
   AND t.contract_holder = c.contract_holder
   AND t.amount_paid = c.amount_paid
   AND t.audit_period = c.audit_period
  LEFT JOIN b2g_matches b
    ON b.business_name = c.business_name
   AND b.contract_holder = c.contract_holder
   AND b.amount_paid = c.amount_paid
   AND b.audit_period = c.audit_period
  JOIN vendor_dim v
    ON t.business_name IS NULL
   AND b.business_name IS NULL
   AND c.canon_name = v.canon_name
),

-- 6) pick the first available match per contracts row: tax_id > b2g_id > name
contracts_with_cert AS (
  SELECT * FROM tax_matches
  UNION ALL
  SELECT * FROM b2g_matches
  UNION ALL
  SELECT * FROM name_matches
)

-- 7) Final table: every contracts row + certification fields (or unmatched)
SELECT
  -- keep these core columns as requested
  c.b2g_id,
  c.prime_vendor,
  c.contract_holder,
  c.business_name,
  c.vendor_type,
  c.amount_paid,
  c.audit_period,
  c.taxidnumber_txt            AS taxidnumber,

  -- attached certification
  cw.certification_type,
  cw.matched_tax_id,
  cw.matched_b2g_id,
  COALESCE(cw.match_method_cert, 'unmatched') AS match_method_cert
FROM contracts_norm c
LEFT JOIN contracts_with_cert cw
  ON cw.business_name = c.business_name
 AND cw.contract_holder = c.contract_holder
 AND cw.amount_paid = c.amount_paid
 AND cw.audit_period = c.audit_period;
