-- Aggregate totals from contracts with certification attached
-- One row per (agency, prime/business, vendor_type)

DROP TABLE IF EXISTS ppt.contract_powerbi_cert_totals;

CREATE TABLE ppt.contract_powerbi_cert_totals AS
SELECT
  c.contract_holder                               AS agency_name,
  UPPER(TRIM(c.vendor_type))                      AS vendor_type,        -- 'PRIME'/'SUBCONTRACTOR' (or 'SUB')
  c.prime_vendor,                                                     -- prime on the contract
  c.business_name,                                                   -- vendor on the row (prime or sub)
  NULLIF(TRIM(c.taxidnumber::text), '')          AS tax_id,             -- row vendor tax id (text)
  NULLIF(TRIM(c.b2g_id::text), '')               AS b2g_id,             -- row vendor b2g id (text, if present)
  c.certification_type,                                               -- row vendor cert (already attached)
  c.matched_tax_id::text                         AS matched_tax_id,     -- from vendor matching
  c.match_method_cert                             AS match_method_cert,  -- 'tax_id' | 'b2g_id' | 'name' | 'unmatched'
  SUM(COALESCE(c.amount_paid, 0))                AS total_amount
FROM ppt.contract_powerbi_cert c
WHERE c.contract_holder IS NOT NULL
  AND c.business_name  IS NOT NULL
GROUP BY
  c.contract_holder,
  UPPER(TRIM(c.vendor_type)),
  c.prime_vendor,
  c.business_name,
  NULLIF(TRIM(c.taxidnumber::text), ''),
  NULLIF(TRIM(c.b2g_id::text), ''),
  c.certification_type,
  c.matched_tax_id::text,
  c.match_method_cert;

-- (optional) indexes to keep it snappy
CREATE INDEX IF NOT EXISTS ix_cpct_agency_vendor
  ON ppt.contract_powerbi_cert_totals (agency_name, vendor_type, prime_vendor, business_name);
CREATE INDEX IF NOT EXISTS ix_cpct_vendor_type
  ON ppt.contract_powerbi_cert_totals (vendor_type);
CREATE INDEX IF NOT EXISTS ix_cpct_tax_b2g
  ON ppt.contract_powerbi_cert_totals (tax_id, b2g_id);
