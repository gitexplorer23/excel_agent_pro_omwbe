-- Aggregate totals from contracts with certification attached
-- One row per (agency, prime/business, vendor_type) with ALL original columns
DROP TABLE IF EXISTS ppt.contract_powerbi_cert_totals2;
CREATE TABLE ppt.contract_powerbi_cert_totals2 AS
SELECT
  -- All original columns from contract_powerbi_cert
  c.b2g_id,
  c.prime_vendor,
  c.contract_holder AS agency_name,
  c.business_name,
  UPPER(TRIM(c.vendor_type)) AS vendor_type,  -- 'PRIME'/'SUBCONTRACTOR' (or 'SUB')
  
  -- Tax and certification info
  NULLIF(TRIM(c.taxidnumber::text), '') AS tax_id,
  c.certification_type,
  c.matched_tax_id::text AS matched_tax_id,
  c.matched_b2g_id::text AS matched_b2g_id,
  c.match_method_cert,
  
  -- Audit period (taking MAX in case there are multiple, though should be consistent)
  MAX(c.audit_period) AS audit_period,
  
  -- Aggregated amount
  SUM(COALESCE(c.amount_paid, 0)) AS total_amount

FROM ppt.contract_powerbi_cert c
WHERE c.contract_holder IS NOT NULL
  AND c.business_name IS NOT NULL
GROUP BY
  c.b2g_id,
  c.prime_vendor,
  c.contract_holder,
  c.business_name,
  UPPER(TRIM(c.vendor_type)),
  NULLIF(TRIM(c.taxidnumber::text), ''),
  c.certification_type,
  c.matched_tax_id::text,
  c.matched_b2g_id::text,
  c.match_method_cert;

-- Indexes to keep it snappy
CREATE INDEX IF NOT EXISTS ix_cpct_agency_vendor
  ON ppt.contract_powerbi_cert_totals (agency_name, vendor_type, prime_vendor, business_name);
CREATE INDEX IF NOT EXISTS ix_cpct_vendor_type
  ON ppt.contract_powerbi_cert_totals (vendor_type);
CREATE INDEX IF NOT EXISTS ix_cpct_tax_b2g
  ON ppt.contract_powerbi_cert_totals (tax_id, b2g_id);
CREATE INDEX IF NOT EXISTS ix_cpct_b2g_id
  ON ppt.contract_powerbi_cert_totals (b2g_id);
CREATE INDEX IF NOT EXISTS ix_cpct_prime_vendor
  ON ppt.contract_powerbi_cert_totals (prime_vendor);