DROP TABLE IF EXISTS ppt.contract_subs_with_cert_spend;

CREATE TABLE ppt.contract_subs_with_cert_spend AS
SELECT
  c.b2g_id,
  c.prime_vendor,
  c.agency_name,
  c.business_name,
  c.vendor_type,
  c.tax_id,
  c.certification_type,
  c.matched_tax_id,
  c.matched_b2g_id,
  c.match_method_cert,
  c.audit_period,
  c.total_amount,
  
  -- Certified spend: only count if qualifying certification
  CASE 
    WHEN UPPER(TRIM(c.certification_type)) IN ('MBE', 'WBE', 'MWBE', 'SEDBE', 'CBE')
    THEN c.total_amount
    ELSE 0
  END AS certified_spend
  
FROM ppt.contract_powerbi_cert_totals c
WHERE UPPER(TRIM(c.vendor_type)) IN ('SUBCONTRACTOR', 'SUB');

-- Indexes for joining
CREATE INDEX IF NOT EXISTS ix_contract_subs_cert_prime_agency
  ON ppt.contract_subs_with_cert_spend (prime_vendor, agency_name);
CREATE INDEX IF NOT EXISTS ix_contract_subs_cert_business
  ON ppt.contract_subs_with_cert_spend (business_name);
CREATE INDEX IF NOT EXISTS ix_contract_subs_cert_b2g
  ON ppt.contract_subs_with_cert_spend (b2g_id);