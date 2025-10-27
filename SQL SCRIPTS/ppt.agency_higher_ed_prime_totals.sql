DROP TABLE IF EXISTS ppt.agency_higher_ed_prime_totals;

CREATE TABLE ppt.agency_higher_ed_prime_totals AS
SELECT
  f.source,                          -- carry as-is
  f.agency_number,
  f.agency_name,
  f.business_name         AS prime_vendor,
  f.tax_id,                                   -- HE/agency-side tax id
  -- certification + match info from the cert join (should be stable per prime)
  MAX(f.certification_type)       AS prime_certification_type,
  MAX(f.matched_tax_id)           AS matched_tax_id,
  MAX(f.match_method)             AS match_method,

  -- carry contract presence flags (any occurrence = TRUE)
  BOOL_OR(f.in_contracts_business_name) AS in_contracts_business_name,
  BOOL_OR(f.has_vendor_type_prime)      AS has_vendor_type_prime,
  BOOL_OR(f.has_vendor_type_sub)        AS has_vendor_type_sub,

  -- carry contract match metadata (pick a non-null representative)
  NULLIF(MAX(COALESCE(f.matched_taxid_to_contracts, '')), '') AS matched_taxid_to_contracts,
  NULLIF(MAX(COALESCE(f.match_method_contract, '')), '')      AS match_method_contract,

  -- aggregated prime total
  SUM(COALESCE(f.total_amount, 0)) AS prime_total_amount
FROM ppt.agency_higher_ed_with_cert_flags f
GROUP BY
  f.source,
  f.agency_number,
  f.agency_name,
  f.business_name,
  f.tax_id;

-- Helpful indexes for joins & drilldowns
CREATE INDEX IF NOT EXISTS ix_prime_totals_agency_prime
  ON ppt.agency_higher_ed_prime_totals (agency_name, prime_vendor);
CREATE INDEX IF NOT EXISTS ix_prime_totals_source
  ON ppt.agency_higher_ed_prime_totals (source);
