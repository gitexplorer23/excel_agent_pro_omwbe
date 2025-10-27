-- Final certified spend per (agency, prime)
-- Uses prime totals from ppt.agency_higher_ed_with_cert_flags,
-- adds subs totals from ppt.contract_powerbi_cert, then computes certified_spend.

DROP TABLE IF EXISTS ppt.agency_higher_ed_certified_spend;

CREATE TABLE ppt.agency_higher_ed_certified_spend AS
WITH
-- 1) Sum subs by (agency, prime) from contracts
subs_by_agency_prime AS (
  SELECT
    -- canonicalized agency and prime for joining
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.contract_holder)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_agency,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.prime_vendor)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_prime_vendor,
    SUM(CASE WHEN c.vendor_type IN ('SUBCONTRACTOR','SUB')
              AND c.certification_type IS NOT NULL
             THEN c.amount_paid ELSE 0 END) AS subs_certified_amount,
    SUM(CASE WHEN c.vendor_type IN ('SUBCONTRACTOR','SUB')
              AND c.certification_type IS NULL
             THEN c.amount_paid ELSE 0 END) AS subs_uncertified_amount
  FROM ppt.contract_powerbi_cert c
  WHERE c.business_name IS NOT NULL
    AND c.prime_vendor IS NOT NULL
    AND c.contract_holder IS NOT NULL
  GROUP BY 1,2
),

-- 2) Canonicalize agency/prime on the HE/Agency table for joining
ah_norm AS (
  SELECT
    f.*,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(f.agency_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_agency,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(f.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_prime_vendor
  FROM ppt.agency_higher_ed_with_cert_flags f
)

-- 3) Final: keep every row from HE/Agency, add subs totals and certified_spend
SELECT
  a.source,
  a.agency_number,
  a.agency_name,
  a.business_name,
  a.tax_id,
  a.total_amount,
  a.certification_type,
  a.matched_tax_id,
  a.match_method,
  a.in_contracts_business_name,
  a.has_vendor_type_prime,
  a.has_vendor_type_sub,
  a.matched_taxid_to_contracts,
  a.match_method_contract,

  COALESCE(s.subs_certified_amount, 0)   AS subs_certified_amount,
  COALESCE(s.subs_uncertified_amount, 0) AS subs_uncertified_amount,

  CASE
    WHEN a.certification_type IS NOT NULL
      THEN GREATEST(a.total_amount::numeric - COALESCE(s.subs_uncertified_amount, 0)::numeric, 0)
    ELSE COALESCE(s.subs_certified_amount, 0)::numeric
  END AS certified_spend
FROM ah_norm a
LEFT JOIN subs_by_agency_prime s
  ON a.canon_agency = s.canon_agency
 AND a.canon_prime_vendor = s.canon_prime_vendor;
