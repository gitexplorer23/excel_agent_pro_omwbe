DROP TABLE IF EXISTS ppt.agency_higher_ed_prime_cert_summary;

CREATE TABLE ppt.agency_higher_ed_prime_cert_summary AS
WITH
-- Canonicalize primes
primes_canon AS (
  SELECT
    p.source,
    p.agency_number,
    p.agency_name,
    p.prime_vendor,
    p.tax_id AS prime_tax_id,
    p.prime_certification_type,
    p.matched_tax_id AS prime_matched_tax_id,
    p.match_method AS prime_match_method,
    p.prime_total_amount,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(p.prime_vendor)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_prime_vendor,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(p.agency_name)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_agency_name
  FROM ppt.agency_higher_ed_prime_totals p
),

-- Canonicalize contract summary and get tax_id
contract_summary_canon AS (
  SELECT
    c.prime_vendor,
    c.agency_name,
    c.total_subs_amount,
    c.certified_subs_spend,
    c.uncertified_subs_spend,
    c.sub_count,
    (SELECT DISTINCT ct.tax_id 
     FROM ppt.contract_powerbi_cert_totals ct 
     WHERE ct.prime_vendor = c.prime_vendor 
       AND ct.agency_name = c.agency_name 
       AND UPPER(TRIM(ct.vendor_type)) = 'PRIME'
     LIMIT 1) AS prime_tax_id,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.prime_vendor)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_prime_vendor,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.agency_name)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_agency_name
  FROM ppt.contract_prime_cert_spend_summary c
),

-- Step 1: Match by tax_id - aggregate to prevent duplicates
matched_by_tax AS (
  SELECT
    p.source,
    p.agency_number,
    p.agency_name,
    p.prime_vendor,
    p.prime_tax_id,
    p.prime_certification_type,
    p.prime_matched_tax_id,
    p.prime_match_method,
    p.prime_total_amount,
    SUM(c.total_subs_amount) AS total_subs_amount,
    SUM(c.certified_subs_spend) AS certified_subs_spend,
    SUM(c.uncertified_subs_spend) AS uncertified_subs_spend,
    SUM(c.sub_count) AS sub_count,
    'tax_id' AS match_method
  FROM primes_canon p
  INNER JOIN contract_summary_canon c
    ON p.prime_matched_tax_id IS NOT NULL
    AND c.prime_tax_id IS NOT NULL
    AND p.prime_matched_tax_id::text = c.prime_tax_id::text
    AND p.canon_agency_name = c.canon_agency_name
  GROUP BY p.source, p.agency_number, p.agency_name, p.prime_vendor,
           p.prime_tax_id, p.prime_certification_type, p.prime_matched_tax_id,
           p.prime_match_method, p.prime_total_amount
),

-- Step 2: Primes that didn't match by tax_id
primes_unmatched_by_tax AS (
  SELECT p.*
  FROM primes_canon p
  LEFT JOIN matched_by_tax mt
    ON p.source = mt.source
    AND p.agency_number = mt.agency_number
    AND p.agency_name = mt.agency_name
    AND p.prime_vendor = mt.prime_vendor
  WHERE mt.source IS NULL
),

-- Step 3: Match remaining by name - aggregate to prevent duplicates
matched_by_name AS (
  SELECT
    p.source,
    p.agency_number,
    p.agency_name,
    p.prime_vendor,
    p.prime_tax_id,
    p.prime_certification_type,
    p.prime_matched_tax_id,
    p.prime_match_method,
    p.prime_total_amount,
    SUM(c.total_subs_amount) AS total_subs_amount,
    SUM(c.certified_subs_spend) AS certified_subs_spend,
    SUM(c.uncertified_subs_spend) AS uncertified_subs_spend,
    SUM(c.sub_count) AS sub_count,
    'name' AS match_method
  FROM primes_unmatched_by_tax p
  INNER JOIN contract_summary_canon c
    ON p.canon_prime_vendor = c.canon_prime_vendor
    AND p.canon_agency_name = c.canon_agency_name
  GROUP BY p.source, p.agency_number, p.agency_name, p.prime_vendor,
           p.prime_tax_id, p.prime_certification_type, p.prime_matched_tax_id,
           p.prime_match_method, p.prime_total_amount
),

-- Combine all matches
all_matches AS (
  SELECT * FROM matched_by_tax
  UNION ALL
  SELECT * FROM matched_by_name
)

-- Final output
SELECT
  p.source,
  p.agency_number,
  p.agency_name,
  p.prime_vendor,
  p.prime_tax_id,
  p.prime_certification_type,
  p.prime_matched_tax_id,
  p.prime_match_method,
  p.prime_total_amount,
  
  COALESCE(m.total_subs_amount, 0) AS total_subs_amount,
  COALESCE(m.certified_subs_spend, 0) AS certified_subs_spend,
  COALESCE(m.uncertified_subs_spend, 0) AS uncertified_subs_spend,
  COALESCE(m.sub_count, 0) AS sub_count,
  m.match_method,
  
  -- Certified Spend Calculation
  CASE
    WHEN UPPER(TRIM(p.prime_certification_type)) IN ('MBE', 'WBE', 'MWBE', 'SEDBE', 'CBE') THEN
      CASE
        WHEN COALESCE(m.uncertified_subs_spend, 0) > 0 
        THEN p.prime_total_amount - m.uncertified_subs_spend
        ELSE p.prime_total_amount
      END
    ELSE
      COALESCE(m.certified_subs_spend, 0)
  END AS total_certified_spend

FROM primes_canon p
LEFT JOIN all_matches m
  ON p.source = m.source
  AND p.agency_number = m.agency_number
  AND p.agency_name = m.agency_name
  AND p.prime_vendor = m.prime_vendor;

CREATE INDEX IF NOT EXISTS ix_prime_cert_summary_agency_prime
  ON ppt.agency_higher_ed_prime_cert_summary (agency_name, prime_vendor);
CREATE INDEX IF NOT EXISTS ix_prime_cert_summary_source
  ON ppt.agency_higher_ed_prime_cert_summary (source);
CREATE INDEX IF NOT EXISTS ix_prime_cert_summary_match_method
  ON ppt.agency_higher_ed_prime_cert_summary (match_method);