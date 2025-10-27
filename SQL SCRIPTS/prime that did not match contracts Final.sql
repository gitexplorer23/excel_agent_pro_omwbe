WITH
-- Canonicalize contract primes
contract_summary_canon AS (
  SELECT
    c.prime_vendor,
    c.agency_name,
    c.total_subs_amount,
    c.certified_subs_spend,
    c.uncertified_subs_spend,
    c.sub_count,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.prime_vendor)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_prime_vendor,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.agency_name)), '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', '', 'gi'),
        '[^A-Z0-9 ]', '', 'g'),
      '\s+', ' ', 'g')) AS canon_agency_name,
    -- Get tax_id from underlying contract data
    (SELECT DISTINCT ct.tax_id 
     FROM ppt.contract_powerbi_cert_totals ct 
     WHERE ct.prime_vendor = c.prime_vendor 
       AND ct.agency_name = c.agency_name 
       AND UPPER(TRIM(ct.vendor_type)) = 'PRIME'
     LIMIT 1) AS prime_tax_id
  FROM ppt.contract_prime_cert_spend_summary c
),

-- Canonicalize agency primes
primes_canon AS (
  SELECT
    p.agency_name,
    p.prime_vendor,
    p.matched_tax_id,
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
)

-- Find contracts that don't match agency/higher ed
SELECT
  c.prime_vendor,
  c.agency_name,
  c.prime_tax_id,
  c.total_subs_amount,
  c.certified_subs_spend,
  c.uncertified_subs_spend,
  c.sub_count
FROM contract_summary_canon c
LEFT JOIN primes_canon p
  ON (
    -- Try tax_id match
    (c.prime_tax_id IS NOT NULL 
     AND p.matched_tax_id IS NOT NULL
     AND c.prime_tax_id::text = p.matched_tax_id::text
     AND c.canon_agency_name = p.canon_agency_name)
    OR
    -- Try name match
    (c.canon_prime_vendor = p.canon_prime_vendor
     AND c.canon_agency_name = p.canon_agency_name)
  )
WHERE p.prime_vendor IS NULL  -- No match found
ORDER BY c.certified_subs_spend DESC;