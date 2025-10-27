DROP TABLE IF EXISTS ppt.agency_higher_ed_prime_with_subs2;

CREATE TABLE ppt.agency_higher_ed_prime_with_subs2 AS
WITH
-- Get all subs from contracts with canonicalized names
contract_subs AS (
  SELECT
    c.agency_name,
    c.prime_vendor,
    c.tax_id,
    -- Canonicalize both prime and agency
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          UPPER(TRIM(c.prime_vendor)), 
          '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', 
          '', 
          'gi'
        ),
        '[^A-Z0-9 ]', '', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS canon_prime_vendor,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          UPPER(TRIM(c.agency_name)), 
          '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', 
          '', 
          'gi'
        ),
        '[^A-Z0-9 ]', '', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS canon_agency_name,
    c.b2g_id,
    c.business_name AS sub_business_name,
    c.vendor_type,
    c.certification_type AS sub_certification_type,
    c.matched_tax_id,
    c.matched_b2g_id,
    c.match_method_cert AS sub_match_method,
    c.audit_period,
    c.total_amount AS sub_amount
  FROM ppt.contract_powerbi_cert_totals c
  WHERE UPPER(TRIM(c.vendor_type)) IN ('SUBCONTRACTOR', 'SUB')
),

-- Canonicalize names from agency_higher_ed_prime_totals
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
        REGEXP_REPLACE(
          UPPER(TRIM(p.prime_vendor)), 
          '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', 
          '', 
          'gi'
        ),
        '[^A-Z0-9 ]', '', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS canon_prime_vendor,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          UPPER(TRIM(p.agency_name)), 
          '\s+(LLC|INC\.?|LTD\.?|CO\.?|CORP\.?|COMPANY|INCORPORATED|CORPORATION)(\s|$)', 
          '', 
          'gi'
        ),
        '[^A-Z0-9 ]', '', 'g'
      ),
      '\s+', ' ', 'g'
    )) AS canon_agency_name
  FROM ppt.agency_higher_ed_prime_totals p
),

-- Step 1: Match by tax_id
subs_matched_by_tax AS (
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
    p.canon_prime_vendor,
    p.canon_agency_name,
    s.b2g_id,
    s.sub_business_name,
    s.vendor_type,
    s.tax_id,
    s.sub_certification_type,
    s.matched_tax_id,
    s.matched_b2g_id,
    s.sub_match_method,
    s.audit_period,
    s.sub_amount,
    'tax_id' AS match_method
  FROM primes_canon p
  INNER JOIN contract_subs s
    ON p.prime_matched_tax_id IS NOT NULL
    AND s.tax_id IS NOT NULL
    AND p.prime_matched_tax_id::text = s.tax_id::text
    AND p.canon_agency_name = s.canon_agency_name
),

-- Step 2: Get primes that didn't match by tax_id
primes_unmatched_by_tax AS (
  SELECT p.*
  FROM primes_canon p
  LEFT JOIN subs_matched_by_tax stax
    ON p.source = stax.source
    AND p.agency_number = stax.agency_number
    AND p.agency_name = stax.agency_name
    AND p.prime_vendor = stax.prime_vendor
  WHERE stax.source IS NULL
),

-- Step 3: Match those by name
subs_matched_by_name AS (
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
    p.canon_prime_vendor,
    p.canon_agency_name,
    s.b2g_id,
    s.sub_business_name,
    s.vendor_type,
    s.tax_id,
    s.sub_certification_type,
    s.matched_tax_id,
    s.matched_b2g_id,
    s.sub_match_method,
    s.audit_period,
    s.sub_amount,
    'name' AS match_method
  FROM primes_unmatched_by_tax p
  INNER JOIN contract_subs s
    ON p.canon_prime_vendor = s.canon_prime_vendor
    AND p.canon_agency_name = s.canon_agency_name
),

-- Combine all matched subs
all_matched_subs AS (
  SELECT * FROM subs_matched_by_tax
  UNION ALL
  SELECT * FROM subs_matched_by_name
),

-- Calculate sub totals for each prime (only counting qualifying certifications)
sub_totals AS (
  SELECT
    source,
    agency_number,
    agency_name,
    prime_vendor,
    -- Only count MBE, WBE, MWBE, SEDBE, CBE as certified
    SUM(CASE WHEN UPPER(TRIM(sub_certification_type)) IN ('MBE', 'WBE', 'MWBE', 'SEDBE', 'CBE')
             THEN COALESCE(sub_amount, 0) 
             ELSE 0 END) AS certified_subs_total,
    -- Uncertified = NULL cert OR non-qualifying cert types
    SUM(CASE WHEN sub_certification_type IS NULL 
              OR UPPER(TRIM(sub_certification_type)) NOT IN ('MBE', 'WBE', 'MWBE', 'SEDBE', 'CBE')
             THEN COALESCE(sub_amount, 0) 
             ELSE 0 END) AS uncertified_subs_total
  FROM all_matched_subs
  GROUP BY source, agency_number, agency_name, prime_vendor
)

-- PRIME ROWS with certified spend calculation
SELECT 
  p.source,
  p.agency_number,
  p.agency_name,
  p.prime_vendor,
  p.prime_vendor AS business_name,
  'PRIME' AS vendor_type,
  p.prime_tax_id,
  p.prime_certification_type,
  p.prime_matched_tax_id,
  p.prime_match_method,
  p.prime_total_amount,
  
  -- Certified Spend Calculation (only MBE, WBE, MWBE, SEDBE, CBE)
  CASE
    -- Prime has qualifying certification
    WHEN UPPER(TRIM(p.prime_certification_type)) IN ('MBE', 'WBE', 'MWBE', 'SEDBE', 'CBE') THEN
      CASE
        -- Has uncertified subs: subtract them from prime total
        WHEN st.uncertified_subs_total > 0 
        THEN p.prime_total_amount - st.uncertified_subs_total
        -- No uncertified subs: certified spend = prime total
        ELSE p.prime_total_amount
      END
    -- Prime does NOT have qualifying certification
    ELSE
      -- Certified spend = only certified subs with qualifying certs
      COALESCE(st.certified_subs_total, 0)
  END AS certified_spend,
  
  -- Sub columns NULL for prime row
  NULL::text AS sub_b2g_id,
  NULL::text AS sub_tax_id,
  NULL::text AS sub_certification_type,
  NULL::text AS sub_matched_tax_id,
  NULL::text AS sub_matched_b2g_id,
  NULL::text AS sub_match_method,
  NULL::timestamp AS sub_audit_period,
  NULL::numeric AS sub_amount,
  NULL::text AS match_method
  
FROM primes_canon p
LEFT JOIN sub_totals st
  ON p.source = st.source
  AND p.agency_number = st.agency_number
  AND p.agency_name = st.agency_name
  AND p.prime_vendor = st.prime_vendor

UNION ALL

-- SUB ROWS
SELECT 
  source,
  agency_number,
  agency_name,
  prime_vendor,
  sub_business_name AS business_name,
  'SUBCONTRACTOR' AS vendor_type,
  prime_tax_id,
  prime_certification_type,
  prime_matched_tax_id,
  prime_match_method,
  NULL::numeric AS prime_total_amount,
  NULL::numeric AS certified_spend,
  
  -- Sub details
  b2g_id::text AS sub_b2g_id,
  tax_id::text AS sub_tax_id,
  sub_certification_type,
  matched_tax_id::text AS sub_matched_tax_id,
  matched_b2g_id::text AS sub_matched_b2g_id,
  sub_match_method,
  audit_period AS sub_audit_period,
  sub_amount,
  match_method  -- Shows 'tax_id' or 'name'
  
FROM all_matched_subs;

-- Indexes
CREATE INDEX IF NOT EXISTS ix_prime_subs_agency_prime
  ON ppt.agency_higher_ed_prime_with_subs (agency_name, prime_vendor);
CREATE INDEX IF NOT EXISTS ix_prime_subs_source
  ON ppt.agency_higher_ed_prime_with_subs (source);
CREATE INDEX IF NOT EXISTS ix_prime_subs_vendor_type
  ON ppt.agency_higher_ed_prime_with_subs (vendor_type);