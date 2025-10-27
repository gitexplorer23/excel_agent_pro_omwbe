DROP TABLE IF EXISTS ppt.agency_higher_ed_prime_with_subs;

CREATE TABLE ppt.agency_higher_ed_prime_with_subs AS
WITH
-- Get all subs from contracts with canonicalized names
contract_subs AS (
  SELECT
    c.agency_name,
    c.prime_vendor,
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
    c.tax_id,
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
)

-- PRIME ROWS (one row per prime from agency_higher_ed_prime_totals)
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
  
  -- Sub columns NULL for prime row
  NULL::text AS sub_b2g_id,
  NULL::text AS sub_tax_id,
  NULL::text AS sub_certification_type,
  NULL::text AS sub_matched_tax_id,
  NULL::text AS sub_matched_b2g_id,
  NULL::text AS sub_match_method,
  NULL::timestamp AS sub_audit_period,
  NULL::numeric AS sub_amount
  
FROM primes_canon p

UNION ALL

-- SUB ROWS - Match on BOTH prime_vendor AND agency_name
SELECT 
  p.source,
  p.agency_number,
  p.agency_name,
  p.prime_vendor,
  s.sub_business_name AS business_name,
  'SUBCONTRACTOR' AS vendor_type,
  p.prime_tax_id,
  p.prime_certification_type,
  p.prime_matched_tax_id,
  p.prime_match_method,
  NULL::numeric AS prime_total_amount,  -- Subs don't carry prime total
  
  -- Sub details
  s.b2g_id::text AS sub_b2g_id,
  s.tax_id::text AS sub_tax_id,
  s.sub_certification_type,
  s.matched_tax_id::text AS sub_matched_tax_id,
  s.matched_b2g_id::text AS sub_matched_b2g_id,
  s.sub_match_method,
  s.audit_period AS sub_audit_period,
  s.sub_amount
  
FROM primes_canon p
INNER JOIN contract_subs s
  ON p.canon_prime_vendor = s.canon_prime_vendor
  AND p.canon_agency_name = s.canon_agency_name;

-- Indexes
CREATE INDEX IF NOT EXISTS ix_prime_subs_agency_prime
  ON ppt.agency_higher_ed_prime_with_subs (agency_name, prime_vendor);
CREATE INDEX IF NOT EXISTS ix_prime_subs_source
  ON ppt.agency_higher_ed_prime_with_subs (source);
CREATE INDEX IF NOT EXISTS ix_prime_subs_vendor_type
  ON ppt.agency_higher_ed_prime_with_subs (vendor_type);