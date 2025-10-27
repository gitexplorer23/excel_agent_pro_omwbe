DROP TABLE IF EXISTS ppt.agency_higher_ed_with_cert_flags;

CREATE TABLE ppt.agency_higher_ed_with_cert_flags AS
WITH
-- 1) Normalize contract-side vendor names & types
contracts_norm AS (
  SELECT
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(c.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    )                                         AS cn_contract_business_name,
    NULLIF(TRIM(c.taxidnumber::text), '')     AS contract_tax_id,
    UPPER(TRIM(c.vendor_type))                AS vendor_type_u,
    UPPER(TRIM(c.payment_type))               AS payment_type_u
  FROM ppt.contract_powerbi c
  WHERE c.business_name IS NOT NULL
),

-- 2) Flags aggregated by NAME
contract_flags_by_name AS (
  SELECT
    cn_contract_business_name,
    TRUE AS in_contracts_business_name,
    (COUNT(*) FILTER (
       WHERE vendor_type_u IN ('PRIME','PRIME CONTRACTOR','PRIME VENDOR','CONTRACTOR')
          OR payment_type_u = 'PRIME'
     ) > 0)                                   AS has_vendor_type_prime,
    (COUNT(*) FILTER (
       WHERE vendor_type_u IN ('SUBCONTRACTOR','SUB')
          OR payment_type_u IN ('SUBCONTRACTOR','SUB')
     ) > 0)                                   AS has_vendor_type_sub
  FROM contracts_norm
  GROUP BY cn_contract_business_name
),

-- 3) Flags aggregated by TAX ID (for first-pass tax-id matching)
contract_flags_by_taxid AS (
  SELECT
    contract_tax_id,
    TRUE AS in_contracts_business_name,
    (COUNT(*) FILTER (
       WHERE vendor_type_u IN ('PRIME','PRIME CONTRACTOR','PRIME VENDOR','CONTRACTOR')
          OR payment_type_u = 'PRIME'
     ) > 0)                                   AS has_vendor_type_prime,
    (COUNT(*) FILTER (
       WHERE vendor_type_u IN ('SUBCONTRACTOR','SUB')
          OR payment_type_u IN ('SUBCONTRACTOR','SUB')
     ) > 0)                                   AS has_vendor_type_sub
  FROM contracts_norm
  WHERE contract_tax_id IS NOT NULL
  GROUP BY contract_tax_id
),

-- 4) Normalize names on the HE/Agency table (keep source exactly as-is)
ah_norm AS (
  SELECT
    h.source,
    h.agency_number,
    h.agency_name,
    h.business_name,
    h.tax_id,
    h.total_amount,
    h.certification_type,
    h.matched_tax_id,
    h.match_method,
    ROW_NUMBER() OVER ()                      AS he_row_id,
    NULLIF(TRIM(h.tax_id::text), '')          AS he_tax_id_txt,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(h.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    )                                         AS cn_he_business_name
  FROM ppt.agency_higher_ed_with_cert h
),

-- 5) First: match to contracts by TAX ID
tax_matches AS (
  SELECT
    a.he_row_id,
    t.in_contracts_business_name,
    t.has_vendor_type_prime,
    t.has_vendor_type_sub,
    t.contract_tax_id                    AS matched_taxid_to_contracts,
    'tax_id'::text                       AS match_method_contract
  FROM ah_norm a
  JOIN contract_flags_by_taxid t
    ON a.he_tax_id_txt IS NOT NULL
   AND a.he_tax_id_txt = t.contract_tax_id
),

-- 6) Then: for rows NOT matched by tax, match by NAME
name_matches AS (
  SELECT
    a.he_row_id,
    n.in_contracts_business_name,
    n.has_vendor_type_prime,
    n.has_vendor_type_sub,
    NULL::text                           AS matched_taxid_to_contracts,
    'name'::text                         AS match_method_contract
  FROM ah_norm a
  LEFT JOIN tax_matches tm
    ON tm.he_row_id = a.he_row_id
  JOIN contract_flags_by_name n
    ON tm.he_row_id IS NULL
   AND a.cn_he_business_name = n.cn_contract_business_name
),

-- 7) Prefer tax matches, else name matches
combined_contract_match AS (
  SELECT * FROM tax_matches
  UNION ALL
  SELECT * FROM name_matches
)

-- 8) Final: keep ALL rows; add flags + contract match info
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
  COALESCE(c.in_contracts_business_name, FALSE)  AS in_contracts_business_name,
  COALESCE(c.has_vendor_type_prime, FALSE)        AS has_vendor_type_prime,
  COALESCE(c.has_vendor_type_sub, FALSE)          AS has_vendor_type_sub,
  c.matched_taxid_to_contracts,
  COALESCE(c.match_method_contract, 'unmatched')  AS match_method_contract
FROM ah_norm a
LEFT JOIN combined_contract_match c
  ON c.he_row_id = a.he_row_id;
