

-- any duplicates? (should be zero)
SELECT agency_number, agency_name, business_name, tax_id, COUNT(*)
FROM ppt.agency_higher_ed_with_cert
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1;


--quick sanity & coverage

-- 1) how many rows matched by what method?
SELECT match_method, COUNT(*) AS rows
FROM ppt.agency_higher_ed_with_cert
GROUP BY match_method
ORDER BY match_method;

-- 2) unmatched vendors to review (sample 50)
SELECT agency_number, agency_name, business_name, tax_id, total_amount
FROM ppt.agency_higher_ed_with_cert
WHERE match_method = 'unmatched'
ORDER BY total_amount DESC NULLS LAST
LIMIT 50;

-- 3) coverage rate
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN match_method <> 'unmatched' THEN 1 ELSE 0 END) AS matched_rows,
  ROUND(100.0 * SUM(CASE WHEN match_method <> 'unmatched' THEN 1 ELSE 0 END) / COUNT(*), 2) AS matched_pct
FROM ppt.agency_higher_ed_with_cert;

--tax id consistency (HE vs vendor)

-- 4) rows where both sides have a tax id but they differ Best one shows why tax id did not match but name did
SELECT
  agency_number, agency_name, business_name,
  tax_id AS he_tax_id,
  matched_tax_id AS vendor_tax_id,
  certification_type,
  match_method,
  total_amount
FROM ppt.agency_higher_ed_with_cert
WHERE match_method IN ('tax_id','name')
  AND NULLIF(TRIM(tax_id::text), '') IS NOT NULL
  AND NULLIF(TRIM(matched_tax_id), '') IS NOT NULL
  AND TRIM(tax_id::text) <> TRIM(matched_tax_id)
ORDER BY total_amount DESC NULLS LAST
LIMIT 200;

-- 5) count of tax-id matches where HE tax_id was missing (should be 0)
SELECT COUNT(*) AS tax_match_but_he_tax_missing
FROM ppt.agency_higher_ed_with_cert
WHERE match_method = 'tax_id'
  AND NULLIF(TRIM(tax_id::text), '') IS NULL;

-- 6) name matches where HE *does* have a tax id (these deserve a look)
SELECT
  agency_number, agency_name, business_name, tax_id, matched_tax_id, certification_type, total_amount
FROM ppt.agency_higher_ed_with_cert
WHERE match_method = 'name'
  AND NULLIF(TRIM(tax_id::text), '') IS NOT NULL
ORDER BY total_amount DESC NULLS LAST
LIMIT 200;

--certification sanity
-- 7) certification distribution
SELECT certification_type, COUNT(*) AS rows
FROM ppt.agency_higher_ed_with_cert
GROUP BY certification_type
ORDER BY certification_type;

-- 8) name matches that yielded NULL vendor tax_id (expected for some VOBs)
SELECT
  agency_number, agency_name, business_name, tax_id, matched_tax_id, certification_type
FROM ppt.agency_higher_ed_with_cert
WHERE match_method = 'name' AND matched_tax_id IS NULL
LIMIT 200;

--optional: top unmatched & top mismatched vendors
-- 11) top 50 unmatched by spend
SELECT business_name, tax_id, SUM(total_amount) AS total_unmatched_amount, COUNT(*) AS rows
FROM ppt.agency_higher_ed_with_cert
WHERE match_method = 'unmatched'
GROUP BY business_name, tax_id
ORDER BY total_unmatched_amount DESC NULLS LAST
LIMIT 50;

-- 12) top 50 with tax-id disagreements by spend
WITH tax_disagreements AS (
  SELECT business_name, tax_id::text AS he_tax_id, matched_tax_id,
         total_amount
  FROM ppt.agency_higher_ed_with_cert
  WHERE match_method IN ('tax_id','name')
    AND NULLIF(TRIM(tax_id::text), '') IS NOT NULL
    AND NULLIF(TRIM(matched_tax_id), '') IS NOT NULL
    AND TRIM(tax_id::text) <> TRIM(matched_tax_id)
)
SELECT business_name, he_tax_id, matched_tax_id,
       SUM(total_amount) AS amount_at_issue, COUNT(*) AS rows
FROM tax_disagreements
GROUP BY business_name, he_tax_id, matched_tax_id
ORDER BY amount_at_issue DESC NULLS LAST
LIMIT 50;


