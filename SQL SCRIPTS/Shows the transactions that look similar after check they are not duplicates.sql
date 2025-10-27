-- rows are duplicates if canon(business_name) and total_amount appear more than once
SELECT *
FROM (
  SELECT
    a.*,
    -- canonicalize name: UPPER, strip non-alnum, collapse spaces, drop common suffixes
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(a.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_name,
    COUNT(*) OVER (
      PARTITION BY
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(UPPER(TRIM(a.business_name)), '[^A-Z0-9 ]', '', 'g'),
            '\s+', ' ', 'g'
          ),
          '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
        ),
        a.total_amount
    ) AS dupe_count
  FROM ppt.agency_higher_ed_with_cert a
) x
WHERE dupe_count > 1
ORDER BY canon_name, total_amount, business_name;
