--CUrrent script

DROP TABLE IF EXISTS ppt.agency_higher_ed_with_cert;

CREATE TABLE ppt.agency_higher_ed_with_cert AS
WITH
vendor_dim AS (
  SELECT *
  FROM (
    SELECT
      NULLIF(TRIM(v.tax_id::text), '') AS tax_id,  -- vendor tax id as text (may be NULL for some VOB)

      -- Canonical business name
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(UPPER(TRIM(v.business_name)), '[^A-Z0-9 ]', '', 'g'),
          '\s+', ' ', 'g'
        ),
        '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
      ) AS canon_name,

      v.certification_type,
      v.certification_status,

      ROW_NUMBER() OVER (
        PARTITION BY
          COALESCE(NULLIF(TRIM(v.tax_id::text), ''), 'NO_TAX'),
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(UPPER(TRIM(v.business_name)), '[^A-Z0-9 ]', '', 'g'),
              '\s+', ' ', 'g'
            ),
            '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
          )
        ORDER BY
          CASE WHEN v.certification_status = 'Active' THEN 0 ELSE 1 END,
          /* priority: MBE(1) < MWBE(2) < WBE(3) < CBE(4) < SEDBE(5) < VOB(6) */
          CASE v.certification_type
            WHEN 'MBE' THEN 1
            WHEN 'MWBE' THEN 2
            WHEN 'WBE' THEN 3
            WHEN 'CBE' THEN 4
            WHEN 'SEDBE' THEN 5
            WHEN 'VOB' THEN 6
            ELSE 999
          END
      ) AS rn
    FROM ppt.vendor_search_results_taxid v
  ) s
  WHERE rn = 1
    AND certification_status = 'Active'
),

higher_ed_norm AS (
  SELECT
    he.*,
    ROW_NUMBER() OVER () AS he_row_id,
    NULLIF(TRIM(he.tax_id::text), '') AS tax_id_norm,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(TRIM(he.business_name)), '[^A-Z0-9 ]', '', 'g'),
        '\s+', ' ', 'g'
      ),
      '\b(LLC|INC|LTD|CO|CORP|COMPANY)\b', '', 'g'
    ) AS canon_name
  FROM ppt.agency_higher_ed_combine he
),

tax_matches AS (
  SELECT
    h.he_row_id,
    v.certification_type,
    v.tax_id AS matched_tax_id,
    'tax_id'::text AS match_method
  FROM higher_ed_norm h
  JOIN vendor_dim v
    ON h.tax_id_norm IS NOT NULL
   AND v.tax_id = h.tax_id_norm
),

name_matches AS (
  SELECT
    h.he_row_id,
    v.certification_type,
    v.tax_id AS matched_tax_id,
    'name'::text AS match_method
  FROM higher_ed_norm h
  LEFT JOIN tax_matches t
    ON t.he_row_id = h.he_row_id
  JOIN vendor_dim v
    ON t.he_row_id IS NULL
   AND h.canon_name = v.canon_name
),

combined_match AS (
  SELECT * FROM tax_matches
  UNION ALL
  SELECT * FROM name_matches
)

SELECT
  h.agency_number,
  h.agency_name,
  h.business_name,
  h.tax_id,                 -- original HE/agency tax id
  h.total_amount,
  cm.certification_type,    -- chosen via MBE > MWBE > WBE > CBE > SEDBE > VOB
  cm.matched_tax_id,        -- vendor-side tax id (NULL possible)
  COALESCE(cm.match_method, 'unmatched') AS match_method
FROM higher_ed_norm h
LEFT JOIN combined_match cm
  ON cm.he_row_id = h.he_row_id;
