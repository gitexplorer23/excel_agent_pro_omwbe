SELECT
  COUNT(*) FILTER (WHERE match_method = 'tax_id') AS matched_by_tax_id,
  COUNT(*) FILTER (WHERE match_method = 'name') AS matched_by_name,
  COUNT(*) FILTER (WHERE match_method IS NULL) AS no_match,
  COUNT(*) AS total_in_agency_higher_ed,
  (SELECT COUNT(*) FROM ppt.contract_prime_cert_spend_summary) AS total_in_contract_summary
FROM ppt.agency_higher_ed_prime_cert_summary;