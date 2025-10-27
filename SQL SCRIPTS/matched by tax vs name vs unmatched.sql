SELECT match_method, COUNT(*) AS rows
FROM ppt.agency_higher_ed_with_cert
GROUP BY match_method
ORDER BY match_method;
