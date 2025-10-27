-- if tables are large, these help future joins/filters
CREATE INDEX IF NOT EXISTS ix_he_with_cert_match_method ON ppt.agency_higher_ed_with_cert(match_method);
CREATE INDEX IF NOT EXISTS ix_he_with_cert_vendor_tax ON ppt.agency_higher_ed_with_cert(matched_tax_id);
CREATE INDEX IF NOT EXISTS ix_he_with_cert_he_tax ON ppt.agency_higher_ed_with_cert(tax_id);
