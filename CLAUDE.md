# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Excel-to-PostgreSQL data integration system for the Washington State Department of Transportation's Office of Women and Minority Business Enterprise (OMWBE). The main script (`excel_postgres_integration_V4.1_clean.py`) provides automated synchronization between Excel worksheets and PostgreSQL databases with intelligent change detection.

## Main Script: excel_postgres_integration_V4.1_clean.py

### Core Functionality
- Reads Excel files and syncs data to PostgreSQL
- Detects changes using MD5 row hashing
- Only updates records that have actually changed
- Maintains audit trail with timestamps
- Handles multiple worksheets in a single run
- Auto-creates tables and schemas if needed

### Key Technical Details

#### Column Normalization
All Excel column names are automatically normalized:
```python
# Spaces → underscores, uppercase → lowercase
"Business Name" → "business_name"
"B2GNow Vendor Number" → "b2gnow_vendor_number"
```

#### Conflict Keys Configuration
Each Excel sheet requires unique identifier columns defined in `CONFLICT_KEYS`:
```python
CONFLICT_KEYS = {
    "vendor_search_results": ["uniqueid", "b2gnow_vendor_number"],
    "another_sheet": ["id_column", "secondary_key"]
}
```

#### Change Detection Algorithm
1. Canonicalizes all values (nulls, dates, floats) for consistent hashing
2. Creates MD5 hash of row data (excluding audit columns)
3. Only updates rows where hash differs from database
4. Deletes rows no longer present in Excel source

## Environment Setup

### Required Dependencies
```bash
pip install pandas sqlalchemy psycopg2-binary python-dotenv openpyxl
```

### Environment Variables (.env)
```env
# Excel source
EXCEL_FILE_PATH=C:\path\to\file.xlsx
SHEET_NAMES=vendor_search_results,other_sheet

# Database connection
DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/database
SCHEMA_NAME=public  # Optional, defaults to 'public'
```

## Common Development Tasks

### Running the Integration
```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Run integration
python excel_postgres_integration_V4.1_clean.py
```

### Adding a New Excel Sheet
1. Add the sheet name to `SHEET_NAMES` in .env
2. Define unique keys in `CONFLICT_KEYS` dictionary
3. Run the script - table will be auto-created

### Testing Changes
```bash
# Check what will be processed without making changes
# Add logging to see what would be upserted/deleted
python excel_postgres_integration_V4.1_clean.py
```

### Debugging Issues
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Common issues:
# - Missing conflict keys: Add sheet to CONFLICT_KEYS
# - Column name mismatch: Check normalized names (lowercase, underscores)
# - Connection errors: Verify DATABASE_URL and network access
```

## Database Schema Pattern

### Auto-Created Table Structure
```sql
-- Original Excel columns (normalized names)
business_name TEXT,
b2gnow_vendor_number INTEGER,
...

-- Audit columns (auto-added)
created_at TIMESTAMPTZ DEFAULT now(),
updated_at TIMESTAMPTZ,
row_hash TEXT,

-- Unique constraint on conflict keys
CONSTRAINT uq_vendor_search_results_uniqueid_b2gnow_vendor_number 
  UNIQUE (uniqueid, b2gnow_vendor_number)
```

## Project Structure

```
/
├── excel_postgres_integration_V4.1_clean.py  # Main integration script
├── .env                                       # Environment configuration
├── .env.example                               # Template for environment vars
├── requirements.txt                           # Python dependencies
├── OWMBE/materialized tables/                # Business intelligence views
│   ├── materialized_table_refresh_all.py     # Refresh all views
│   └── *.py                                   # Individual view definitions
├── update_scripts/                            # Data maintenance utilities
├── executables/                               # Windows batch automation
└── other/                                     # Utilities and helpers
```

## Critical Implementation Notes

1. **Conflict Keys**: Must match normalized column names exactly (lowercase, underscores)
2. **Row Hash**: Excludes audit columns (created_at, updated_at, row_hash) from calculation
3. **Null Handling**: All null-like values canonicalized to empty string for consistent hashing
4. **Date Formatting**: Dates → YYYY-MM-DD, Datetimes → YYYY-MM-DDTHH:MM:SS
5. **Float Precision**: Uses .10g formatting for stable hash generation
6. **Transaction Safety**: All operations in transactions - either all succeed or all rollback

## Troubleshooting Guide

### "No conflict keys defined"
Add the sheet name to CONFLICT_KEYS dictionary with appropriate unique columns

### "Dropped X rows due to missing conflict keys"  
Some rows have NULL or empty values in the key columns - review data quality

### "Duplicate row_hash values"
Indicates actual duplicate data in Excel - review source data

### Connection Issues
1. Verify DATABASE_URL format: `postgresql+psycopg2://user:pass@host:port/db`
2. Check PostgreSQL is running and accessible
3. Verify user has CREATE TABLE and INSERT/UPDATE/DELETE permissions

## Performance Optimization

- Script processes ~30,000 rows in ~25 seconds
- Uses bulk upserts for efficiency
- Only updates changed records (hash comparison)
- Maintains indexes on conflict keys for fast lookups

## Security Considerations

- Never commit .env file with credentials
- Use environment variables for all sensitive data
- Database user should have minimum required permissions
- Consider using connection pooling for production