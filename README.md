# Excel to PostgreSQL Integration System - OMWBE

## Overview

A robust data integration solution for the Washington State Department of Transportation's Office of Women and Minority Business Enterprise (OMWBE). This system automatically synchronizes Excel worksheets with PostgreSQL databases, providing intelligent change detection, data validation, and comprehensive audit trails.

## Key Features

- **Intelligent Synchronization**: Only updates records that have actually changed using MD5 hash comparison
- **Multi-Sheet Support**: Process multiple Excel worksheets in a single run
- **Automatic Table Creation**: Creates database tables and schemas automatically if they don't exist
- **Change Detection**: Row-level change tracking with hash-based comparison
- **Data Cleanup**: Removes stale records no longer present in Excel source
- **Audit Trail**: Maintains created_at and updated_at timestamps for all records
- **Column Normalization**: Automatically converts Excel column names to database-friendly format
- **Type Detection**: Intelligently detects and converts data types

## Quick Start

### Prerequisites

- Python 3.7 or higher
- PostgreSQL database
- Excel files (.xlsx, .xlsm format)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/wsdot/excel_agent_omwbe.git
cd excel_agent_omwbe
```

2. Create virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Configuration

Create a `.env` file with the following variables:

```env
# Excel source configuration
EXCEL_FILE_PATH=C:\path\to\your\excel_file.xlsx
SHEET_NAMES=vendor_search_results,other_sheet

# Database connection
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/database_name
SCHEMA_NAME=public  # Optional, defaults to 'public'
```

### Define Conflict Keys

Edit the `CONFLICT_KEYS` dictionary in `excel_postgres_integration_V4.1_clean.py`:

```python
CONFLICT_KEYS = {
    "vendor_search_results": ["uniqueid", "b2gnow_vendor_number"],
    "your_sheet_name": ["primary_key_column", "secondary_key_column"]
}
```

**Important**: Keys must match the normalized column names (lowercase, underscores instead of spaces)

### Run the Integration

```bash
python excel_postgres_integration_V4.1_clean.py
```

## How It Works

1. **Read Excel Data**: Loads specified sheets from the Excel file
2. **Normalize Columns**: Converts column names to lowercase with underscores
3. **Validate Data**: Checks for required conflict keys and removes invalid rows
4. **Calculate Hashes**: Creates MD5 hash of each row for change detection
5. **Create/Update Tables**: Automatically creates tables if needed, adds audit columns
6. **Perform Upserts**: Inserts new records, updates changed records only
7. **Clean Stale Data**: Removes records no longer present in Excel source
8. **Log Results**: Provides detailed logging of all operations

## Example Output

```
2025-08-15 10:28:05 INFO Configuration:
  Excel file: C:\Users\osmelc\Documents\HQ_OMWBE_local_macro.xlsm
  Sheets: ['vendor_search_results']
  Schema: ppt
2025-08-15 10:28:05 INFO ✓ Database connection successful
2025-08-15 10:28:05 INFO ▶ Processing sheet 'vendor_search_results'
2025-08-15 10:28:07 INFO ✓ Created table 'ppt.vendor_search_results'
2025-08-15 10:28:31 INFO Upserted 31170 rows (only changed data)
2025-08-15 10:28:31 INFO 🗑️ Deleted 0 stale rows
2025-08-15 10:28:31 INFO ✔ Finished 'vendor_search_results' in 25.81s
2025-08-15 10:28:31 INFO ✅ All processing complete
```

## Database Schema

The script automatically creates tables with the following structure:

```sql
-- Original Excel columns (automatically detected types)
business_name TEXT,
b2gnow_vendor_number INTEGER,
certification_date TIMESTAMP,
...

-- Audit columns (automatically added)
created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
updated_at TIMESTAMPTZ,
row_hash TEXT,

-- Unique constraint based on conflict keys
CONSTRAINT uq_tablename_key1_key2 UNIQUE (key1, key2)
```

## Advanced Usage

### Processing Multiple Sheets

Add multiple sheet names to the `SHEET_NAMES` environment variable:
```env
SHEET_NAMES=sheet1,sheet2,sheet3
```

### Custom Schema

Specify a custom database schema:
```env
SCHEMA_NAME=my_custom_schema
```

### Batch Processing

For Windows users, use the provided batch files:
```batch
executables\main_tables_refresh_process.bat
```

### Materialized Views

Refresh business intelligence views:
```bash
python OWMBE/materialized_tables/materialized_table_refresh_all.py
```

## Troubleshooting

### Common Issues

1. **"No conflict keys defined"**
   - Add your sheet name to the `CONFLICT_KEYS` dictionary
   - Ensure keys match normalized column names

2. **"Dropped X rows due to missing conflict keys"**
   - Some rows have NULL values in key columns
   - Review data quality in Excel source

3. **Connection errors**
   - Verify DATABASE_URL format
   - Check PostgreSQL is running
   - Ensure database user has required permissions

4. **Column name issues**
   - Remember: "Business Name" becomes "business_name"
   - Special characters are removed
   - Multiple spaces become single underscore

## Project Structure

```
excel_agent_omwbe/
├── excel_postgres_integration_V4.1_clean.py  # Main integration script
├── .env                                       # Configuration (git-ignored)
├── .env.example                               # Configuration template
├── requirements.txt                           # Python dependencies
├── README.md                                  # This file
├── CLAUDE.md                                  # AI assistant documentation
├── OWMBE/
│   └── materialized_tables/                  # Business intelligence views
├── update_scripts/                            # Update and maintenance scripts
├── executables/                               # Windows batch automation
└── other/                                     # Utility scripts
```

## Performance

- Processes ~30,000 rows in approximately 25 seconds
- Only updates records that have actually changed
- Uses bulk operations for efficiency
- Maintains database indexes for fast lookups

## Security

- Store credentials in `.env` file (never commit to git)
- Use environment variables for all sensitive data
- Grant minimum required database permissions
- Regular audit of access logs recommended

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues or questions:
- Check the troubleshooting section
- Review CLAUDE.md for technical details
- Contact OMWBE IT support

## License

Property of Washington State Department of Transportation - OMWBE Division

## Acknowledgments

Developed for the Office of Women and Minority Business Enterprise to streamline vendor data management and improve operational efficiency.

---

**Version**: 4.1 Clean  
**Last Updated**: 2025  
**Maintainer**: WSDOT OMWBE IT Team