#!/usr/bin/env python3
"""
Excel to PostgreSQL Data Integration Script (V4.1 Clean)

OVERVIEW:
This script provides a comprehensive solution for processing Excel worksheets and synchronizing 
their data with a PostgreSQL database. It handles data type detection, change tracking, 
and intelligent upsert operations to maintain data consistency between Excel files and database tables.

KEY FEATURES:
- Automated Excel sheet processing with multiple worksheet support
- Column name normalization (converts spaces to underscores, lowercases)
- Intelligent data type detection and conversion
- Row-level change detection using MD5 hashing
- Upsert operations (insert new, update changed records only)
- Stale data cleanup (removes rows no longer present in source)
- Audit trail with created_at/updated_at timestamps
- Conflict resolution using configurable unique keys per sheet
- Schema and table auto-creation if they don't exist

HOW IT WORKS:
1. Reads specified Excel sheets from the configured file
2. Normalizes column names and detects appropriate data types
3. Calculates a row hash for each record to detect changes
4. Creates PostgreSQL tables with proper constraints if they don't exist
5. Performs intelligent upserts - only updates rows where data has actually changed
6. Removes stale records that are no longer present in the Excel source
7. Maintains audit timestamps for tracking data lineage

CONFIGURATION REQUIREMENTS:
1. CONFLICT_KEYS Dictionary:
   - Define unique key combinations for each sheet (must match normalized column names)
   - These keys are used to identify existing records for upsert operations
   - Example: {"sheet_name": ["client_id", "date_of_service"]}

2. Environment Variables (.env file):
   - EXCEL_FILE_PATH: Full path to the Excel file to process
   - SHEET_NAMES: Comma-separated list of worksheet names to process
   - DATABASE_URL: PostgreSQL connection string (postgresql://user:pass@host:port/db)
   - SCHEMA_NAME: (Optional) Database schema name, defaults to 'public'
   - DB_NAME: Target database name

EXAMPLE USAGE:
This script is designed for scenarios where:
- Regular Excel reports need to be synchronized with a database
- Data integrity and change tracking are important
- Multiple worksheets contain related but distinct data sets
- You need to maintain historical audit trails
- Source data may have additions, updates, and deletions

The script ensures that your PostgreSQL database stays in sync with your Excel data source
while preserving referential integrity and providing comprehensive logging.

AUTHOR: Washington State Department of Transportation - OMWBE
VERSION: 4.1 Clean
DATE: 2025
"""
import os
import sys
import logging
import hashlib
import time
from datetime import datetime, date
from dotenv import load_dotenv

import pandas as pd
import sqlalchemy
from sqlalchemy import text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Define conflict keys per sheet (must match normalized column names)
CONFLICT_KEYS = {
    "vendor_search_results": ["uniqueid", "b2gnow_vendor_number"],
    "wa_geographical_district": ["unique_id","zipcode"],
    "afers_ofm": ["index"]
}

def canonicalize(value):
    """Canonicalize values for consistent hashing across runs."""
    # Handle null values
    if value is None or (hasattr(pd, 'isna') and pd.isna(value)):
        return ""
    
    # Handle dates
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    
    # Handle floats with consistent precision
    if isinstance(value, float):
        if pd.isna(value):
            return ""
        return f"{value:.10g}"
    
    # Everything else as string
    return str(value).strip()

def process_sheet(engine, sheet_name, df, schema_name):
    """Process a single Excel sheet and sync with database."""
    start_time = time.time()
    logger.info(f"▶ Processing sheet '{sheet_name}'")
    
    # 1) Normalize column names (spaces to underscores, lowercase)
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(' ', '_', regex=False)
        .str.lower()
    )
    
    # 2) Log detected data types
    logger.info(f"Detected dtypes for '{sheet_name}':\n{df.dtypes}")
    
    # 3) Convert to best possible dtypes
    df = df.convert_dtypes()
    
    # 4) Get conflict keys for this sheet
    keys = CONFLICT_KEYS.get(sheet_name)
    if not keys:
        logger.error(f"No conflict keys defined for sheet '{sheet_name}'")
        logger.info(f"Add this sheet to CONFLICT_KEYS dictionary with appropriate unique columns")
        sys.exit(1)
    
    # 5) Clean and validate conflict key columns
    for key in keys:
        if key in df.columns:
            # Convert to string and strip whitespace
            df[key] = df[key].astype("string").str.strip()
    
    # Find rows that would be dropped
    before = len(df)
    mask_na = df[keys].isna().any(axis=1)
    mask_empty = pd.DataFrame(False, index=df.index, columns=['empty'])
    for key in keys:
        mask_empty['empty'] |= (df[key] == "")
    
    rows_to_drop = mask_na | mask_empty['empty']
    
    # If rows will be dropped, save them to CSV for inspection
    if rows_to_drop.any():
        num_dropped = rows_to_drop.sum()
        logger.warning(f"Found {num_dropped} rows with missing/empty conflict keys: {keys}")
        
        # Save dropped rows to CSV for analysis (only first 100 to avoid huge files)
        dropped_df = df[rows_to_drop].head(100).copy()
        dropped_file = f"dropped_rows_{sheet_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        dropped_df.to_csv(dropped_file, index=False)
        logger.info(f"Saved first 100 dropped rows to '{dropped_file}' for inspection")
        
        # Show just first 3 rows in console as examples
        logger.info("Sample of dropped rows:")
        for idx, row in df[rows_to_drop].head(3).iterrows():
            key_values = {k: f"'{row[k]}'" if pd.notna(row[k]) else 'NULL' for k in keys}
            logger.info(f"  Row {idx}: {key_values}")
    
    # Drop rows with missing conflict keys
    df = df.dropna(subset=keys)
    for key in keys:
        df = df[df[key] != ""]
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} rows due to missing conflict keys: {keys}")
    
    # 6) Calculate row hash for change detection
    immutable_cols = ('created_at', 'updated_at', 'row_hash')
    data_cols = [c for c in df.columns if c not in immutable_cols]
    df['row_hash'] = df.apply(
        lambda row: hashlib.md5(
            "|".join(canonicalize(row[c]) for c in data_cols).encode("utf-8")
        ).hexdigest(),
        axis=1
    )
    
    # 7) Add updated_at timestamp
    df['updated_at'] = datetime.now()
    
    # 8) Check for duplicate hashes
    dup = df['row_hash'].duplicated().sum()
    if dup:
        logger.warning(f"{dup} duplicate row_hash values in '{sheet_name}'")
    
    # 9) Check if table exists
    inspector = inspect(engine)
    table_exists = inspector.has_table(sheet_name, schema=schema_name)
    
    with engine.begin() as conn:
        # Set schema search path
        conn.execute(text(f"SET search_path TO {schema_name}"))
        
        # 10) Create table if it doesn't exist
        if not table_exists:
            df.head(0).to_sql(
                name=sheet_name,
                con=conn,
                schema=schema_name,
                if_exists='append',
                index=False
            )
            logger.info(f"✓ Created table '{schema_name}.{sheet_name}'")
        
        # 11) Ensure audit columns exist
        conn.execute(text(f"""
            ALTER TABLE {schema_name}.{sheet_name}
              ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
              ADD COLUMN IF NOT EXISTS row_hash TEXT;
            ALTER TABLE {schema_name}.{sheet_name}
              ALTER COLUMN updated_at DROP DEFAULT;
        """))
        
        # 12) Create unique constraint if it doesn't exist
        constraint_name = f"uq_{sheet_name}_{'_'.join(keys)}"
        cols_list = ", ".join(keys)
        conn.execute(text(f"""
            DO $$ BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = '{schema_name}.{sheet_name}'::regclass
                  AND conname = '{constraint_name}'
              ) THEN
                ALTER TABLE {schema_name}.{sheet_name}
                  ADD CONSTRAINT {constraint_name} UNIQUE ({cols_list});
              END IF;
            END $$;
        """))
        
        # 13) Reflect table metadata for upsert
        metadata = MetaData()
        table = Table(sheet_name, metadata, autoload_with=conn, schema=schema_name)
        
        # 14) Convert NaN to None for proper NULL handling
        df = df.where(pd.notna(df), None)
        
        # 15) Prepare upsert statement
        records = df.to_dict(orient='records')
        stmt = insert(table).values(records)
        
        # Define columns to update (exclude keys and created_at)
        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name not in (*keys, 'created_at')
        }
        update_cols['updated_at'] = text('now()')
        
        # 16) Execute upsert with change detection
        upsert = stmt.on_conflict_do_update(
            index_elements=keys,
            set_=update_cols,
            where=(table.c.row_hash != stmt.excluded.row_hash)
        )
        result = conn.execute(upsert)
        logger.info(f"Upserted {result.rowcount} rows (only changed data)")
        
        # 17) Delete stale rows not in source
        incoming_hashes = tuple(df['row_hash'].unique())
        if incoming_hashes:
            delete_stmt = text(f"""
                DELETE FROM {schema_name}.{sheet_name}
                WHERE row_hash IS NOT NULL
                  AND row_hash NOT IN :hashes
            """).bindparams(hashes=incoming_hashes)
            deleted = conn.execute(delete_stmt).rowcount
            if deleted > 0:
                logger.info(f"🗑️ Deleted {deleted} stale rows")
    
    elapsed = time.time() - start_time
    logger.info(f"✔ Finished '{sheet_name}' in {elapsed:.2f}s\n")

def process_excel_tabs(engine, excel_file, sheet_list, schema_name):
    """Process multiple Excel sheets from a file."""
    try:
        excel = pd.ExcelFile(excel_file)
    except Exception as e:
        logger.error(f"Cannot open Excel file '{excel_file}': {e}")
        sys.exit(1)
    
    # Process each requested sheet
    for sheet in sheet_list:
        if sheet not in excel.sheet_names:
            logger.warning(f"Sheet '{sheet}' not found in Excel file; skipping")
            logger.info(f"Available sheets: {excel.sheet_names}")
            continue
        
        df = excel.parse(sheet)
        if df.empty:
            logger.warning(f"Sheet '{sheet}' is empty; skipping")
            continue
            
        process_sheet(engine, sheet, df, schema_name)

def main():
    """Main execution function."""
    # Load environment variables
    load_dotenv()
    
    # Validate required environment variables
    required = ['EXCEL_FILE_PATH', 'SHEET_NAMES', 'DATABASE_URL']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        logger.info("Please check your .env file")
        sys.exit(1)
    
    # Get configuration
    excel_file = os.getenv('EXCEL_FILE_PATH')
    sheets = [s.strip() for s in os.getenv('SHEET_NAMES').split(',') if s.strip()]
    schema_name = os.getenv('SCHEMA_NAME', 'public')
    database_url = os.getenv('DATABASE_URL')
    
    # Validate Excel file exists
    if not os.path.exists(excel_file):
        logger.error(f"Excel file not found: {excel_file}")
        sys.exit(1)
    
    logger.info(f"Configuration:")
    logger.info(f"  Excel file: {excel_file}")
    logger.info(f"  Sheets: {sheets}")
    logger.info(f"  Schema: {schema_name}")
    
    # Create database engine and test connection
    try:
        engine = sqlalchemy.create_engine(database_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    # Process Excel sheets
    process_excel_tabs(engine, excel_file, sheets, schema_name)
    logger.info("✅ All processing complete")

if __name__ == '__main__':
    main()