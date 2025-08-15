'''
Excel to PostgreSQL Data Integration Script (V4.1)

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

'''
import os
import sys
import logging
import hashlib
import time
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd
import sqlalchemy
from sqlalchemy import text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert

# ── Configure Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Define per‐sheet conflict keys ──────────────────────────────────────────────
# Keys must match normalized column names (lowercase, underscores)
CONFLICT_KEYS = {
    "vendor_search_results": ["uniqueid", "b2gnow_vendor_number"]
}



def calculate_row_hash(row, data_cols):
    """Concatenate and hash all relevant fields for change detection."""
    concat = "|".join(str(row[c]) for c in data_cols)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def process_sheet(engine, sheet_name, df, schema_name):
    start_time = time.time()
    logger.info(f"▶ Processing sheet '{sheet_name}'")

    # 1) Normalize column names
    df.columns = (
        df.columns
          .str.strip()
          .str.replace(' ', '_', regex=False)
          .str.lower()
    )

    # 2) Log inferred dtypes
    logger.info(f"Detected dtypes for '{sheet_name}':\n{df.dtypes}")

    # 3) Convert dtypes
    df = df.convert_dtypes()

    # 4) Compute row_hash for change detection
    immutable = ('created_at', 'updated_at', 'row_hash')
    data_cols = [c for c in df.columns if c not in immutable]
    df['row_hash'] = df.apply(lambda r: calculate_row_hash(r, data_cols), axis=1)

    # 5) Stamp updated_at
    df['updated_at'] = datetime.now()

    # 6) Warn on duplicate hashes
    dup = df['row_hash'].duplicated().sum()
    if dup:
        logger.warning(f"{dup} duplicate row_hash values in '{sheet_name}'")

    # 7) Get conflict key for this sheet
    keys = CONFLICT_KEYS.get(sheet_name)
    if not keys:
        logger.error(f"No conflict keys defined for sheet '{sheet_name}'")
        sys.exit(1)

    inspector = inspect(engine)
    table_exists = inspector.has_table(sheet_name, schema=schema_name)

    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}"))

        # 8) Create table if missing
        if not table_exists:
            df.head(0).to_sql(
                name=sheet_name,
                con=conn,
                schema=schema_name,
                if_exists='append',  # never drop existing
                index=False
            )
            logger.info(f"✓ Created table '{schema_name}.{sheet_name}'")

        # 9) Ensure audit and hash columns
        conn.execute(text(f"""
            ALTER TABLE {schema_name}.{sheet_name}
              ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
              ADD COLUMN IF NOT EXISTS row_hash TEXT;
            ALTER TABLE {schema_name}.{sheet_name}
              ALTER COLUMN updated_at DROP DEFAULT;
        """))

        # 10) Ensure UNIQUE on conflict keys
        conname = f"uq_{sheet_name}_{'_'.join(keys)}"
        cols = ", ".join(keys)
        conn.execute(text(f"""
            DO $$ BEGIN
              IF NOT EXISTS (
                SELECT 1
                  FROM pg_constraint
                 WHERE conrelid = '{schema_name}.{sheet_name}'::regclass
                   AND conname = '{conname}'
              ) THEN
                ALTER TABLE {schema_name}.{sheet_name}
                  ADD CONSTRAINT {conname} UNIQUE ({cols});
              END IF;
            END $$;
        """))

        # 11) Reflect table metadata for upsert
        meta = MetaData()
        meta.clear()
        table = Table(sheet_name, meta, autoload_with=conn, schema=schema_name)

        # 12) Prepare and execute UPSERT
        records = df.to_dict(orient='records')
        stmt = insert(table).values(records)
        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name not in (*keys, 'created_at')
        }
        update_cols['updated_at'] = text('now()')

        
        # Use ON CONFLICT to update only if row_hash has change
        upsert = stmt.on_conflict_do_update(
            index_elements=keys,
            set_=update_cols,
            where=(table.c.row_hash != stmt.excluded.row_hash)
            
        )
        # 13) Delete stale rows no longer in source
        incoming_hashes = tuple(df['row_hash'].unique())
        if incoming_hashes:  # avoid deleting everything if input is empty
            delete_stmt = text(f"""
                DELETE FROM {schema_name}.{sheet_name}
                WHERE row_hash IS NOT NULL
                  AND row_hash NOT IN :hashes
            """).bindparams(hashes=incoming_hashes)
            deleted = conn.execute(delete_stmt).rowcount
            logger.info(f"🗑️ Deleted {deleted} stale rows from '{sheet_name}'")


        result = conn.execute(upsert)
        logger.info(f"Upserted {result.rowcount} rows (only data & updated_at on change)")

    elapsed = time.time() - start_time
    logger.info(f"✔ Finished '{sheet_name}' in {elapsed:.2f}s\n")
    
    


def process_excel_tabs(engine, excel_file, sheet_list, schema_name):
    try:
        excel = pd.ExcelFile(excel_file)
    except Exception as e:
        logger.error(f"Cannot open Excel '{excel_file}': {e}")
        sys.exit(1)

    for sheet in sheet_list:
        if sheet not in excel.sheet_names:
            logger.warning(f"Sheet '{sheet}' missing; skipping")
            continue
        df = excel.parse(sheet)
        process_sheet(engine, sheet, df, schema_name)


def main():
    load_dotenv()
    required = ['EXCEL_FILE_PATH','SHEET_NAMES','DATABASE_URL']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"Missing env vars: {missing}")
        sys.exit(1)

    excel_file = os.getenv('EXCEL_FILE_PATH')
    sheets = [s.strip() for s in os.getenv('SHEET_NAMES').split(',') if s.strip()]
    schema = os.getenv('SCHEMA_NAME','public')

    try:
        engine = sqlalchemy.create_engine(os.getenv('DATABASE_URL'))
        with engine.connect(): pass
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        sys.exit(1)

    process_excel_tabs(engine, excel_file, sheets, schema)


if __name__ == '__main__':
    main()
