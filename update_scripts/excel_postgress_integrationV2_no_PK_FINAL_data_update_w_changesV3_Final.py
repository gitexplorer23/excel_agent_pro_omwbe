'''
This script processes an Excel file, exports specified sheets to CSV,
and updates a PostgreSQL database with the csv data. It handles change detection
by calculating a hash for each row, allowing for efficient upserts and deletions
of stale data. It uses SQLAlchemy for database interactions and Pandas for data manipulation.
Processing includes:
1. Normalizing column names and calculating a row hash for change detection.
2. Stamping each row with an updated timestamp.
3. Deleting rows in the database that do not match the incoming data.
4. Inserting new or changed rows while skipping unchanged ones.
Note: If the source excel file has a new column, it needs to be added to the database schema manually. then rund the script again.

'''

import os
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
from sqlalchemy import text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

def calculate_row_hash(row, data_cols):
    """Concatenate and hash all relevant fields for change detection."""
    concat = '|'.join(str(row[col]) for col in data_cols)
    return hashlib.md5(concat.encode('utf-8')).hexdigest()

def process_sheet(engine, sheet_name, df, schema_name):
    # 1) Normalize column names and compute row_hash
    df.columns = df.columns.str.strip().str.replace(' ', '_')
    data_cols = [c for c in df.columns if c not in ('created_at','updated_at','row_hash')]
    df['row_hash'] = df.apply(lambda r: calculate_row_hash(r, data_cols), axis=1)

    # 2) Stamp updated_at for every incoming row
    now = datetime.now()
    df['updated_at'] = now

    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema_name}"))

        # 3) Ensure table exists and has audit/hash columns
        df.head(0).to_sql(
            name=sheet_name, con=conn, schema=schema_name,
            if_exists='append', index=False
        )
        conn.execute(text(f"""
            ALTER TABLE {schema_name}.{sheet_name}
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now(),
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now(),
            ADD COLUMN IF NOT EXISTS row_hash TEXT;
        """))

        # 4) Reflect table metadata for insert/delete
        metadata = MetaData()
        table = Table(sheet_name, metadata, autoload_with=conn, schema=schema_name)

        # 5) Delete rows whose row_hash is not in incoming set
        incoming_hashes = tuple(df['row_hash'].unique())
        conn.execute(text(f"""
            DELETE FROM {schema_name}.{sheet_name}
            WHERE row_hash NOT IN :hashes
        """).bindparams(hashes=incoming_hashes))

        # 6) Insert all incoming rows, skipping unchanged ones
        records = df.to_dict(orient='records')
        stmt = insert(table).values(records)
        upsert = stmt.on_conflict_do_nothing(index_elements=['row_hash'])
        conn.execute(upsert)

        print(f"Processed '{sheet_name}': stale rows removed, new/changed rows inserted.")

def export_sheets_to_csv(excel_file, sheet_list, csv_dir):
    """Export specified sheets from Excel to CSV."""
    excel = pd.ExcelFile(excel_file)
    for name in sheet_list:
        if name in excel.sheet_names:
            df = excel.parse(name)
            df.to_csv(os.path.join(csv_dir, f"{name}.csv"), index=False)
            print(f"Exported sheet '{name}' to {csv_dir}/{name}.csv")
        else:
            print(f"⚠️ Sheet '{name}' not found in Excel file.")

def process_all_csv(engine, sheet_list, csv_dir, schema_name):
    """Process each CSV via process_sheet."""
    for sheet_name in sheet_list:
        csv_path = os.path.join(csv_dir, f"{sheet_name}.csv")
        if not os.path.exists(csv_path):
            print(f"⚠️ CSV for '{sheet_name}' not found, skipping.")
            continue
        df = pd.read_csv(csv_path, dtype=str)
        print(f"\n▶ Processing '{sheet_name}' …")
        process_sheet(engine, sheet_name, df, schema_name)

def main():
    load_dotenv()
    # Required environment variables
    excel_file = os.getenv('EXCEL_FILE_PATH')
    schema_name = os.getenv('SCHEMA_NAME', 'public')
    sheet_list = [s.strip() for s in os.getenv('SHEET_NAMES', '').split(',') if s.strip()]

    missing = [v for v in ['EXCEL_FILE_PATH','SHEET_NAMES','DB_USER','DB_PASSWORD','DB_HOST','DB_NAME']
               if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    # Build database connection
    conn_str = (
        os.getenv("DATABASE_URL")
    )
    engine = sqlalchemy.create_engine(conn_str)

    # Prepare CSV directory
    base_dir = os.path.dirname(excel_file)
    csv_dir = os.path.join(base_dir, 'CLEANED_CSV')
    os.makedirs(csv_dir, exist_ok=True)

    # Execute export and processing phases
    export_sheets_to_csv(excel_file, sheet_list, csv_dir)
    process_all_csv(engine, sheet_list, csv_dir, schema_name)

if __name__ == "__main__":
    main()

