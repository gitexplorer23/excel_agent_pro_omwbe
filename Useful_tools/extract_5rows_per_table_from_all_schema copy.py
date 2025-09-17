import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
SCHEMA_NAME = os.getenv("SCHEMA_NAME", "ppt")
EXPORT_PATH = os.getenv("EXPORT_PATH", ".")  # Default to current directory

# Ensure export path exists
os.makedirs(EXPORT_PATH, exist_ok=True)

# Create DB connection
engine = sqlalchemy.create_engine(DATABASE_URL)

# Query table names
query_tables = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{SCHEMA_NAME}'
      AND table_type = 'BASE TABLE';
"""

with engine.connect() as conn:
    tables = [row[0] for row in conn.execute(text(query_tables))]

# Define output file path
output_file = os.path.join(EXPORT_PATH, "tables_preview.xlsx")

# Save to Excel with one sheet per table
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for table in tables:
        try:
            df = pd.read_sql_query(f'SELECT * FROM "{SCHEMA_NAME}"."{table}" LIMIT 5', con=engine)

            # Strip timezone info from datetime columns
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)

            df.to_excel(writer, sheet_name=table[:31], index=False)  # Excel sheet name limit = 31 chars
            print(f"✅ Wrote sheet for table: {table}")
        except Exception as e:
            print(f"⚠️ Skipping table '{table}': {e}")

print(f"✅ Finished: {output_file} created with sheets per table.")
