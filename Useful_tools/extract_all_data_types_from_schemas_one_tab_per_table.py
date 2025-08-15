import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
EXPORT_PATH = os.getenv("EXPORT_PATH", ".")
SCHEMAS = os.getenv("SCHEMAS", "public").split(",")  # e.g. "public,analytics"

# Ensure export path exists
os.makedirs(EXPORT_PATH, exist_ok=True)

# Create DB connection
engine = sqlalchemy.create_engine(DATABASE_URL)

# Fetch column metadata
query_metadata = f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_schema IN ({', '.join(f"'{s.strip()}'" for s in SCHEMAS)})
    ORDER BY table_schema, table_name, ordinal_position;
"""

metadata_df = pd.read_sql_query(text(query_metadata), con=engine)

# Write each table's metadata to its own sheet
output_file = os.path.join(EXPORT_PATH, "schema_metadata_by_table.xlsx")
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for (schema, table), group in metadata_df.groupby(['table_schema', 'table_name']):
        sheet_name = f"{schema}.{table}"[:31]  # Excel sheet name limit
        group[['column_name', 'data_type']].to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"✅ Wrote metadata for: {schema}.{table}")

print(f"✅ Finished exporting schema metadata to: {output_file}")
