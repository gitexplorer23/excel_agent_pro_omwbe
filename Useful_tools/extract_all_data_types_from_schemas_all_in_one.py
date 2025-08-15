import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
EXPORT_PATH = os.getenv("EXPORT_PATH", ".")
# Optional: specify schemas to include, or omit to include all non-system schemas
SCHEMAS = os.getenv("SCHEMAS", "public").split(",")  # e.g. "public,analytics"

# Ensure export path exists
os.makedirs(EXPORT_PATH, exist_ok=True)

# Create DB connection
engine = sqlalchemy.create_engine(DATABASE_URL)

# Query table and column metadata
query_metadata = f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_schema IN ({', '.join(f"'{s}'" for s in SCHEMAS)})
    ORDER BY table_schema, table_name, ordinal_position;
"""

# Fetch metadata into DataFrame
with engine.connect() as conn:
    metadata_df = pd.read_sql_query(text(query_metadata), con=conn)

# Write metadata to Excel
output_file = os.path.join(EXPORT_PATH, "schema_metadata.xlsx")
metadata_df.to_excel(output_file, index=False, sheet_name="columns_and_types")

print(f"✅ Schema metadata written to: {output_file}")
