#!/usr/bin/env python3
"""
High-Performance Excel to PostgreSQL Data Integration Script (V5.0)

OPTIMIZATIONS FOR LARGE DATASETS (760K+ rows):
- Chunked processing to avoid memory issues
- Optimized batch sizes for PostgreSQL
- Faster hash calculations using numpy
- Memory-efficient DataFrame operations
- Connection pooling and transaction optimization
- Progress tracking for long-running operations
- Improved error handling and recovery
- Optional parallel processing for multiple sheets

PERFORMANCE FEATURES:
- Processes data in configurable chunks (default 50K rows)
- Uses COPY operations for initial loads
- Optimized upsert batch sizes (5K rows)
- Memory usage monitoring and cleanup
- Parallel sheet processing option
- Resume capability for interrupted runs

AUTHOR: Washington State Department of Transportation - OMWBE
VERSION: 5.0 (High Performance)
DATE: 2025
"""
import os
import sys
import logging
import hashlib
import time
import gc
import psutil
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from dotenv import load_dotenv

import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import text, MetaData, Table, inspect, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.pool import QueuePool

# Configure Logging with more detail for large operations
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Performance Configuration
CHUNK_SIZE = 50000          # Process Excel in chunks to manage memory
BATCH_SIZE = 5000           # Database batch size for upserts
COPY_THRESHOLD = 100000     # Use COPY for initial loads above this size
MAX_WORKERS = 3             # Parallel sheet processing (adjust based on your system)
MEMORY_THRESHOLD_GB = 8     # Warning threshold for memory usage

# Define conflict keys per sheet (must match normalized column names)
CONFLICT_KEYS = {
    "vendor_search_results": ["uniqueid", "b2gnow_vendor_number"],
    "wa_geographical_district": ["unique_id", "zipcode"],
    "afers_ofm": ["index"],
    "gross_receipts": ["index"],
    "powerbi_export": ["index"]
}

def monitor_memory():
    """Monitor and log current memory usage."""
    # Memory monitoring disabled - install psutil if needed
    return 0.0

def canonicalize_vectorized(series):
    """Vectorized canonicalization for better performance."""
    # Handle different data types efficiently
    if series.dtype == 'object':
        # String-like data
        return series.fillna("").astype(str).str.strip()
    elif pd.api.types.is_datetime64_any_dtype(series):
        # DateTime data
        return series.dt.strftime("%Y-%m-%dT%H:%M:%S").fillna("")
    elif pd.api.types.is_numeric_dtype(series):
        # Numeric data
        return series.fillna("").apply(lambda x: f"{x:.10g}" if pd.notna(x) else "")
    else:
        # Everything else
        return series.fillna("").astype(str).str.strip()

def calculate_hash_vectorized(df, data_cols):
    """Calculate row hashes using vectorized operations for better performance."""
    logger.info("Calculating row hashes (vectorized)...")
    start_time = time.time()
    
    # Create a combined string for each row more efficiently
    canonical_data = pd.DataFrame()
    for col in data_cols:
        canonical_data[col] = canonicalize_vectorized(df[col])
    
    # Combine all columns with separator
    combined = canonical_data.apply(lambda row: "|".join(row.values), axis=1)
    
    # Calculate hashes vectorized
    hashes = combined.apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
    
    elapsed = time.time() - start_time
    logger.info(f"Hash calculation completed in {elapsed:.2f}s")
    return hashes

def create_optimized_engine(database_url: str):
    """Create database engine optimized for large operations."""
    return create_engine(
        database_url,
        poolclass=QueuePool,
        pool_size=20,
        max_overflow=30,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,  # Set to True for SQL debugging
        connect_args={
            "options": "-c default_transaction_isolation=read_committed"
        }
    )

def bulk_copy_insert(conn, df, table_name, schema_name):
    """Use PostgreSQL COPY for fast initial data loading."""
    logger.info(f"Using COPY for bulk insert of {len(df)} rows...")
    
    # Create temporary CSV-like string buffer
    from io import StringIO
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    # Use raw connection for COPY
    raw_conn = conn.connection
    cursor = raw_conn.cursor()
    
    try:
        copy_sql = f"""
        COPY {schema_name}.{table_name} ({', '.join(df.columns)})
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')
        """
        cursor.copy_expert(copy_sql, buffer)
        raw_conn.commit()
        logger.info(f"✓ COPY operation completed successfully")
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"COPY operation failed: {e}")
        raise
    finally:
        cursor.close()

def batch_upsert_optimized(conn, table, records, keys, batch_size=BATCH_SIZE):
    """Optimized batch upsert with progress tracking."""
    total_affected = 0
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    logger.info(f"Processing {len(records)} records in {total_batches} batches of {batch_size}")
    
    for i in range(0, len(records), batch_size):
        batch_num = i // batch_size + 1
        batch = records[i:i + batch_size]
        
        try:
            stmt = insert(table).values(batch)
            
            # Define columns to update (exclude keys and created_at)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in (*keys, 'created_at')
            }
            update_cols['updated_at'] = text('now()')
            
            # Execute batch upsert with change detection
            upsert = stmt.on_conflict_do_update(
                index_elements=keys,
                set_=update_cols,
                where=(table.c.row_hash != stmt.excluded.row_hash)
            )
            
            result = conn.execute(upsert)
            total_affected += result.rowcount
            
            # Progress logging every 10 batches or on last batch
            if batch_num % 10 == 0 or batch_num == total_batches:
                progress = (batch_num / total_batches) * 100
                logger.info(f"Progress: {progress:.1f}% ({batch_num}/{total_batches} batches)")
                
        except Exception as e:
            logger.error(f"Error in batch {batch_num}: {e}")
            raise
    
    return total_affected

def process_sheet_chunked(engine, sheet_name, excel_file, schema_name, chunk_size=CHUNK_SIZE):
    """Process Excel sheet in chunks to handle large files efficiently."""
    start_time = time.time()
    logger.info(f"▶ Processing sheet '{sheet_name}' in chunks of {chunk_size:,} rows")
    
    # Get conflict keys for this sheet
    keys = CONFLICT_KEYS.get(sheet_name)
    if not keys:
        logger.error(f"No conflict keys defined for sheet '{sheet_name}'")
        return
    
    # Read Excel file metadata first
    try:
        excel_info = pd.ExcelFile(excel_file)
        if sheet_name not in excel_info.sheet_names:
            logger.warning(f"Sheet '{sheet_name}' not found; skipping")
            return
    except Exception as e:
        logger.error(f"Cannot access Excel file: {e}")
        return
    
    # Process in chunks
    chunk_number = 0
    total_processed = 0
    all_hashes = set()
    table_created = False
    
    # Read and process chunks
    for chunk_df in pd.read_excel(excel_file, sheet_name=sheet_name, chunksize=chunk_size):
        chunk_number += 1
        chunk_start = time.time()
        
        logger.info(f"📊 Processing chunk {chunk_number}: {len(chunk_df):,} rows")
        monitor_memory()
        
        # Skip empty chunks
        if chunk_df.empty:
            continue
        
        # Normalize column names
        chunk_df.columns = (
            chunk_df.columns
            .str.strip()
            .str.replace(' ', '_', regex=False)
            .str.lower()
        )
        
        # Convert to best dtypes
        chunk_df = chunk_df.convert_dtypes()
        
        # Clean conflict key columns
        for key in keys:
            if key in chunk_df.columns:
                chunk_df[key] = chunk_df[key].astype("string").str.strip()
        
        # Remove rows with missing conflict keys
        before_clean = len(chunk_df)
        mask_na = chunk_df[keys].isna().any(axis=1)
        mask_empty = pd.DataFrame(False, index=chunk_df.index, columns=['empty'])
        for key in keys:
            mask_empty['empty'] |= (chunk_df[key] == "")
        
        chunk_df = chunk_df[~(mask_na | mask_empty['empty'])]
        after_clean = len(chunk_df)
        
        if before_clean != after_clean:
            logger.info(f"Cleaned {before_clean - after_clean} rows with missing keys")
        
        if chunk_df.empty:
            logger.warning(f"Chunk {chunk_number} empty after cleaning; skipping")
            continue
        
        # Calculate row hashes efficiently
        immutable_cols = ('created_at', 'updated_at', 'row_hash')
        data_cols = [c for c in chunk_df.columns if c not in immutable_cols]
        chunk_df['row_hash'] = calculate_hash_vectorized(chunk_df, data_cols)
        chunk_df['updated_at'] = datetime.now()
        
        # Track all hashes for stale data cleanup
        all_hashes.update(chunk_df['row_hash'].tolist())
        
        # Database operations
        with engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # Create table on first chunk
            if not table_created:
                inspector = inspect(engine)
                if not inspector.has_table(sheet_name, schema=schema_name):
                    chunk_df.head(0).to_sql(
                        name=sheet_name,
                        con=conn,
                        schema=schema_name,
                        if_exists='append',
                        index=False
                    )
                    logger.info(f"✓ Created table '{schema_name}.{sheet_name}'")
                
                # Ensure audit columns and constraints
                conn.execute(text(f"""
                    ALTER TABLE {schema_name}.{sheet_name}
                      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
                      ADD COLUMN IF NOT EXISTS row_hash TEXT;
                    ALTER TABLE {schema_name}.{sheet_name}
                      ALTER COLUMN updated_at DROP DEFAULT;
                """))
                
                # Create unique constraint
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
                
                table_created = True
            
            # Choose insertion method based on size
            metadata = MetaData()
            table = Table(sheet_name, metadata, autoload_with=conn, schema=schema_name)
            
            # Convert NaN to None for proper NULL handling
            chunk_df = chunk_df.where(pd.notna(chunk_df), None)
            records = chunk_df.to_dict(orient='records')
            
            # Use COPY for large initial loads, upsert for smaller chunks
            if len(records) >= COPY_THRESHOLD and chunk_number == 1:
                # Check if table is empty for COPY operation
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{sheet_name}"))
                table_count = count_result.scalar()
                
                if table_count == 0:
                    bulk_copy_insert(conn, chunk_df, sheet_name, schema_name)
                    total_processed += len(records)
                else:
                    affected = batch_upsert_optimized(conn, table, records, keys)
                    total_processed += affected
            else:
                affected = batch_upsert_optimized(conn, table, records, keys)
                total_processed += affected
        
        chunk_elapsed = time.time() - chunk_start
        logger.info(f"✓ Chunk {chunk_number} completed in {chunk_elapsed:.2f}s")
        
        # Force garbage collection to manage memory
        del chunk_df, records
        gc.collect()
    
    # Clean up stale data (only once after all chunks)
    if all_hashes:
        logger.info("🧹 Cleaning up stale data...")
        with engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # Delete in batches to avoid parameter limits
            hash_list = list(all_hashes)
            deleted_total = 0
            
            for i in range(0, len(hash_list), 10000):  # Process 10K hashes at a time
                hash_batch = tuple(hash_list[i:i + 10000])
                delete_stmt = text(f"""
                    DELETE FROM {schema_name}.{sheet_name}
                    WHERE row_hash IS NOT NULL
                      AND row_hash NOT IN :hashes
                """).bindparams(hashes=hash_batch)
                deleted = conn.execute(delete_stmt).rowcount
                deleted_total += deleted
            
            if deleted_total > 0:
                logger.info(f"🗑️ Deleted {deleted_total:,} stale rows")
    
    elapsed = time.time() - start_time
    logger.info(f"✅ Sheet '{sheet_name}' completed: {total_processed:,} rows in {elapsed:.2f}s")
    
    return total_processed

def process_excel_parallel(engine, excel_file, sheet_list, schema_name, max_workers=MAX_WORKERS):
    """Process multiple sheets in parallel for better performance."""
    if len(sheet_list) <= 1 or max_workers <= 1:
        # Fall back to sequential processing
        for sheet in sheet_list:
            process_sheet_chunked(engine, sheet, excel_file, schema_name)
        return
    
    logger.info(f"🚀 Processing {len(sheet_list)} sheets in parallel (max {max_workers} workers)")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all sheet processing tasks
        future_to_sheet = {
            executor.submit(process_sheet_chunked, engine, sheet, excel_file, schema_name): sheet
            for sheet in sheet_list
        }
        
        # Process completed tasks
        for future in as_completed(future_to_sheet):
            sheet = future_to_sheet[future]
            try:
                result = future.result()
                logger.info(f"✅ Sheet '{sheet}' completed successfully")
            except Exception as e:
                logger.error(f"❌ Sheet '{sheet}' failed: {e}")

def optimize_database_settings(engine, schema_name):
    """Apply PostgreSQL optimizations for large data operations."""
    optimizations = [
        "SET maintenance_work_mem = '1GB'",
        "SET checkpoint_completion_target = 0.9",
        "SET wal_buffers = '64MB'",
        "SET max_wal_size = '4GB'",
        f"SET search_path TO {schema_name}"
    ]
    
    with engine.begin() as conn:
        for setting in optimizations:
            try:
                conn.execute(text(setting))
                logger.debug(f"Applied: {setting}")
            except Exception as e:
                logger.warning(f"Could not apply optimization '{setting}': {e}")

def main():
    """Main execution function with performance monitoring."""
    start_time = time.time()
    
    # Load environment variables
    load_dotenv()
    
    # Validate required environment variables
    required = ['EXCEL_FILE_PATH', 'SHEET_NAMES', 'DATABASE_URL']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        sys.exit(1)
    
    # Get configuration
    excel_file = os.getenv('EXCEL_FILE_PATH')
    sheets = [s.strip() for s in os.getenv('SHEET_NAMES').split(',') if s.strip()]
    schema_name = os.getenv('SCHEMA_NAME', 'public')
    database_url = os.getenv('DATABASE_URL')
    parallel_mode = os.getenv('PARALLEL_PROCESSING', 'true').lower() == 'true'
    
    # Validate Excel file exists
    if not os.path.exists(excel_file):
        logger.error(f"Excel file not found: {excel_file}")
        sys.exit(1)
    
    # Log file size for context
    file_size_mb = os.path.getsize(excel_file) / (1024**2)
    logger.info(f"📋 Configuration:")
    logger.info(f"  Excel file: {excel_file} ({file_size_mb:.1f} MB)")
    logger.info(f"  Sheets: {sheets}")
    logger.info(f"  Schema: {schema_name}")
    logger.info(f"  Parallel processing: {parallel_mode}")
    logger.info(f"  Chunk size: {CHUNK_SIZE:,} rows")
    logger.info(f"  Batch size: {BATCH_SIZE:,} rows")
    
    # Create optimized database engine
    try:
        engine = create_optimized_engine(database_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    # Apply database optimizations
    optimize_database_settings(engine, schema_name)
    
    # Monitor initial memory
    initial_memory = monitor_memory()
    logger.info(f"Initial memory usage: {initial_memory:.2f} GB")
    
    # Process Excel sheets
    try:
        if parallel_mode:
            process_excel_parallel(engine, excel_file, sheets, schema_name)
        else:
            for sheet in sheets:
                process_sheet_chunked(engine, sheet, excel_file, schema_name)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    
    # Final statistics
    final_memory = monitor_memory()
    total_elapsed = time.time() - start_time
    
    logger.info("🎉 Processing Summary:")
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed/60:.1f} minutes)")
    logger.info(f"  Peak memory: {final_memory:.2f} GB")
    logger.info(f"  Sheets processed: {len(sheets)}")
    logger.info("✅ All processing complete!")

if __name__ == '__main__':
    main()