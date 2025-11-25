"""
File operations and storage management for LinkedIn Scraper Pipeline
"""
import os
import json
import glob
import logging
import pandas as pd
import traceback
from datetime import datetime
import sys
sys.path.append(
    "/Workspace/Shared/linkedin_scraper/CareerFlowEngine/src"
)
# Current import (may be missing get_file_size)
from monitoring.memory_monitor import get_file_size, get_directory_size

# Make sure this line exists at the top of file_manager.py


logger = logging.getLogger("linkedin_scraper")

def save_json_safely(df, file_path):
    """Save DataFrame to JSON file safely"""
    try:
        df.to_json(file_path, orient="records", lines=True, force_ascii=False)
        file_size = get_file_size(file_path)
        logger.info(f"✓ Saved JSON: {os.path.basename(file_path)} ({file_size} MB)")
        return True, file_size
    except Exception as e:
        logger.error(f"Failed to save JSON {file_path}: {e}")
        return False, 0

def load_json_safely(file_path):
    """Load JSON file safely"""
    try:
        if not os.path.exists(file_path):
            logger.info(f"File doesn't exist (will create new): {os.path.basename(file_path)}")
            return pd.DataFrame()
        
        df = pd.read_json(file_path, lines=True)
        logger.info(f"✓ Loaded {len(df)} records from {os.path.basename(file_path)}")
        return df
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return pd.DataFrame()

def save_parquet_files(df, parquet_dir, timestamp):
    """Save DataFrame as parquet files with proper data type handling"""
    file_sizes = {}
    parquet_saved = False
    
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Create parquet directory
        os.makedirs(parquet_dir, exist_ok=True)
        logger.info(f"✓ Parquet directory ready: {parquet_dir}")
        
        # Prepare DataFrame for parquet conversion
        df_parquet = df.copy()
        
        logger.info("🔧 Converting data types for parquet compatibility...")
        
        # Handle list columns (responsibilities, skills)
        for col in ['responsibilities', 'skills']:
            if col in df_parquet.columns:
                df_parquet[col] = df_parquet[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else str(x) if x is not None else ""
                )
        
        # Convert all columns to strings
        for col in df_parquet.columns:
            df_parquet[col] = df_parquet[col].astype(str)
        
        # Define explicit schema
        schema_fields = [pa.field(col, pa.string()) for col in df_parquet.columns]
        schema = pa.schema(schema_fields)
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df_parquet, schema=schema)
        
        # Save parquet files
        files_to_save = [
            (f"linkedin_jobs_{timestamp}.parquet", "history_parquet_mb"),
            ("linkedin_jobs_latest.parquet", "latest_parquet_mb"),
            ("linkedin_jobs_cumulative.parquet", "cumulative_parquet_mb")
        ]
        
        for filename, size_key in files_to_save:
            file_path = os.path.join(parquet_dir, filename)
            
            if filename == "linkedin_jobs_cumulative.parquet" and os.path.exists(file_path):
                # Handle cumulative file specially
                try:
                    existing_table = pq.read_table(file_path)
                    df_existing = existing_table.to_pandas()
                    
                    df_combined = pd.concat([df_existing, df_parquet], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=["job_id"], keep='last').reset_index(drop=True)
                    
                    combined_table = pa.Table.from_pandas(df_combined, schema=schema)
                    pq.write_table(combined_table, file_path, compression='snappy')
                    
                except Exception as e:
                    logger.error(f"Failed to update cumulative parquet: {e}")
                    pq.write_table(table, file_path, compression='snappy')
            else:
                pq.write_table(table, file_path, compression='snappy')
            
            file_sizes[size_key] = get_file_size(file_path)
            logger.info(f"✓ Saved parquet: {filename} ({file_sizes[size_key]} MB)")
        
        parquet_saved = True
        logger.info("🎉 All parquet files created successfully!")
        
    except ImportError:
        logger.warning("⚠ PyArrow not available, skipping parquet file creation")
    except Exception as e:
        logger.error(f"Failed to save parquet files: {e}")
        logger.debug(traceback.format_exc())
    
    return parquet_saved, file_sizes

def save_performance_data(performance_summary, json_dir, timestamp):
    """Save detailed performance data"""
    try:
        # Save timestamped performance data
        perf_file = os.path.join(json_dir, f"performance_data_{timestamp}.json")
        with open(perf_file, 'w', encoding='utf-8') as f:
            json.dump(performance_summary, f, indent=2, ensure_ascii=False)
        
        # Save latest performance data
        perf_latest_file = os.path.join(json_dir, "performance_data_latest.json")
        with open(perf_latest_file, 'w', encoding='utf-8') as f:
            json.dump(performance_summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Performance data saved: {os.path.basename(perf_file)}")
        
    except Exception as e:
        logger.warning(f"Could not save performance data: {e}")

def save_pipeline_status(status, details, json_dir):
    """Save pipeline status for monitoring"""
    try:
        os.makedirs(json_dir, exist_ok=True)
        status_file = os.path.join(json_dir, "pipeline_status.json")
        
        status_info = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": details or {}
        }
        
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(status_info, f, indent=2, ensure_ascii=False)
        
        # Print status for console
        if status == "SUCCESS":
            print(f"🟢 PIPELINE SUCCESS: {details}")
        elif status == "RUNNING":
            print(f"🟡 PIPELINE RUNNING: {details}")
        else:
            print(f"🔴 PIPELINE {status}: {details}")
            
    except Exception as e:
        logger.warning(f"Could not save pipeline status: {e}")

def cleanup_old_files(directory, pattern, max_files):
    """Keep only the latest max_files, delete older ones"""
    try:
        if not os.path.exists(directory):
            logger.warning(f"Directory does not exist: {directory}")
            return
        
        files = sorted(glob.glob(os.path.join(directory, pattern)), key=os.path.getmtime, reverse=True)
        
        if len(files) <= max_files:
            logger.info(f"No cleanup needed in {directory}. Found {len(files)} files, limit is {max_files}")
            return
        
        deleted_count = 0
        for old_file in files[max_files:]:
            try:
                os.remove(old_file)
                deleted_count += 1
                logger.debug(f"Deleted old file: {os.path.basename(old_file)}")
            except Exception as e:
                logger.warning(f"Failed to delete {old_file}: {e}")
        
        logger.info(f"Cleanup completed in {directory}: deleted {deleted_count}/{len(files) - max_files} old files")
                
    except Exception as e:
        logger.error(f"Cleanup operation failed in {directory}: {e}")