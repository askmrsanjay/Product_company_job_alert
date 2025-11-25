"""
Data validation and processing for LinkedIn Scraper Pipeline
"""
import pandas as pd
import logging
from datetime import datetime
import sys
import os

from config import REQUIRED_COLUMNS

logger = logging.getLogger("linkedin_scraper")

def ensure_required_columns(df):
    """Ensure DataFrame has all required columns with default values"""
    required_columns = REQUIRED_COLUMNS.copy()
    required_columns['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for col, default_value in required_columns.items():
        if col not in df.columns:
            df[col] = default_value
            logger.debug(f"Added missing column '{col}' with default value")
    
    return df

def validate_dataframe(df):
    """Validate DataFrame structure and content"""
    if df.empty:
        logger.warning("DataFrame is empty")
        return False
    
    required_columns = ['job_id', 'job_title', 'company_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for null job_ids
    null_job_ids = df['job_id'].isnull().sum()
    if null_job_ids > 0:
        logger.warning(f"Found {null_job_ids} jobs with null job_ids, removing them")
        df.dropna(subset=['job_id'], inplace=True)
    
    # Check for duplicate job_ids
    duplicates = df['job_id'].duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate job_ids, removing duplicates")
        df.drop_duplicates(subset=['job_id'], inplace=True)
    
    logger.info(f"DataFrame validation passed. Shape: {df.shape}")
    return True

def analyze_duplicates(df_existing, df_new):
    """Analyze and report on duplicate data"""
    existing_job_ids = set(df_existing['job_id'].astype(str))
    new_job_ids = set(df_new['job_id'].astype(str))
    overlapping_ids = existing_job_ids.intersection(new_job_ids)
    
    analysis = {
        "existing_unique_jobs": len(existing_job_ids),
        "new_unique_jobs": len(new_job_ids),
        "overlapping_jobs": len(overlapping_ids),
        "truly_new_jobs": len(new_job_ids) - len(overlapping_ids),
        "sample_overlapping_ids": list(overlapping_ids)[:5] if overlapping_ids else []
    }
    
    logger.info(f"Job ID analysis:")
    logger.info(f"  - Existing unique job_ids: {analysis['existing_unique_jobs']}")
    logger.info(f"  - New unique job_ids: {analysis['new_unique_jobs']}")
    logger.info(f"  - Overlapping job_ids: {analysis['overlapping_jobs']}")
    logger.info(f"  - Truly new job_ids: {analysis['truly_new_jobs']}")
    
    if analysis['sample_overlapping_ids']:
        logger.info(f"  - Sample overlapping IDs: {analysis['sample_overlapping_ids']}")
    
    return analysis

def process_cumulative_data(df_existing, df_new):
    """Process and merge cumulative data with detailed analysis"""
    initial_count = len(df_existing)
    
    # Analyze duplicates
    duplicate_analysis = analyze_duplicates(df_existing, df_new)
    
    # Combine datasets
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    before_dedup_count = len(df_combined)
    
    # Detailed duplicate analysis
    duplicate_mask = df_combined.duplicated(subset=["job_id"], keep=False)
    duplicate_jobs = df_combined[duplicate_mask]
    unique_duplicate_ids = duplicate_jobs['job_id'].nunique() if len(duplicate_jobs) > 0 else 0
    
    logger.info(f"Duplicate analysis:")
    logger.info(f"  - Total rows before deduplication: {before_dedup_count}")
    logger.info(f"  - Rows marked as duplicates: {duplicate_mask.sum()}")
    logger.info(f"  - Unique job_ids with duplicates: {unique_duplicate_ids}")
    
    if unique_duplicate_ids > 0:
        sample_duplicate_ids = duplicate_jobs['job_id'].unique()[:5]
        logger.info(f"  - Sample duplicate job_ids: {list(sample_duplicate_ids)}")
    
    # Remove duplicates
    df_combined = df_combined.drop_duplicates(subset=["job_id"], keep='last').reset_index(drop=True)
    final_count = len(df_combined)
    
    # Calculate metrics
    duplicates_removed = before_dedup_count - final_count
    net_change = final_count - initial_count
    actual_new_unique_jobs = duplicate_analysis['truly_new_jobs']
    
    logger.info(f"Final deduplication results:")
    logger.info(f"  - Total before deduplication: {before_dedup_count}")
    logger.info(f"  - Duplicates removed: {duplicates_removed}")
    logger.info(f"  - Final count: {final_count}")
    logger.info(f"  - Net database change: {net_change}")
    logger.info(f"  - Actual new unique jobs: {actual_new_unique_jobs}")
    
    # Smart interpretation
    if net_change < 0:
        logger.warning(f"⚠️  Net negative change detected ({net_change} jobs)")
        logger.info("📊 Analysis: Improved duplicate detection removing old duplicates")
        logger.info(f"✅ Actual contribution: {actual_new_unique_jobs} new unique jobs")
        new_jobs_count = actual_new_unique_jobs
    else:
        logger.info(f"✅ Successfully added {net_change} net new jobs")
        new_jobs_count = net_change
    
    # Data quality metrics
    quality_metrics = {
        "total_jobs_processed": len(df_new),
        "unique_new_jobs": actual_new_unique_jobs,
        "duplicate_rate": duplicate_analysis['overlapping_jobs'] / len(df_new) * 100 if len(df_new) > 0 else 0,
        "deduplication_efficiency": duplicates_removed / before_dedup_count * 100 if before_dedup_count > 0 else 0
    }
    
    logger.info(f"📈 Data quality metrics:")
    logger.info(f"  - Duplicate rate in new data: {quality_metrics['duplicate_rate']:.1f}%")
    logger.info(f"  - Deduplication efficiency: {quality_metrics['deduplication_efficiency']:.1f}%")
    
    return df_combined, new_jobs_count, quality_metrics