"""
LinkedIn Scraper Pipeline - Main Execution File
"""
import os
import sys
import pandas as pd
import traceback
from datetime import datetime

# Check environment
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    IS_DATABRICKS = True
    print("✓ Running in Databricks environment")
except ImportError:
    IS_DATABRICKS = False
    print("✓ Running in standard Python environment")

# Add the src directory to sys.path
project_root = '/Workspace/Shared/linkedin_scraper/CareerFlowEngine'
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from config import KEYWORDS, LOCATIONS, MAX_JOBS_PER_LOCATION, MAX_HISTORY_FILES, get_directories
from utils.logger_setup import setup_logging
from monitoring.performance_tracker import PerformanceTracker

from monitoring.memory_monitor import (
    log_memory_checkpoint, 
    get_dataframe_memory_usage, 
    cleanup_memory, 
    get_directory_size,
    get_file_size
)
from scraping.enhanced_scraper import TimedLinkedInScraper

from data_manage.data_manager import (
    ensure_required_columns, 
    validate_dataframe, 
    process_cumulative_data
)
from utils.file_manager import (
    save_json_safely, 
    load_json_safely, 
    save_parquet_files,
    save_performance_data, 
    save_pipeline_status, 
    cleanup_old_files
)
from utils.utils import validate_environment, print_summary

def main():
    """Main execution function with comprehensive monitoring"""
    
    # Initialize components
    start_time = datetime.now()
    logger = setup_logging()
    perf_tracker = PerformanceTracker()
    perf_tracker.start_total_tracking()
    
    # Get directories
    directories = get_directories(IS_DATABRICKS)
    
    # Setup directories
    for dir_path in directories.values():
        os.makedirs(dir_path, exist_ok=True)
    
    start_memory = log_memory_checkpoint("Pipeline Start")
    save_pipeline_status("RUNNING", {"start_time": start_time.isoformat()}, directories['json'])
    
    try:
        logger.info("=" * 80)
        logger.info("LINKEDIN SCRAPING PIPELINE - MODULAR VERSION")
        logger.info("=" * 80)
        logger.info(f"Pipeline started at: {start_time}")
        
        # Environment validation
        env_issues = validate_environment()
        post_validation_memory = log_memory_checkpoint("After Environment Validation")
        
        if env_issues:
            logger.warning("Environment validation issues found:")
            for issue in env_issues:
                logger.warning(f"  - {issue}")
            
            # Check if issues are critical
            critical_issues = [i for i in env_issues if "Network" in i or "Missing packages" in i]
            if critical_issues:
                save_pipeline_status("FAILED", {"reason": "Environment validation failed", "issues": env_issues}, directories['json'])
                return False
            else:
                logger.info("Non-critical issues found, continuing...")
        
        logger.info(f"Keywords: {KEYWORDS}")
        logger.info(f"Locations: {LOCATIONS}")
        logger.info(f"Max jobs per location: {MAX_JOBS_PER_LOCATION}")

        # Initialize enhanced scraper
        try:
            scraper = TimedLinkedInScraper(
                keywords=KEYWORDS,
                locations=LOCATIONS,
                delay_range=(6, 12),
                max_retries=3,
                perf_tracker=perf_tracker
            )
            logger.info("✓ Enhanced scraper initialized successfully")
        except Exception as e:
            logger.error(f"Scraper initialization failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Scraper initialization failed", "error": str(e)}, directories['json'])
            return False

        # Scrape jobs with performance tracking
        logger.info("🔄 Starting job scraping process with performance tracking...")
        pre_scraping_memory = log_memory_checkpoint("Before Scraping")
        
        try:
            new_jobs, failed_jobs = scraper.scrape_jobs(max_jobs_per_location=MAX_JOBS_PER_LOCATION)
            post_scraping_memory = log_memory_checkpoint("After Scraping")
            
            # Get performance summary
            performance_summary = perf_tracker.get_performance_summary()
            
            logger.info(f"📊 Scraping performance:")
            logger.info(f"  - Total jobs scraped: {len(new_jobs)}")
            logger.info(f"  - Total jobs failed: {len(failed_jobs)}")
            logger.info(f"  - Total scraping time: {performance_summary['summary_stats'].get('total_scraping_time_seconds', 0)}s")
            logger.info(f"  - Average time per job: {performance_summary['summary_stats'].get('avg_job_duration_seconds', 0)}s")
            
        except Exception as e:
            logger.error(f"Job scraping failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Job scraping failed", "error": str(e)}, directories['json'])
            return False

        # Validate results
        if not new_jobs:
            logger.error("No jobs scraped successfully")
            if failed_jobs:
                logger.error("Sample failures:")
                for i, failure in enumerate(failed_jobs[:3]):
                    logger.error(f"  {i+1}. {failure}")
            save_pipeline_status("FAILED", {"reason": "No jobs scraped", "failed_count": len(failed_jobs)}, directories['json'])
            return False

        # Process scraped data
        try:
            logger.info("🔄 Processing scraped data...")
            df_new = pd.DataFrame(new_jobs)
            df_new = ensure_required_columns(df_new)
            
            # Track DataFrame memory usage
            df_memory_mb = get_dataframe_memory_usage(df_new, "New Jobs DataFrame")
            
            if not validate_dataframe(df_new):
                logger.error("DataFrame validation failed")
                save_pipeline_status("FAILED", {"reason": "DataFrame validation failed"}, directories['json'])
                return False
            
            logger.info(f"✓ Data processing completed. DataFrame shape: {df_new.shape}")
            post_processing_memory = log_memory_checkpoint("After Data Processing")
            
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Data processing failed", "error": str(e)}, directories['json'])
            return False

        # Display sample data
        try:
            logger.info("Sample scraped data:")
            sample_columns = ['job_title', 'company_name', 'country', 'scraped_at']
            available_columns = [col for col in sample_columns if col in df_new.columns]
            print(df_new[available_columns].head())
        except Exception as e:
            logger.warning(f"Could not display sample data: {e}")

        # Save files with comprehensive tracking
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_sizes = {}
            
            logger.info("💾 Saving JSON files...")
            
            # Save historical JSON
            success, size = save_json_safely(df_new, os.path.join(directories['json'], f"linkedin_jobs_{timestamp}.json"))
            if not success:
                raise Exception("Failed to save historical JSON")
            file_sizes['history_json_mb'] = size
            
            # Save latest JSON
            success, size = save_json_safely(df_new, os.path.join(directories['json'], "linkedin_jobs_latest.json"))
            if not success:
                raise Exception("Failed to save latest JSON")
            file_sizes['latest_json_mb'] = size

            # Process cumulative data
            cumulative_file = os.path.join(directories['json'], "linkedin_jobs_cumulative.json")
            old_cumulative_size = get_file_size(cumulative_file)
            
            if os.path.exists(cumulative_file):
                try:
                    df_existing = load_json_safely(cumulative_file)
                    existing_memory_mb = get_dataframe_memory_usage(df_existing, "Existing Cumulative DataFrame")
                    
                    logger.info(f"Loading existing cumulative data:")
                    logger.info(f"  - Existing jobs: {len(df_existing)}")
                    logger.info(f"  - Existing file size: {old_cumulative_size} MB")
                    logger.info(f"  - New jobs scraped: {len(df_new)}")
                    
                    df_combined, new_jobs_count, quality_metrics = process_cumulative_data(df_existing, df_new)
                    
                    # Clean up intermediate DataFrames
                    del df_existing
                    cleanup_memory()
                    
                except Exception as e:
                    logger.error(f"Failed to load existing cumulative data: {e}")
                    df_combined = df_new
                    new_jobs_count = len(df_new)
                    quality_metrics = {"total_jobs_processed": len(df_new), "unique_new_jobs": len(df_new), "duplicate_rate": 0, "deduplication_efficiency": 0}
            else:
                df_combined = df_new
                new_jobs_count = len(df_new)
                quality_metrics = {"total_jobs_processed": len(df_new), "unique_new_jobs": len(df_new), "duplicate_rate": 0, "deduplication_efficiency": 0}
            
            # Save cumulative file
            success, size = save_json_safely(df_combined, cumulative_file)
            if not success:
                raise Exception("Failed to save cumulative JSON")
            file_sizes['cumulative_json_mb'] = size
            
            # Track final DataFrame memory
            final_memory_mb = get_dataframe_memory_usage(df_combined, "Final Combined DataFrame")
            
            # Save parquet files
            parquet_success, parquet_sizes = save_parquet_files(df_combined, directories['parquet'], timestamp)
            file_sizes.update(parquet_sizes)
            
            # Save performance data
            save_performance_data(performance_summary, directories['json'], timestamp)
            
            # Get directory sizes
            json_dir_stats = get_directory_size(directories['json'])
            parquet_dir_stats = get_directory_size(directories['parquet'])
            
            logger.info(f"📊 Storage summary:")
            logger.info(f"  - JSON directory: {json_dir_stats['total_size_mb']} MB ({json_dir_stats['file_count']} files)")
            logger.info(f"  - Parquet directory: {parquet_dir_stats['total_size_mb']} MB ({parquet_dir_stats['file_count']} files)")
            logger.info(f"  - Total storage: {json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb']} MB")
            
            post_saving_memory = log_memory_checkpoint("After File Saving")
            
        except Exception as e:
            logger.error(f"File saving failed: {e}")
            save_pipeline_status("FAILED", {"reason": "File saving failed", "error": str(e)}, directories['json'])
            return False

        # Generate comprehensive statistics
        try:
            end_time = datetime.now()
            end_memory = log_memory_checkpoint("Pipeline End")
            duration = (end_time - start_time).total_seconds()
            success_rate = len(new_jobs) / (len(new_jobs) + len(failed_jobs)) * 100 if (new_jobs or failed_jobs) else 0
            
            stats = {
                "last_updated": timestamp,
                "execution_time_seconds": duration,
                "jobs_scraped_this_run": len(new_jobs),
                "jobs_failed": len(failed_jobs),
                "success_rate_percent": round(success_rate, 1),
                "total_cumulative_jobs": len(df_combined),
                "new_jobs_added": new_jobs_count,
                "companies": df_combined['company_name'].nunique() if 'company_name' in df_combined.columns else 0,
                "countries": df_combined['country'].nunique() if 'country' in df_combined.columns else 0,
                "parquet_files_created": parquet_success,
                "performance_metrics": performance_summary,
                "data_quality_metrics": quality_metrics,
                "memory_stats": {
                    "start_memory_mb": start_memory.get('process_rss_mb', 0),
                    "end_memory_mb": end_memory.get('process_rss_mb', 0),
                    "memory_growth_mb": end_memory.get('process_rss_mb', 0) - start_memory.get('process_rss_mb', 0),
                    "dataframe_memory_mb": final_memory_mb,
                    "system_memory_usage_percent": end_memory.get('system_usage_percent', 0),
                    "psutil_available": True
                },
                "file_sizes": file_sizes,
                "storage_stats": {
                    "json_directory_mb": json_dir_stats['total_size_mb'],
                    "parquet_directory_mb": parquet_dir_stats['total_size_mb'],
                    "total_storage_mb": json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb'],
                    "json_file_count": json_dir_stats['file_count'],
                    "parquet_file_count": parquet_dir_stats['file_count']
                }
            }
            
            # Save statistics
            stats_file = os.path.join(directories['json'], "scraping_stats.json")
            with open(stats_file, 'w', encoding='utf-8') as f:
                import json as json_lib
                json_lib.dump(stats, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Statistics saved: {os.path.basename(stats_file)}")
            
        except Exception as e:
            logger.warning(f"Could not save statistics: {e}")

        # Cleanup old files
        try:
            logger.info("🧹 Starting cleanup of old files...")
            cleanup_old_files(directories['json'], "linkedin_jobs_*.json", MAX_HISTORY_FILES)
            cleanup_old_files(directories['json'], "performance_data_*.json", MAX_HISTORY_FILES)
            cleanup_old_files(directories['parquet'], "linkedin_jobs_*.parquet", MAX_HISTORY_FILES)
            cleanup_old_files(directories['logs'], "linkedin_scraper_*.log", MAX_HISTORY_FILES)
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

        # Print comprehensive summary
        storage_stats = stats['storage_stats']
        print_summary(stats, performance_summary, final_memory_mb, storage_stats)
        
        save_pipeline_status("SUCCESS", {
            "execution_time_seconds": duration,
            "jobs_scraped": len(new_jobs),
            "success_rate_percent": round(success_rate, 1),
            "total_cumulative_jobs": len(df_combined),
            "parquet_files_created": parquet_success,
            "performance_summary": performance_summary['summary_stats'] if performance_summary.get('summary_stats') else {},
            "memory_growth_mb": end_memory.get('process_rss_mb', 0) - start_memory.get('process_rss_mb', 0),
            "total_storage_mb": storage_stats['total_storage_mb']
        }, directories['json'])
        
        return True

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        save_pipeline_status("INTERRUPTED", {"reason": "User interruption"}, directories['json'])
        return False
    except Exception as e:
        logger.error(f"Unexpected pipeline error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        save_pipeline_status("FAILED", {
            "reason": "Unexpected error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }, directories['json'])
        return False

if __name__ == "__main__":
    try:
        print("🚀 Starting LinkedIn Scraper Pipeline - Modular Version")
        print(f"📅 Execution time: {datetime.now()}")
        
        success = main()
        
        if success:
            print("🎉 Pipeline completed successfully!")
            print("✅ Exiting with success")
        else:
            print("💥 Pipeline failed!")
            print("❌ Exiting with failure")
            
    except Exception as e:
        print(f"🆘 Fatal error: {e}")
        traceback.print_exc()
        raise