import pandas as pd
import os
import logging
import json
from datetime import datetime
from scraper import LinkedInScraper
import glob
import sys
import traceback
import gc
import time

# Try to import Databricks utilities
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    IS_DATABRICKS = True
    print("✓ Running in Databricks environment")
except ImportError:
    IS_DATABRICKS = False
    print("✓ Running in standard Python environment")

# Try to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠ psutil not available - memory monitoring will be limited")

# ----------------- Config -----------------
MAX_HISTORY_FILES = 10
MAX_JOBS_PER_LOCATION = 2
RETRY_ATTEMPTS = 3
DELAY_RANGE = (5, 10)
# ------------------------------------------

# ----------------- Performance Tracking Classes -----------------
class PerformanceTracker:
    """Track execution times for jobs and locations"""
    
    def __init__(self):
        self.location_times = {}
        self.job_times = {}
        self.location_start_times = {}
        self.job_start_times = {}
        self.total_start_time = None
        
    def start_total_tracking(self):
        """Start tracking total pipeline time"""
        self.total_start_time = time.time()
        
    def start_location_tracking(self, location):
        """Start tracking time for a specific location"""
        self.location_start_times[location] = time.time()
        
    def end_location_tracking(self, location, jobs_scraped, jobs_failed):
        """End tracking time for a specific location"""
        if location in self.location_start_times:
            duration = time.time() - self.location_start_times[location]
            self.location_times[location] = {
                "duration_seconds": round(duration, 2),
                "jobs_scraped": jobs_scraped,
                "jobs_failed": jobs_failed,
                "jobs_per_second": round(jobs_scraped / duration, 3) if duration > 0 else 0,
                "avg_seconds_per_job": round(duration / jobs_scraped, 2) if jobs_scraped > 0 else 0
            }
            
    def start_job_tracking(self, job_id, location):
        """Start tracking time for a specific job"""
        key = f"{location}_{job_id}"
        self.job_start_times[key] = time.time()
        
    def end_job_tracking(self, job_id, location, success=True):
        """End tracking time for a specific job"""
        key = f"{location}_{job_id}"
        if key in self.job_start_times:
            duration = time.time() - self.job_start_times[key]
            self.job_times[job_id] = {
                "job_id": job_id,
                "location": location,
                "duration_seconds": round(duration, 2),
                "success": success,
                "timestamp": datetime.now().isoformat()
            }
            
    def get_performance_summary(self):
        """Get comprehensive performance summary"""
        total_duration = time.time() - self.total_start_time if self.total_start_time else 0
        
        return {
            "total_pipeline_duration_seconds": round(total_duration, 2),
            "location_performance": self.location_times,
            "job_performance": self.job_times,
            "summary_stats": self._calculate_summary_stats()
        }
        
    def _calculate_summary_stats(self):
        """Calculate summary statistics"""
        if not self.job_times:
            return {}
            
        job_durations = [job["duration_seconds"] for job in self.job_times.values()]
        successful_jobs = [job for job in self.job_times.values() if job["success"]]
        
        return {
            "total_jobs_tracked": len(self.job_times),
            "successful_jobs": len(successful_jobs),
            "failed_jobs": len(self.job_times) - len(successful_jobs),
            "avg_job_duration_seconds": round(sum(job_durations) / len(job_durations), 2),
            "min_job_duration_seconds": round(min(job_durations), 2),
            "max_job_duration_seconds": round(max(job_durations), 2),
            "total_scraping_time_seconds": round(sum(job_durations), 2)
        }

# Initialize global performance tracker
perf_tracker = PerformanceTracker()

# ----------------- Robust Logging Setup -----------------
def setup_logging():
    """Production-ready logging setup that never fails"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Console handler (always works)
    try:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        print("✓ Console logging enabled")
    except Exception as e:
        print(f"⚠ Console logging failed (unusual): {e}")
    
    # File logging (with fallbacks, won't break if fails)
    log_created = False
    log_dirs_to_try = []
    
    if IS_DATABRICKS:
        log_dirs_to_try = [
            "/tmp/linkedin_logs/",
            "/tmp/logs/", 
            "/tmp/"
        ]
    else:
        log_dirs_to_try = [
            "/tmp/linkedin_scraper_logs/",
            "/tmp/logs/",
            "/tmp/"
        ]
    
    for log_dir in log_dirs_to_try:
        try:
            os.makedirs(log_dir, exist_ok=True)
            
            # Test write access
            test_file = os.path.join(log_dir, f"test_{timestamp}.tmp")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            
            # Create log files
            log_file = os.path.join(log_dir, f"linkedin_scraper_{timestamp}.log")
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            
            print(f"✓ Log file created: {log_file}")
            log_created = True
            break
            
        except Exception as e:
            print(f"⚠ Cannot create logs in {log_dir}: {e}")
            continue
    
    if not log_created:
        print("⚠ File logging unavailable, using console only")
    
    # Test logger
    try:
        logger.info("🚀 Logging system initialized successfully")
        return logger
    except Exception as e:
        print(f"⚠ Logger test failed: {e}")
        # Return a basic logger even if tests fail
        return logging.getLogger(__name__)

# Initialize logging
logger = setup_logging()

# ----------------- Memory & File Size Monitoring Functions -----------------
def get_memory_usage():
    """Get current memory usage statistics"""
    if not PSUTIL_AVAILABLE:
        return {
            "process_rss_mb": 0,
            "process_vms_mb": 0,
            "system_total_gb": 0,
            "system_available_gb": 0,
            "system_usage_percent": 0
        }
    
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        
        # Convert bytes to MB
        rss_mb = memory_info.rss / 1024 / 1024
        vms_mb = memory_info.vms / 1024 / 1024
        
        # System memory
        system_memory = psutil.virtual_memory()
        system_total_gb = system_memory.total / 1024 / 1024 / 1024
        system_available_gb = system_memory.available / 1024 / 1024 / 1024
        system_percent = system_memory.percent
        
        return {
            "process_rss_mb": round(rss_mb, 2),
            "process_vms_mb": round(vms_mb, 2),
            "system_total_gb": round(system_total_gb, 2),
            "system_available_gb": round(system_available_gb, 2),
            "system_usage_percent": round(system_percent, 1)
        }
    except Exception as e:
        logger.warning(f"Could not get memory usage: {e}")
        return {
            "process_rss_mb": 0,
            "process_vms_mb": 0,
            "system_total_gb": 0,
            "system_available_gb": 0,
            "system_usage_percent": 0
        }

def get_file_size(file_path):
    """Get file size in MB"""
    try:
        if os.path.exists(file_path):
            size_bytes = os.path.getsize(file_path)
            size_mb = size_bytes / 1024 / 1024
            return round(size_mb, 2)
        return 0
    except Exception as e:
        logger.warning(f"Could not get file size for {file_path}: {e}")
        return 0

def get_dataframe_memory_usage(df, name="DataFrame"):
    """Get DataFrame memory usage statistics"""
    try:
        memory_usage = df.memory_usage(deep=True)
        total_mb = memory_usage.sum() / 1024 / 1024
        
        logger.debug(f"{name} memory usage:")
        logger.debug(f"  - Shape: {df.shape}")
        logger.debug(f"  - Memory: {total_mb:.2f} MB")
        logger.debug(f"  - Memory per row: {(total_mb / len(df)):.4f} MB" if len(df) > 0 else "  - Memory per row: 0 MB")
        
        return round(total_mb, 2)
    except Exception as e:
        logger.warning(f"Could not get DataFrame memory usage: {e}")
        return 0

def log_memory_checkpoint(stage_name):
    """Log memory usage at different pipeline stages"""
    memory_stats = get_memory_usage()
    if PSUTIL_AVAILABLE:
        logger.info(f"💾 Memory checkpoint - {stage_name}:")
        logger.info(f"  - Process memory: {memory_stats['process_rss_mb']} MB")
        logger.info(f"  - System memory: {memory_stats['system_usage_percent']}% used")
        logger.info(f"  - Available: {memory_stats['system_available_gb']} GB")
    else:
        logger.debug(f"Memory checkpoint - {stage_name} (psutil not available)")
    return memory_stats

def cleanup_memory():
    """Force garbage collection to free memory"""
    try:
        collected = gc.collect()
        logger.debug(f"Garbage collector freed {collected} objects")
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")

def get_directory_size(directory_path):
    """Get total size of all files in directory"""
    try:
        if not os.path.exists(directory_path):
            return {"total_size_mb": 0, "file_count": 0, "avg_file_size_mb": 0}
        
        total_size = 0
        file_count = 0
        
        for dirpath, dirnames, filenames in os.walk(directory_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    file_size = os.path.getsize(filepath)
                    total_size += file_size
                    file_count += 1
                except Exception:
                    continue
        
        total_mb = total_size / 1024 / 1024
        return {
            "total_size_mb": round(total_mb, 2),
            "file_count": file_count,
            "avg_file_size_mb": round(total_mb / file_count, 2) if file_count > 0 else 0
        }
    except Exception as e:
        logger.warning(f"Could not calculate directory size: {e}")
        return {"total_size_mb": 0, "file_count": 0, "avg_file_size_mb": 0}

# ----------------- Enhanced LinkedInScraper Wrapper -----------------
class TimedLinkedInScraper(LinkedInScraper):
    """Enhanced scraper with detailed timing"""
    
    def __init__(self, keywords, locations, delay_range=(5, 10), max_retries=3):
        super().__init__(keywords, locations, delay_range, max_retries)
        self.perf_tracker = perf_tracker
        
    def scrape_jobs(self, max_jobs_per_location=50):
        """Enhanced scrape_jobs with detailed timing per location and job"""
        logger.info(f"Starting job scraping process for {len(self.locations)} location(s)...")
        all_jobs = []
        failed_jobs = []

        for loc_idx, loc in enumerate(self.locations):
            logger.info(f"📍 Processing location {loc_idx + 1}/{len(self.locations)}: {loc}")
            
            # Start location timing
            self.perf_tracker.start_location_tracking(loc)
            location_start_time = time.time()
            location_jobs = []
            location_failed = []
            
            try:
                loc_encoded = self._encode_location(loc)
                keywords_encoded = self._encode_keywords(self.keywords)
                base_url = f"https://www.linkedin.com/jobs/search?keywords={keywords_encoded}&location={loc_encoded}&position=1&pageNum=0"
                
                job_ids = self.get_job_ids(base_url)
                
                if not job_ids:
                    logger.warning(f"No job IDs found for location: {loc}")
                    self.perf_tracker.end_location_tracking(loc, 0, 0)
                    continue
                
                # Limit jobs per location
                job_ids = job_ids[:max_jobs_per_location]
                logger.info(f"🔄 Processing {len(job_ids)} jobs for location: {loc}")
                
                for job_idx, job_id in enumerate(job_ids):
                    logger.info(f"  📋 Processing job {job_idx + 1}/{len(job_ids)} (ID: {job_id})")
                    
                    # Start job timing
                    self.perf_tracker.start_job_tracking(job_id, loc)
                    job_start_time = time.time()
                    
                    try:
                        details = self.get_job_details(job_id, loc)
                        job_duration = time.time() - job_start_time
                        
                        if details:
                            location_jobs.append(details)
                            self.perf_tracker.end_job_tracking(job_id, loc, success=True)
                            logger.info(f"    ✓ Success (ID: {job_id}) - {job_duration:.2f}s")
                        else:
                            failed_jobs.append({"job_id": job_id, "location": loc, "reason": "extraction_failed"})
                            location_failed.append(job_id)
                            self.perf_tracker.end_job_tracking(job_id, loc, success=False)
                            logger.warning(f"    ✗ Failed (ID: {job_id}) - {job_duration:.2f}s")
                        
                    except Exception as e:
                        job_duration = time.time() - job_start_time
                        error_msg = str(e)
                        logger.error(f"    💥 Error processing job ID {job_id}: {error_msg}")
                        failed_jobs.append({"job_id": job_id, "location": loc, "reason": error_msg})
                        location_failed.append(job_id)
                        self.perf_tracker.end_job_tracking(job_id, loc, success=False)
                    
                    # Smart delay between job requests
                    if job_idx < len(job_ids) - 1:
                        self._smart_delay()
                
                # End location timing
                location_duration = time.time() - location_start_time
                self.perf_tracker.end_location_tracking(loc, len(location_jobs), len(location_failed))
                
                logger.info(f"📊 Location {loc} completed:")
                logger.info(f"  - Duration: {location_duration:.2f} seconds")
                logger.info(f"  - Jobs scraped: {len(location_jobs)}")
                logger.info(f"  - Jobs failed: {len(location_failed)}")
                if (len(location_jobs) + len(location_failed)) > 0:
                    success_rate = len(location_jobs)/(len(location_jobs)+len(location_failed))*100
                    logger.info(f"  - Success rate: {success_rate:.1f}%")
                logger.info(f"  - Avg time per job: {location_duration/len(job_ids):.2f}s")
                
                all_jobs.extend(location_jobs)
                
            except Exception as e:
                logger.error(f"Error processing location {loc}: {e}")
                self.perf_tracker.end_location_tracking(loc, 0, 1)
                continue
            
            # Longer delay between locations
            if loc_idx < len(self.locations) - 1:
                longer_delay = 15.0
                logger.info(f"⏳ Waiting {longer_delay}s before processing next location...")
                time.sleep(longer_delay)

        # Summary
        logger.info("=" * 80)
        logger.info("SCRAPING PERFORMANCE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"📋 Total jobs successfully scraped: {len(all_jobs)}")
        logger.info(f"❌ Total jobs failed: {len(failed_jobs)}")
        
        if all_jobs or failed_jobs:
            success_rate = (len(all_jobs) / (len(all_jobs) + len(failed_jobs)) * 100)
            logger.info(f"📊 Overall success rate: {success_rate:.1f}%")

        # Performance per location summary
        logger.info("📍 Performance by location:")
        for loc, perf in self.perf_tracker.location_times.items():
            logger.info(f"  {loc}: {perf['duration_seconds']}s, {perf['jobs_scraped']} jobs, {perf['avg_seconds_per_job']}s/job")
        
        logger.info("=" * 80)

        return all_jobs, failed_jobs
    
    def _encode_location(self, location):
        """Encode location for URL"""
        from urllib.parse import quote
        return quote(location)
    
    def _encode_keywords(self, keywords):
        """Encode keywords for URL"""
        from urllib.parse import quote
        return quote(keywords)

# ----------------- Enhanced Functions -----------------
def validate_environment():
    """Validate environment and dependencies"""
    logger.info("🔍 Validating environment...")
    issues = []
    
    # Memory check first
    initial_memory = log_memory_checkpoint("Environment Validation")
    
    # Check psutil
    if not PSUTIL_AVAILABLE:
        issues.append("psutil not available - memory monitoring will be limited")
    else:
        logger.info("✓ psutil available for memory monitoring")
    
    # Check core imports
    try:
        import requests
        import pandas as pd
        from bs4 import BeautifulSoup
        logger.info("✓ Core packages available")
    except ImportError as e:
        issues.append(f"Missing packages: {e}")
    
    # Check PyArrow for parquet support
    try:
        import pyarrow
        logger.info("✓ PyArrow available for parquet support")
    except ImportError as e:
        issues.append(f"PyArrow not available: {e} - parquet files will be skipped")
    
    # Check network connectivity
    try:
        import requests
        response = requests.get("https://www.google.com", timeout=10)
        if response.status_code == 200:
            logger.info("✓ Network connectivity confirmed")
        else:
            issues.append(f"Network issue: HTTP {response.status_code}")
    except Exception as e:
        issues.append(f"Network connectivity failed: {e}")
    
    return issues

def cleanup_old_files(directory, pattern, max_files):
    """Keep only the latest max_files, delete older ones"""
    try:
        if not os.path.exists(directory):
            logger.warning(f"Directory does not exist: {directory}")
            return
        
        files = sorted(glob.glob(os.path.join(directory, pattern)), key=os.path.getmtime, reverse=True)
        
        if len(files) <= max_files:
            logger.info(f"No cleanup needed. Found {len(files)} files, limit is {max_files}")
            return
        
        deleted_count = 0
        for old_file in files[max_files:]:
            try:
                os.remove(old_file)
                deleted_count += 1
                logger.debug(f"Deleted old file: {os.path.basename(old_file)}")
            except Exception as e:
                logger.warning(f"Failed to delete {old_file}: {e}")
        
        logger.info(f"Cleanup completed: deleted {deleted_count}/{len(files) - max_files} old files")
                
    except Exception as e:
        logger.error(f"Cleanup operation failed: {e}")

def save_pipeline_status(status, details=None):
    """Save pipeline status for monitoring"""
    try:
        if IS_DATABRICKS:
            json_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/json/"
        else:
            json_dir = "/tmp/linkedin_scraper_etl/json/"
            
        # Ensure directory exists
        os.makedirs(json_dir, exist_ok=True)
        status_file = os.path.join(json_dir, "pipeline_status.json")
        
        status_info = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": details or {}
        }
        
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(status_info, f, indent=2, ensure_ascii=False)
        
        # Print status for Databricks console
        if status == "SUCCESS":
            print(f"🟢 PIPELINE SUCCESS: {details}")
        elif status == "RUNNING":
            print(f"🟡 PIPELINE RUNNING: {details}")
        else:
            print(f"🔴 PIPELINE {status}: {details}")
            
    except Exception as e:
        logger.warning(f"Could not save pipeline status: {e}")

def ensure_required_columns(df):
    """Ensure DataFrame has all required columns with default values"""
    required_columns = {
        'job_id': None,
        'job_title': None,
        'company_name': None,
        'country': None,
        'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'scraper_version': '2.0',
        'seniority_level': None,
        'employment_type': None,
        'job_function': None,
        'industries': None,
        'responsibilities': [],
        'skills': [],
        'raw_description': None,
        'time_posted': None,
        'num_applicants': None
    }
    
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

def save_parquet_files(df, parquet_dir, timestamp, file_sizes_dict=None):
    """Save DataFrame as parquet files with proper data type handling"""
    if file_sizes_dict is None:
        file_sizes_dict = {}
    
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
        
        # Convert all columns to strings to avoid type conflicts
        for col in df_parquet.columns:
            df_parquet[col] = df_parquet[col].astype(str)
        
        # Define explicit schema
        schema_fields = []
        for col in df_parquet.columns:
            schema_fields.append(pa.field(col, pa.string()))
        schema = pa.schema(schema_fields)
        
        # Convert DataFrame to PyArrow Table with explicit schema
        table = pa.Table.from_pandas(df_parquet, schema=schema)
        
        # Historical parquet with timestamp
        parquet_file_history = os.path.join(parquet_dir, f"linkedin_jobs_{timestamp}.parquet")
        pq.write_table(table, parquet_file_history, compression='snappy')
        file_sizes_dict['history_parquet_mb'] = get_file_size(parquet_file_history)
        logger.info(f"✓ Saved historical parquet: {os.path.basename(parquet_file_history)} ({file_sizes_dict['history_parquet_mb']} MB)")
        
        # Latest parquet (overwritten)
        parquet_file_latest = os.path.join(parquet_dir, "linkedin_jobs_latest.parquet")
        pq.write_table(table, parquet_file_latest, compression='snappy')
        file_sizes_dict['latest_parquet_mb'] = get_file_size(parquet_file_latest)
        logger.info(f"✓ Saved latest parquet: {os.path.basename(parquet_file_latest)} ({file_sizes_dict['latest_parquet_mb']} MB)")
        
        # Cumulative parquet
        parquet_cumulative = os.path.join(parquet_dir, "linkedin_jobs_cumulative.parquet")
        if os.path.exists(parquet_cumulative):
            old_size = get_file_size(parquet_cumulative)
            try:
                # Read existing parquet
                existing_table = pq.read_table(parquet_cumulative)
                df_existing = existing_table.to_pandas()
                
                # Combine with new data
                df_combined = pd.concat([df_existing, df_parquet], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["job_id"], keep='last').reset_index(drop=True)
                
                # Convert to table and save
                combined_table = pa.Table.from_pandas(df_combined, schema=schema)
                pq.write_table(combined_table, parquet_cumulative, compression='snappy')
                
                file_sizes_dict['cumulative_parquet_mb'] = get_file_size(parquet_cumulative)
                logger.info(f"✓ Updated cumulative parquet: {os.path.basename(parquet_cumulative)}")
                logger.info(f"  - Size change: {old_size} MB → {file_sizes_dict['cumulative_parquet_mb']} MB")
                logger.info(f"  - Total parquet records: {len(df_combined)}")
                
            except Exception as e:
                logger.error(f"Failed to update cumulative parquet: {e}")
                pq.write_table(table, parquet_cumulative, compression='snappy')
                file_sizes_dict['cumulative_parquet_mb'] = get_file_size(parquet_cumulative)
                logger.info(f"✓ Created new cumulative parquet: {os.path.basename(parquet_cumulative)} ({file_sizes_dict['cumulative_parquet_mb']} MB)")
        else:
            pq.write_table(table, parquet_cumulative, compression='snappy')
            file_sizes_dict['cumulative_parquet_mb'] = get_file_size(parquet_cumulative)
            logger.info(f"✓ Created new cumulative parquet: {os.path.basename(parquet_cumulative)} ({file_sizes_dict['cumulative_parquet_mb']} MB)")
        
        parquet_saved = True
        logger.info("🎉 All parquet files created successfully!")
        
    except ImportError:
        logger.warning("⚠ PyArrow not available, skipping parquet file creation")
        logger.info("To enable parquet support, install: pip install pyarrow")
    except Exception as e:
        logger.error(f"Failed to save parquet files: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.debug(traceback.format_exc())
    
    return parquet_saved

def save_performance_data(performance_summary, json_dir, timestamp):
    """Save detailed performance data"""
    try:
        # Save detailed performance data
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

# ----------------- Main Script with Complete Monitoring -----------------
def main():
    """Main execution function with comprehensive performance tracking"""
    
    start_time = datetime.now()
    perf_tracker.start_total_tracking()
    start_memory = log_memory_checkpoint("Pipeline Start")
    save_pipeline_status("RUNNING", {"start_time": start_time.isoformat()})
    
    try:
        logger.info("=" * 80)
        logger.info("LINKEDIN SCRAPING PIPELINE - WITH PERFORMANCE TRACKING")
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
                save_pipeline_status("FAILED", {"reason": "Environment validation failed", "issues": env_issues})
                return False
            else:
                logger.info("Non-critical issues found, continuing...")
        
        # Parameters
        keywords = "Data Engineer OR Data Analyst OR Machine Learning Engineer OR Data Scientist OR Python Developer OR Software Engineer OR Business Intelligence Analyst OR Analytics Engineer OR AI Engineer OR Visualisation Engineer"
        locations = ["India", "United States", "Germany", "United Kingdom", "Canada","Australia", "Netherlands", "France", "Singapore", "Switzerland"]  # Top 10 first
        
        logger.info(f"Keywords: {keywords}")
        logger.info(f"Locations: {locations}")
        logger.info(f"Max jobs per location: {MAX_JOBS_PER_LOCATION}")

        # Create enhanced scraper
        try:
            scraper = TimedLinkedInScraper(
                keywords=keywords, 
                locations=locations,
                delay_range=DELAY_RANGE,
                max_retries=RETRY_ATTEMPTS
            )
            logger.info("✓ Enhanced scraper initialized successfully")
        except Exception as e:
            logger.error(f"Scraper initialization failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Scraper initialization failed", "error": str(e)})
            return False

        # Scrape jobs with detailed performance tracking
        logger.info("🔄 Starting job scraping process with performance tracking...")
        pre_scraping_memory = log_memory_checkpoint("Before Scraping")
        
        try:
            new_jobs, failed_jobs = scraper.scrape_jobs(max_jobs_per_location=MAX_JOBS_PER_LOCATION)
            post_scraping_memory = log_memory_checkpoint("After Scraping")
            
            # Get comprehensive performance summary
            performance_summary = perf_tracker.get_performance_summary()
            
            logger.info(f"📊 Scraping performance:")
            logger.info(f"  - Total jobs scraped: {len(new_jobs)}")
            logger.info(f"  - Total jobs failed: {len(failed_jobs)}")
            logger.info(f"  - Total scraping time: {performance_summary['summary_stats'].get('total_scraping_time_seconds', 0)}s")
            logger.info(f"  - Average time per job: {performance_summary['summary_stats'].get('avg_job_duration_seconds', 0)}s")
            
        except Exception as e:
            logger.error(f"Job scraping failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Job scraping failed", "error": str(e)})
            return False

        # Validate results
        if not new_jobs:
            logger.error("No jobs scraped successfully")
            if failed_jobs:
                logger.error("Sample failures:")
                for i, failure in enumerate(failed_jobs[:3]):
                    logger.error(f"  {i+1}. {failure}")
            save_pipeline_status("FAILED", {"reason": "No jobs scraped", "failed_count": len(failed_jobs)})
            return False

        # Convert to DataFrame with memory monitoring
        try:
            logger.info("🔄 Processing scraped data...")
            df_new = pd.DataFrame(new_jobs)
            df_new = ensure_required_columns(df_new)
            
            # Track DataFrame memory usage
            df_memory_mb = get_dataframe_memory_usage(df_new, "New Jobs DataFrame")
            
            if not validate_dataframe(df_new):
                logger.error("DataFrame validation failed")
                save_pipeline_status("FAILED", {"reason": "DataFrame validation failed"})
                return False
            
            logger.info(f"✓ Data processing completed. DataFrame shape: {df_new.shape}")
            post_processing_memory = log_memory_checkpoint("After Data Processing")
            
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Data processing failed", "error": str(e)})
            return False

        # Display sample data
        try:
            logger.info("Sample scraped data:")
            sample_columns = ['job_title', 'company_name', 'country', 'scraped_at']
            available_columns = [col for col in sample_columns if col in df_new.columns]
            print(df_new[available_columns].head())
        except Exception as e:
            logger.warning(f"Could not display sample data: {e}")

        # Set up directories
        try:
            if IS_DATABRICKS:
                json_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/json/"
                parquet_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/parquet/"
                log_dir = "/tmp/linkedin_logs/"  # Use /tmp for logs in Databricks
            else:
                json_dir = "/tmp/linkedin_scraper_etl/json/"
                parquet_dir = "/tmp/linkedin_scraper_etl/parquet/"
                log_dir = "/tmp/linkedin_scraper_logs/"
                
            os.makedirs(json_dir, exist_ok=True)
            os.makedirs(parquet_dir, exist_ok=True)
            logger.info(f"✓ JSON directory ready: {json_dir}")
            logger.info(f"✓ Parquet directory ready: {parquet_dir}")
            
        except Exception as e:
            logger.error(f"Directory setup failed: {e}")
            save_pipeline_status("FAILED", {"reason": "Directory setup failed", "error": str(e)})
            return False

        # Save files with size monitoring
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_sizes = {}
            
            logger.info("💾 Saving JSON files...")
            
            # Historical JSON with timestamp
            json_file_history = os.path.join(json_dir, f"linkedin_jobs_{timestamp}.json")
            df_new.to_json(json_file_history, orient="records", lines=True, force_ascii=False)
            file_sizes['history_json_mb'] = get_file_size(json_file_history)
            logger.info(f"✓ Saved historical JSON: {os.path.basename(json_file_history)} ({file_sizes['history_json_mb']} MB)")

            # Latest JSON (overwritten)
            json_file_latest = os.path.join(json_dir, "linkedin_jobs_latest.json")
            df_new.to_json(json_file_latest, orient="records", lines=True, force_ascii=False)
            file_sizes['latest_json_mb'] = get_file_size(json_file_latest)
            logger.info(f"✓ Saved latest JSON: {os.path.basename(json_file_latest)} ({file_sizes['latest_json_mb']} MB)")

            # Enhanced cumulative JSON with detailed duplicate analysis
            cumulative_file = os.path.join(json_dir, "linkedin_jobs_cumulative.json")
            if os.path.exists(cumulative_file):
                old_cumulative_size = get_file_size(cumulative_file)
                try:
                    df_existing = pd.read_json(cumulative_file, lines=True)
                    existing_memory_mb = get_dataframe_memory_usage(df_existing, "Existing Cumulative DataFrame")
                    
                    initial_count = len(df_existing)
                    logger.info(f"Loading existing cumulative data:")
                    logger.info(f"  - Existing jobs: {initial_count}")
                    logger.info(f"  - Existing file size: {old_cumulative_size} MB")
                    logger.info(f"  - New jobs scraped: {len(df_new)}")
                    
                    # Enhanced job ID analysis
                    existing_job_ids = set(df_existing['job_id'].astype(str))
                    new_job_ids = set(df_new['job_id'].astype(str))
                    overlapping_ids = existing_job_ids.intersection(new_job_ids)
                    
                    logger.info(f"Job ID analysis:")
                    logger.info(f"  - Existing unique job_ids: {len(existing_job_ids)}")
                    logger.info(f"  - New unique job_ids: {len(new_job_ids)}")
                    logger.info(f"  - Overlapping job_ids: {len(overlapping_ids)}")
                    logger.info(f"  - Truly new job_ids: {len(new_job_ids) - len(overlapping_ids)}")
                    
                    if overlapping_ids:
                        sample_overlapping = list(overlapping_ids)[:3]
                        logger.info(f"  - Sample overlapping IDs: {sample_overlapping}")
                    
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
                    
                    # Enhanced calculations
                    duplicates_removed = before_dedup_count - final_count
                    net_change = final_count - initial_count
                    actual_new_unique_jobs = len(new_job_ids) - len(overlapping_ids)
                    
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
                        "duplicate_rate": len(overlapping_ids) / len(df_new) * 100 if len(df_new) > 0 else 0,
                        "deduplication_efficiency": duplicates_removed / before_dedup_count * 100 if before_dedup_count > 0 else 0
                    }
                    
                    logger.info(f"📈 Data quality metrics:")
                    logger.info(f"  - Duplicate rate in new data: {quality_metrics['duplicate_rate']:.1f}%")
                    logger.info(f"  - Deduplication efficiency: {quality_metrics['deduplication_efficiency']:.1f}%")
                    
                    # Clean up
                    del df_existing, duplicate_jobs
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
            df_combined.to_json(cumulative_file, orient="records", lines=True, force_ascii=False)
            file_sizes['cumulative_json_mb'] = get_file_size(cumulative_file)
            logger.info(f"✓ Updated cumulative JSON: {os.path.basename(cumulative_file)} ({file_sizes['cumulative_json_mb']} MB)")
            
            # Track final DataFrame memory
            final_memory_mb = get_dataframe_memory_usage(df_combined, "Final Combined DataFrame")
            
            # Save parquet files
            parquet_success = save_parquet_files(df_combined, parquet_dir, timestamp, file_sizes)
            
            # Save performance data
            save_performance_data(performance_summary, json_dir, timestamp)
            
            # Directory size summary
            json_dir_stats = get_directory_size(json_dir)
            parquet_dir_stats = get_directory_size(parquet_dir)
            
            logger.info(f"📊 Storage summary:")
            logger.info(f"  - JSON directory: {json_dir_stats['total_size_mb']} MB ({json_dir_stats['file_count']} files)")
            logger.info(f"  - Parquet directory: {parquet_dir_stats['total_size_mb']} MB ({parquet_dir_stats['file_count']} files)")
            logger.info(f"  - Total storage: {json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb']} MB")
            
            post_saving_memory = log_memory_checkpoint("After File Saving")
            
        except Exception as e:
            logger.error(f"File saving failed: {e}")
            save_pipeline_status("FAILED", {"reason": "File saving failed", "error": str(e)})
            return False

        # Enhanced statistics with performance data
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
                
                # Enhanced performance metrics
                "performance_metrics": performance_summary,
                
                # Data quality metrics
                "data_quality_metrics": quality_metrics,
                
                # Memory statistics
                "memory_stats": {
                    "start_memory_mb": start_memory.get('process_rss_mb', 0),
                    "end_memory_mb": end_memory.get('process_rss_mb', 0),
                    "memory_growth_mb": end_memory.get('process_rss_mb', 0) - start_memory.get('process_rss_mb', 0),
                    "dataframe_memory_mb": final_memory_mb,
                    "system_memory_usage_percent": end_memory.get('system_usage_percent', 0),
                    "psutil_available": PSUTIL_AVAILABLE
                },
                
                # File size statistics
                "file_sizes": file_sizes,
                "storage_stats": {
                    "json_directory_mb": json_dir_stats['total_size_mb'],
                    "parquet_directory_mb": parquet_dir_stats['total_size_mb'],
                    "total_storage_mb": json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb'],
                    "json_file_count": json_dir_stats['file_count'],
                    "parquet_file_count": parquet_dir_stats['file_count']
                }
            }
            
            stats_file = os.path.join(json_dir, "scraping_stats.json")
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Enhanced statistics with performance data saved: {os.path.basename(stats_file)}")
            
        except Exception as e:
            logger.warning(f"Could not save statistics: {e}")

        # Cleanup old files
        try:
            logger.info("🧹 Starting cleanup of old files...")
            cleanup_old_files(json_dir, "linkedin_jobs_*.json", MAX_HISTORY_FILES)
            cleanup_old_files(json_dir, "performance_data_*.json", MAX_HISTORY_FILES)
            cleanup_old_files(parquet_dir, "linkedin_jobs_*.parquet", MAX_HISTORY_FILES)
            cleanup_old_files(log_dir, "linkedin_scraper_*.log", MAX_HISTORY_FILES)
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

        # Enhanced final summary with performance details
        logger.info("=" * 80)
        logger.info("COMPREHENSIVE PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"✅ Total execution time: {duration:.1f} seconds")
        logger.info(f"✅ Jobs scraped this run: {len(new_jobs)}")
        logger.info(f"✅ Jobs failed: {len(failed_jobs)}")
        logger.info(f"✅ Success rate: {success_rate:.1f}%")
        logger.info(f"✅ Total cumulative jobs: {len(df_combined)}")
        logger.info(f"✅ Net new jobs added: {new_jobs_count}")
        logger.info(f"✅ JSON files created: ✓")
        logger.info(f"✅ Parquet files created: {'✓' if parquet_success else '✗'}")
        
        # Performance summary
        if performance_summary.get('summary_stats'):
            perf_stats = performance_summary['summary_stats']
            logger.info(f"⚡ Average job scraping time: {perf_stats.get('avg_job_duration_seconds', 0)}s")
            logger.info(f"⚡ Fastest job: {perf_stats.get('min_job_duration_seconds', 0)}s")
            logger.info(f"⚡ Slowest job: {perf_stats.get('max_job_duration_seconds', 0)}s")
            logger.info(f"⚡ Total scraping time: {perf_stats.get('total_scraping_time_seconds', 0)}s")
        
        # Location performance summary
        if performance_summary.get('location_performance'):
            logger.info("📍 Performance by location:")
            for loc, perf in performance_summary['location_performance'].items():
                logger.info(f"  - {loc}: {perf['duration_seconds']}s ({perf['jobs_scraped']} jobs, {perf['avg_seconds_per_job']}s/job)")
        
        # Memory and storage summary
        if PSUTIL_AVAILABLE:
            memory_growth = end_memory.get('process_rss_mb', 0) - start_memory.get('process_rss_mb', 0)
            logger.info(f"💾 Memory usage: {end_memory.get('process_rss_mb', 0)} MB (growth: {memory_growth:+.1f} MB)")
            logger.info(f"💾 DataFrame memory: {final_memory_mb} MB")
        logger.info(f"📁 Total storage: {json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb']:.1f} MB")
        logger.info(f"📁 Cumulative file: {file_sizes.get('cumulative_json_mb', 0)} MB")
        logger.info("=" * 80)
        
        save_pipeline_status("SUCCESS", {
            "execution_time_seconds": duration,
            "jobs_scraped": len(new_jobs),
            "success_rate_percent": round(success_rate, 1),
            "total_cumulative_jobs": len(df_combined),
            "parquet_files_created": parquet_success,
            "performance_summary": performance_summary['summary_stats'] if performance_summary.get('summary_stats') else {},
            "memory_growth_mb": end_memory.get('process_rss_mb', 0) - start_memory.get('process_rss_mb', 0),
            "total_storage_mb": json_dir_stats['total_size_mb'] + parquet_dir_stats['total_size_mb']
        })
        
        return True

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        save_pipeline_status("INTERRUPTED", {"reason": "User interruption"})
        return False
    except Exception as e:
        logger.error(f"Unexpected pipeline error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        save_pipeline_status("FAILED", {
            "reason": "Unexpected error",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return False

if __name__ == "__main__":
    try:
        print("🚀 Starting LinkedIn Scraper Pipeline with Performance Tracking")
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