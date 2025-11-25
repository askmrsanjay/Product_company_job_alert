"""
Utility functions for LinkedIn Scraper Pipeline
"""
import logging

logger = logging.getLogger("linkedin_scraper")

def validate_environment():
    """Validate environment and dependencies"""
    logger.info("🔍 Validating environment...")
    issues = []
    
    # Check psutil
    try:
        import psutil
        logger.info("✓ psutil available for memory monitoring")
    except ImportError:
        issues.append("psutil not available - memory monitoring will be limited")
    
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

def print_summary(stats, performance_summary, final_memory_mb, storage_stats):
    """Print comprehensive pipeline execution summary"""
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"✅ Total execution time: {stats['execution_time_seconds']:.1f} seconds")
    logger.info(f"✅ Jobs scraped this run: {stats['jobs_scraped_this_run']}")
    logger.info(f"✅ Jobs failed: {stats['jobs_failed']}")
    logger.info(f"✅ Success rate: {stats['success_rate_percent']:.1f}%")
    logger.info(f"✅ Total cumulative jobs: {stats['total_cumulative_jobs']}")
    logger.info(f"✅ Net new jobs added: {stats['new_jobs_added']}")
    logger.info(f"✅ JSON files created: ✓")
    logger.info(f"✅ Parquet files created: {'✓' if stats['parquet_files_created'] else '✗'}")
    
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
    memory_stats = stats.get('memory_stats', {})
    if memory_stats.get('psutil_available'):
        memory_growth = memory_stats.get('memory_growth_mb', 0)
        logger.info(f"💾 Memory usage: {memory_stats.get('end_memory_mb', 0)} MB (growth: {memory_growth:+.1f} MB)")
        logger.info(f"💾 DataFrame memory: {final_memory_mb} MB")
    
    logger.info(f"📁 Total storage: {storage_stats['total_storage_mb']:.1f} MB")
    logger.info(f"📁 JSON directory: {storage_stats['json_directory_mb']} MB")
    logger.info(f"📁 Parquet directory: {storage_stats['parquet_directory_mb']} MB")
    logger.info("=" * 80)
