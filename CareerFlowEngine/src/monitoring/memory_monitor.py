"""
Memory and file size monitoring for LinkedIn Scraper Pipeline
"""
import os
import gc
import logging

# Try to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger("linkedin_scraper")

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