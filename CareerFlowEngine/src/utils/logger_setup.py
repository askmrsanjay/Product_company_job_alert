"""
Robust logging setup for LinkedIn Scraper Pipeline
"""
import logging
import sys
import os
from datetime import datetime

def setup_logging():
    """Production-ready logging setup that never fails"""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create logger
    logger = logging.getLogger("linkedin_scraper")
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
        print(f"⚠ Console logging failed: {e}")

    # log_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/logs/"

    # File logging with fallbacks
    # log_dirs_to_try = ["/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/logs/"
    #    "./logs/",
    #    "./",
    #    "/tmp/"
    # ]
    # log_created = False
    # Commented out file log creation
    # try:
    #     os.makedirs(log_dir, exist_ok=True)
    #
    #     # Test write access
    #     test_file = os.path.join(log_dir, f"test_{timestamp}.tmp")
    #     with open(test_file, 'w') as f:
    #         f.write("test")
    #     os.remove(test_file)
    #
    #     # Create log files
    #     log_file = os.path.join(log_dir, f"linkedin_scraper_{timestamp}.txt")
    #
    #     file_handler = logging.FileHandler(log_file, encoding='utf-8')
    #     file_handler.setLevel(logging.DEBUG)
    #     file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
    #     file_handler.setFormatter(file_formatter)
    #     logger.addHandler(file_handler)
    #
    #     print(f"✓ Log file created: {os.path.abspath(log_file)}")
    #     log_created = True
    #
    # except Exception as e:
    #     print(f"⚠ Cannot create logs in {log_dir}: {e}")
    #     print(repr(e))
    #     import traceback; traceback.print_exc()

    # if not log_created:
    #     print("⚠ File logging unavailable, using console only")

    # Test logger
    try:
        logger.info("🚀 Logging system initialized successfully")
        return logger
    except Exception as e:
        print(f"⚠ Logger test failed: {e}")
        return logging.getLogger("linkedin_scraper")

#------ New_script for testing ---------- don't touch

# from datetime import datetime
# import sys

# class UCLoggerHandler:
#     """Custom in-memory logger Handler for UC volumes"""
#     def __init__(self):
#         self.log_lines = []

#     def emit(self, record):
#         # This is called by logger when a message should be saved
#         self.log_lines.append(self.format(record))

#     def format(self, record):
#         time = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
#         return f"{time} [{record.levelname}] {record.getMessage()}"

#     def flush(self):
#         pass

# def setup_logging():
#     """Production-ready logging for Databricks Unity Catalog volumes, with Databricks API save at end"""

#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     base_volume_path = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/logs"
#     uc_log_path = f"dbfs:{base_volume_path}/linkedin_scraper_{timestamp}.txt"

#     # Create logger
#     logger = logging.getLogger("linkedin_scraper")
#     logger.setLevel(logging.INFO)

#     if logger.handlers:
#         logger.handlers.clear()

#     # Console handler for interactive output
#     console_handler = logging.StreamHandler(sys.stdout)
#     console_handler.setLevel(logging.INFO)
#     console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
#     console_handler.setFormatter(console_formatter)
#     logger.addHandler(console_handler)
#     print("✓ Console logging enabled")

#     # Custom UC Handler
#     uc_handler = UCLoggerHandler()
#     logger.addHandler(uc_handler)

#     print(f"✓ Logging will be accumulated and at the end written to: {uc_log_path}")

#     # Function to write memory log to UC volume at end
#     def save_uc_log():
#         log_content = "\n".join(uc_handler.log_lines)
#         import builtins
#         if hasattr(builtins, 'dbutils'):
#             try:
#                 dbutils.fs.put(uc_log_path, log_content, overwrite=True)
#                 print(f"✓ Log file saved to Unity Catalog volume: {uc_log_path}")
#             except Exception as e:
#                 print(f"⚠ Could not save log to Unity Catalog: {e}")
#         else:
#             print("⚠ dbutils not available – cannot save log to UC volume.")

#     # Attach method for later use
#     logger.save_uc_log = save_uc_log

#     # Test logger
#     logger.info("🚀 Logging system initialized successfully")
#     logger.info(f"Logs will be saved to: {uc_log_path}")

#     return logger

# # --- Usage example ---
# if __name__ == "__main__":
#     logger = setup_logging()
#     logger.info("Pipeline started")
#     logger.warning("This is a test warning")
#     logger.error("This is a test error")

#     # At the end of your pipeline, call:
#     logger.save_uc_log()
