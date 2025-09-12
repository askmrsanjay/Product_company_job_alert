import pandas as pd
import os
import logging
from datetime import datetime
from scraper import LinkedInScraper
import glob

# ----------------- Config -----------------
MAX_HISTORY_FILES = 10  # Maximum number of historical JSON/log files to keep
# ------------------------------------------

# ----------------- Logging Setup -----------------
log_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/logs/"
os.makedirs(log_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_history = os.path.join(log_dir, f"linkedin_scraper_{timestamp}.log")
log_file_latest = os.path.join(log_dir, "linkedin_scraper_latest.log")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    # Historical log
    fh = logging.FileHandler(log_file_history)
    fh.setLevel(logging.DEBUG)
    # Latest log
    fl = logging.FileHandler(log_file_latest)
    fl.setLevel(logging.DEBUG)
    # Formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    fl.setFormatter(formatter)
    # Stream handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(fl)
    logger.addHandler(ch)
# -------------------------------------------------

def cleanup_old_files(directory, pattern, max_files):
    """Keep only the latest max_files, delete older ones"""
    files = sorted(glob.glob(os.path.join(directory, pattern)), key=os.path.getmtime, reverse=True)
    for old_file in files[max_files:]:
        try:
            os.remove(old_file)
            logger.info(f"Deleted old file: {old_file}")
        except Exception as e:
            logger.warning(f"Failed to delete {old_file}: {e}")

# ----------------- Main Script -----------------
if __name__ == "__main__":
    try:
        # Parameters
        keywords = "Data Engineer OR Data Analyst OR Machine Learning Engineer"
        locations = ["India", "United States", "Germany"]

        # Create scraper
        scraper = LinkedInScraper(keywords=keywords, locations=locations)

        # Scrape jobs
        logger.info("Starting job scraping process...")
        new_jobs = scraper.scrape_jobs()
        logger.info(f"Scraping completed. Total jobs scraped this run: {len(new_jobs)}")

        # Convert new jobs to DataFrame
        df_new = pd.DataFrame(new_jobs)

        # Ensure key columns exist
        for col in ["city", "full_location", "scraped_at"]:
            if col not in df_new.columns:
                df_new[col] = None

        print(df_new.head())

        # JSON directory
        json_dir = "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/json/"
        os.makedirs(json_dir, exist_ok=True)

        # History JSON with timestamp
        json_file_history = os.path.join(json_dir, f"linkedin_jobs_{timestamp}.json")
        df_new.to_json(json_file_history, orient="records", lines=True)
        logger.info(f"Scraped jobs saved to JSON (history): {json_file_history}")

        # Latest JSON (overwritten)
        json_file_latest = os.path.join(json_dir, "linkedin_jobs_latest.json")
        df_new.to_json(json_file_latest, orient="records", lines=True)
        logger.info(f"Scraped jobs saved to JSON (latest): {json_file_latest}")

        # Incremental append to cumulative JSON
        cumulative_file = os.path.join(json_dir, "linkedin_jobs_cumulative.json")
        if os.path.exists(cumulative_file):
            df_existing = pd.read_json(cumulative_file, lines=True)
            df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=["job_id"]).reset_index(drop=True)
        else:
            df_combined = df_new
        df_combined.to_json(cumulative_file, orient="records", lines=True)
        logger.info(f"Cumulative JSON updated: {cumulative_file}. Total jobs: {len(df_combined)}")

        # ----------------- Cleanup old files -----------------
        cleanup_old_files(json_dir, "linkedin_jobs_*.json", MAX_HISTORY_FILES)
        cleanup_old_files(log_dir, "linkedin_scraper_*.log", MAX_HISTORY_FILES)

    except Exception as e:
        logger.exception(f"An error occurred during scraping: {e}")
