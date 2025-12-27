"""
Configuration settings for LinkedIn Scraper Pipeline
"""
from datetime import datetime

# Pipeline Configuration
MAX_HISTORY_FILES = 15
MAX_JOBS_PER_LOCATION = 5
RETRY_ATTEMPTS = 3
DELAY_RANGE = (6, 12)

# Search Parameters
KEYWORDS = "Data Engineer OR Data Analyst OR Machine Learning Engineer OR Data Scientist OR Python Developer OR Software Engineer OR Business Intelligence Analyst OR Analytics Engineer OR AI Engineer OR Data Architect OR Visualization Engineer"

LOCATIONS = [
    "India", 
    "United States", 
    "Germany", 
    "United Kingdom", 
    "Canada",
    "Australia",
    "Netherlands", 
    "France",
    "Singapore",
    "Switzerland",
    "Sweden",
    "Ireland",
    "Japan",
    "South Africa",
    "Norway",
    "Denmark",
    "Poland",
    "Italy",
    "Mexico",
    "China",
    "Malaysia",
    "Philippines",
    "New Zealand",
    "South Korea",
    "Hong Kong",
    "Qatar",
    "Saudi Arabia",
    "Greece",
    "Portugal",
    "Austria",
    "Finland"
]

# Directory Paths
def get_directories(is_databricks=False):
    """Get directory paths based on environment"""
    if is_databricks:
        return {
            'json': "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw/json/",
            'parquet': "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw/parquet/",
            # 'logs': "/Volumes/linkedin_scraper_etl/live_linkedin/linkedin_raw_json/v2/logs/"
        }
    else:
        return {
            'json': "./json/",
            'parquet': "./parquet/",
            # 'logs': "./logs/"
        }

# Required DataFrame Columns
REQUIRED_COLUMNS = {
    'job_id': None,
    'job_title': None,
    'company_name': None,
    'country': None,
    'scraped_at': None,  # Will be set to current time
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