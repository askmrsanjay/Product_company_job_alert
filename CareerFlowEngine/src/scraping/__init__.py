"""
Web scraping components for LinkedIn job data extraction
"""

# Import scraping classes
from .scraper import LinkedInScraper
from .enhanced_scraper import TimedLinkedInScraper

# Define public API
__all__ = [
    'LinkedInScraper',
    'TimedLinkedInScraper'
]

# Package-level configuration
DEFAULT_DELAY_RANGE = (6, 12)
DEFAULT_MAX_RETRIES = 3
