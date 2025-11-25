"""
LinkedInsight Pipeline - LinkedIn Job Data Scraping and Analytics Platform
=========================================================================

A comprehensive, production-ready platform for scraping and analyzing 
LinkedIn job data with real-time performance tracking and monitoring.
"""

# Package metadata
__version__ = "2.0.0"
__title__ = "LinkedInsight Pipeline"
__description__ = "LinkedIn Job Data Scraping and Analytics Platform"
__author__ = "Your Name"
__license__ = "MIT"

# Core imports for easy access
from .scraping.enhanced_scraper import TimedLinkedInScraper
from .monitoring.performance_tracker import PerformanceTracker
from .utils.logger_setup import setup_logging

# Optional imports with error handling
try:
    from .src.scraping import TimedLinkedInScraper
    from .src.monitoring import PerformanceTracker
    from .src.utils import setup_logging
    FULL_FUNCTIONALITY = True
except ImportError as e:
    print(f"Warning: Some components not available: {e}")
    FULL_FUNCTIONALITY = False

# Define public API
__all__ = [
    'main',
    'KEYWORDS',
    'LOCATIONS', 
    'MAX_JOBS_PER_LOCATION',
    '__version__'
]

# Add conditional exports
if FULL_FUNCTIONALITY:
    __all__.extend([
        'TimedLinkedInScraper',
        'PerformanceTracker',
        'setup_logging'
    ])

# Package initialization message
import logging
logger = logging.getLogger(__name__)
logger.info(f"LinkedInsight Pipeline v{__version__} initialized")

# Configuration validation
if not KEYWORDS or not LOCATIONS:
    raise ValueError("KEYWORDS and LOCATIONS must be configured")

print(f"✅ LinkedInsight Pipeline v{__version__} ready")
print(f"📍 Configured for {len(LOCATIONS)} locations")
print(f"🔍 Targeting: {len(KEYWORDS.split(' OR '))} job types")