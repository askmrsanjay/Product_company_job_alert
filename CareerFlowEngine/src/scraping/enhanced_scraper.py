"""
Enhanced LinkedIn Scraper with performance tracking
"""
import time
import logging
from scraping.scraper import LinkedInScraper

logger = logging.getLogger("linkedin_scraper")

class TimedLinkedInScraper(LinkedInScraper):
    """Enhanced scraper with detailed timing"""
    
    def __init__(self, keywords, locations, delay_range=(5, 10), max_retries=3, perf_tracker=None):
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
            if self.perf_tracker:
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
                    if self.perf_tracker:
                        self.perf_tracker.end_location_tracking(loc, 0, 0)
                    continue
                
                # Limit jobs per location
                job_ids = job_ids[:max_jobs_per_location]
                logger.info(f"🔄 Processing {len(job_ids)} jobs for location: {loc}")
                
                for job_idx, job_id in enumerate(job_ids):
                    logger.info(f"  📋 Processing job {job_idx + 1}/{len(job_ids)} (ID: {job_id})")
                    
                    # Start job timing
                    if self.perf_tracker:
                        self.perf_tracker.start_job_tracking(job_id, loc)
                    job_start_time = time.time()
                    
                    try:
                        details = self.get_job_details(job_id, loc)
                        job_duration = time.time() - job_start_time
                        
                        if details:
                            location_jobs.append(details)
                            if self.perf_tracker:
                                self.perf_tracker.end_job_tracking(job_id, loc, success=True)
                            logger.info(f"    ✓ Success (ID: {job_id}) - {job_duration:.2f}s")
                        else:
                            failed_jobs.append({"job_id": job_id, "location": loc, "reason": "extraction_failed"})
                            location_failed.append(job_id)
                            if self.perf_tracker:
                                self.perf_tracker.end_job_tracking(job_id, loc, success=False)
                            logger.warning(f"    ✗ Failed (ID: {job_id}) - {job_duration:.2f}s")
                        
                    except Exception as e:
                        job_duration = time.time() - job_start_time
                        error_msg = str(e)
                        logger.error(f"    💥 Error processing job ID {job_id}: {error_msg}")
                        failed_jobs.append({"job_id": job_id, "location": loc, "reason": error_msg})
                        location_failed.append(job_id)
                        if self.perf_tracker:
                            self.perf_tracker.end_job_tracking(job_id, loc, success=False)
                    
                    # Smart delay between job requests
                    if job_idx < len(job_ids) - 1:
                        self._smart_delay()
                
                # End location timing
                location_duration = time.time() - location_start_time
                if self.perf_tracker:
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
                if self.perf_tracker:
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
        if self.perf_tracker and self.perf_tracker.location_times:
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