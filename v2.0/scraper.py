import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
import time
from datetime import datetime
import logging
import re
from urllib.parse import quote
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class LinkedInScraper:
    def __init__(self, keywords, locations, delay_range=(3, 7), max_retries=3):
        """
        Enhanced LinkedIn scraper with anti-detection measures
        
        Args:
            keywords (str): Job search keywords
            locations (str or list): Location(s) to search
            delay_range (tuple): Min/max delay between requests in seconds
            max_retries (int): Maximum retry attempts for failed requests
        """
        self.keywords = keywords
        self.delay_range = delay_range
        self.max_retries = max_retries
        
        # Setup logger for this class
        self.logger = self._setup_class_logger()
        
        # Normalize locations to list
        if isinstance(locations, str):
            self.locations = [locations]
        else:
            self.locations = locations
        
        # Initialize session with enhanced configuration
        self.session = self._create_session()
        
        # User agents pool for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        self.logger.info(f"Initialized scraper for keywords: '{self.keywords}' and locations: {self.locations}")

    def _setup_class_logger(self):
        """Setup logger specifically for the scraper class"""
        logger = logging.getLogger(f"{__name__}.LinkedInScraper")
        logger.setLevel(logging.DEBUG)
        
        # Don't add handlers if they already exist
        if not logger.handlers:
            # Create a simple console handler for the scraper
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s [SCRAPER] %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    def _create_session(self):
        """Create session with retry strategy and enhanced headers - Version agnostic"""
        session = requests.Session()

        # Version-compatible retry strategy
        import urllib3
        try:
            # Check urllib3 version and use appropriate parameters
            if hasattr(urllib3.util.retry.Retry, '_allowed_methods'):
                # New version (1.26.0+)
                retry_strategy = Retry(
                    total=self.max_retries,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "OPTIONS"],
                    backoff_factor=1
                )
            else:
                # Old version (< 1.26.0)
                retry_strategy = Retry(
                    total=self.max_retries,
                    status_forcelist=[429, 500, 502, 503, 504],
                    method_whitelist=["HEAD", "GET", "OPTIONS"],
                    backoff_factor=1
                )
        except Exception:
            # Fallback - basic retry without method restrictions
            retry_strategy = Retry(
                total=self.max_retries,
                status_forcelist=[429, 500, 502, 503, 504],
                backoff_factor=1
            )
            self.logger.warning("Using basic retry strategy due to compatibility issues")

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Enhanced headers
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0"
        })

        return session

    # def _create_session(self):
    #     """Create session with retry strategy and enhanced headers"""
    #     session = requests.Session()
        
    #     # Retry strategy
    #     retry_strategy = Retry(
    #         total=self.max_retries,
    #         status_forcelist=[429, 500, 502, 503, 504],
    #         method_whitelist=["HEAD", "GET", "OPTIONS"],
    #         backoff_factor=1
    #     )
        
    #     adapter = HTTPAdapter(max_retries=retry_strategy)
    #     session.mount("http://", adapter)
    #     session.mount("https://", adapter)
        
    #     # Enhanced headers
    #     session.headers.update({
    #         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    #         "Accept-Language": "en-US,en;q=0.9",
    #         "Accept-Encoding": "gzip, deflate, br",
    #         "Connection": "keep-alive",
    #         "Upgrade-Insecure-Requests": "1",
    #         "Sec-Fetch-Dest": "document",
    #         "Sec-Fetch-Mode": "navigate",
    #         "Sec-Fetch-Site": "none",
    #         "Cache-Control": "max-age=0"
    #     })
        
    #     return session

    def _rotate_user_agent(self):
        """Rotate user agent for each request"""
        self.session.headers["User-Agent"] = random.choice(self.user_agents)

    def _smart_delay(self):
        """Implement smart delay with randomization"""
        delay = random.uniform(*self.delay_range)
        self.logger.debug(f"Waiting {delay:.2f} seconds before next request")
        time.sleep(delay)

    def _make_request(self, url, max_retries=None):
        """Make HTTP request with retry logic and error handling"""
        if max_retries is None:
            max_retries = self.max_retries
            
        self._rotate_user_agent()
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Making request to: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=15)
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                self.logger.debug(f"Successfully fetched URL with status: {response.status_code}")
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    self.logger.error(f"All retry attempts failed for URL: {url}")
                    return None
                
                # Exponential backoff
                wait_time = (2 ** attempt) + random.uniform(1, 3)
                self.logger.debug(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
        
        return None

    def get_job_ids(self, base_url):
        """Extract job IDs from search result page with enhanced error handling"""
        self.logger.info(f"Fetching job IDs from URL: {base_url}")
        
        response = self._make_request(base_url)
        if not response:
            self.logger.error("Failed to fetch job search results")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Multiple selectors for job IDs (LinkedIn changes these frequently)
        job_selectors = [
            "li[data-occludable-job-id]",
            "div.base-card[data-entity-urn]",
            "div[data-job-id]",
            "li.result-card"
        ]
        
        job_ids = []
        jobs_found = False
        
        for selector in job_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    jobs_found = True
                    self.logger.debug(f"Found {len(elements)} job elements using selector: {selector}")
                    
                    for element in elements:
                        try:
                            # Try multiple methods to extract job ID
                            job_id = None
                            
                            if element.get("data-occludable-job-id"):
                                job_id = element.get("data-occludable-job-id")
                            elif element.get("data-entity-urn"):
                                job_id = element.get("data-entity-urn").split(":")[-1]
                            elif element.get("data-job-id"):
                                job_id = element.get("data-job-id")
                            else:
                                # Try to find job ID in href attributes
                                job_link = element.find('a', href=True)
                                if job_link:
                                    href = job_link['href']
                                    job_id_match = re.search(r'/jobs/view/(\d+)', href)
                                    if job_id_match:
                                        job_id = job_id_match.group(1)
                            
                            if job_id and job_id.isdigit():
                                job_ids.append(job_id)
                                self.logger.debug(f"Extracted Job ID: {job_id}")
                                
                        except Exception as e:
                            self.logger.warning(f"Failed to extract job ID from element: {e}")
                            continue
                    break
                    
            except Exception as e:
                self.logger.warning(f"Selector {selector} failed: {e}")
                continue
        
        if not jobs_found:
            self.logger.warning("No job elements found. LinkedIn may have changed their HTML structure.")
            
        # Remove duplicates and validate
        job_ids = list(set(job_ids))
        self.logger.info(f"Total unique job IDs extracted: {len(job_ids)}")
        return job_ids

    def extract_responsibilities_and_skills(self, soup):
        """Enhanced extraction of responsibilities, skills, and raw description"""
        self.logger.debug("Extracting responsibilities and skills...")
        
        # Multiple selectors for description (LinkedIn changes these)
        description_selectors = [
            "div.show-more-less-html__markup",
            "div.description__text",
            "section.description",
            "div[data-module='description']"
        ]
        
        description_div = None
        for selector in description_selectors:
            description_div = soup.select_one(selector)
            if description_div:
                self.logger.debug(f"Found description using selector: {selector}")
                break
        
        responsibilities, skills, raw_description = [], [], None
        
        if description_div:
            raw_description = description_div.get_text(" ", strip=True)
            current_section = None
            
            # Enhanced section detection
            responsibility_keywords = ["responsibilit", "duties", "role", "tasks", "what you'll do"]
            skill_keywords = ["skill", "requirement", "qualifications", "experience", "competenc"]
            
            for elem in description_div.descendants:
                if hasattr(elem, 'name') and elem.name == "p":
                    text = elem.get_text(strip=True).lower()
                    
                    if any(keyword in text for keyword in responsibility_keywords):
                        current_section = "responsibilities"
                        self.logger.debug("Found Responsibilities section")
                    elif any(keyword in text for keyword in skill_keywords):
                        current_section = "skills"
                        self.logger.debug("Found Skills section")
                
                elif hasattr(elem, 'name') and elem.name == "ul":
                    bullets = [li.get_text(strip=True) for li in elem.find_all("li") if li.get_text(strip=True)]
                    if current_section == "responsibilities":
                        responsibilities.extend(bullets)
                        self.logger.debug(f"Added {len(bullets)} responsibility bullets")
                    elif current_section == "skills":
                        skills.extend(bullets)
                        self.logger.debug(f"Added {len(bullets)} skill bullets")
            
            # Fallback: use sentence splitting if no structured content found
            if not responsibilities and not skills and raw_description:
                sentences = [s.strip() for s in re.split(r"[.!?]", raw_description) if s.strip()]
                if len(sentences) > 5:  # Only if we have substantial content
                    responsibilities = sentences[:len(sentences)//2]  # First half as responsibilities
                    skills = sentences[len(sentences)//2:]  # Second half as skills
                self.logger.debug("Used fallback sentence splitting")
        
        return responsibilities, skills, raw_description

    # def extract_location_info(self, soup):
    #     """Enhanced location extraction with multiple fallback methods"""
    #     location_selectors = [
    #         "span.topcard__flavor",
    #         "span.job-criteria__text",
    #         "div.job-criteria",
    #         "span[data-test='job-location']"
    #     ]
        
    #     for selector in location_selectors:
    #         try:
    #             location_elem = soup.select_one(selector)
    #             if location_elem:
    #                 location_text = location_elem.get_text(strip=True)
    #                 if location_text and len(location_text) > 2:  # Basic validation
    #                     city = location_text.split(",")[0].strip()
    #                     return city, location_text
    #         except Exception as e:
    #             self.logger.debug(f"Location selector {selector} failed: {e}")
    #             continue
        
    #     return None, None

    def validate_job_data(self, job_post):
        """Validate essential job data fields"""
        required_fields = ['job_id', 'job_title', 'company_name']
        
        for field in required_fields:
            if not job_post.get(field) or str(job_post.get(field)).strip() == "":
                self.logger.warning(f"Missing or empty required field '{field}' for job {job_post.get('job_id', 'unknown')}")
                return False
        
        return True

    def get_job_details(self, job_id, location):
        """Enhanced job details extraction with comprehensive error handling"""
        self.logger.info(f"Fetching details for Job ID: {job_id} at location: {location}")
        
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        response = self._make_request(job_url)
        
        if not response:
            self.logger.error(f"Failed to fetch job details for ID: {job_id}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        job_post = {}

        def safe_extract(selectors, multiple=False, default=None):
            """Safe extraction with multiple selector fallbacks"""
            if isinstance(selectors, str):
                selectors = [selectors]
                
            for selector in selectors:
                try:
                    if multiple:
                        elements = soup.select(selector)
                        if elements:
                            values = [elem.get_text(strip=True) for elem in elements if elem.get_text(strip=True)]
                            if values:
                                self.logger.debug(f"Extracted multiple values for {selector}: {values}")
                                return values
                    else:
                        element = soup.select_one(selector)
                        if element:
                            value = element.get_text(strip=True)
                            if value:
                                self.logger.debug(f"Extracted value for {selector}: {value}")
                                return value
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            self.logger.debug(f"No value found for selectors {selectors}, using default: {default}")
            return default

        # Basic job information with multiple selector fallbacks
        job_post["job_id"] = job_id
        job_post["job_title"] = safe_extract([
            "h2.top-card-layout__title",
            "h1.t-24",
            ".job-title",
            "h1"
        ])
        
        job_post["company_name"] = safe_extract([
            "a.topcard__org-name-link",
            "span.topcard__flavor",
            "a[data-tracking-control-name='public_jobs_topcard-org-name']",
            ".company-name"
        ])
        
        job_post["time_posted"] = safe_extract([
            "span.posted-time-ago__text",
            "time.job-posted-date",
            ".posted-time-ago"
        ])
        
        job_post["num_applicants"] = safe_extract([
            "span.num-applicants__caption",
            ".applicant-count",
            "span[data-test='num-applicants']"
        ])
        
        job_post["country"] = location

        # Enhanced location extraction
        # city, full_location = self.extract_location_info(soup)
        # job_post["city"] = city
        # job_post["full_location"] = full_location

        # Job criteria with enhanced extraction
        criteria_selectors = [
            "span.description__job-criteria-text",
            ".job-criteria__text",
            "span.job-criteria"
        ]
        
        criteria = safe_extract(criteria_selectors, multiple=True, default=[])
        
        if criteria and len(criteria) >= 4:
            job_post["seniority_level"] = criteria[0] if len(criteria) > 0 else None
            job_post["employment_type"] = criteria[1] if len(criteria) > 1 else None
            job_post["job_function"] = criteria[2] if len(criteria) > 2 else None
            job_post["industries"] = criteria[3] if len(criteria) > 3 else None
        else:
            # Fallback: try individual extraction
            job_post["seniority_level"] = safe_extract("span[data-test='seniority-level']")
            job_post["employment_type"] = safe_extract("span[data-test='employment-type']")
            job_post["job_function"] = safe_extract("span[data-test='job-function']")
            job_post["industries"] = safe_extract("span[data-test='industries']")

        # Enhanced responsibilities & skills extraction
        responsibilities, skills, raw_desc = self.extract_responsibilities_and_skills(soup)
        job_post["responsibilities"] = responsibilities
        job_post["skills"] = skills
        job_post["raw_description"] = raw_desc
        
        # Metadata
        job_post["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_post["scraper_version"] = "2.0"

        # Validate data quality
        if not self.validate_job_data(job_post):
            self.logger.warning(f"Job data validation failed for ID: {job_id}")
            return None

        self.logger.info(f"Successfully extracted job details for ID: {job_id}")
        return job_post

    def scrape_jobs(self, max_jobs_per_location=50):
        """Enhanced main scraping method with better error handling and limits"""
        self.logger.info(f"Starting job scraping process for {len(self.locations)} location(s)...")
        all_jobs = []
        failed_jobs = []

        for loc_idx, loc in enumerate(self.locations):
            self.logger.info(f"Processing location {loc_idx + 1}/{len(self.locations)}: {loc}")
            
            try:
                loc_encoded = quote(loc)
                keywords_encoded = quote(self.keywords)
                base_url = f"https://www.linkedin.com/jobs/search?keywords={keywords_encoded}&location={loc_encoded}&position=1&pageNum=0"
                
                job_ids = self.get_job_ids(base_url)
                
                if not job_ids:
                    self.logger.warning(f"No job IDs found for location: {loc}")
                    continue
                
                # Limit jobs per location to prevent overwhelming
                job_ids = job_ids[:max_jobs_per_location]
                self.logger.info(f"Processing {len(job_ids)} jobs for location: {loc}")
                
                for job_idx, job_id in enumerate(job_ids):
                    self.logger.info(f"Processing job {job_idx + 1}/{len(job_ids)} (ID: {job_id}) for location: {loc}")
                    
                    try:
                        details = self.get_job_details(job_id, loc)
                        
                        if details:
                            all_jobs.append(details)
                            self.logger.info(f"✓ Successfully processed job ID: {job_id}")
                        else:
                            failed_jobs.append({"job_id": job_id, "location": loc, "reason": "extraction_failed"})
                            self.logger.warning(f"✗ Failed to extract details for job ID: {job_id}")
                        
                    except Exception as e:
                        self.logger.error(f"✗ Error processing job ID {job_id}: {e}")
                        failed_jobs.append({"job_id": job_id, "location": loc, "reason": str(e)})
                    
                    # Smart delay between job requests
                    if job_idx < len(job_ids) - 1:  # Don't delay after last job
                        self._smart_delay()
                
            except Exception as e:
                self.logger.error(f"Error processing location {loc}: {e}")
                continue
            
            # Longer delay between locations
            if loc_idx < len(self.locations) - 1:
                longer_delay = random.uniform(10, 20)
                self.logger.info(f"Waiting {longer_delay:.1f} seconds before processing next location...")
                time.sleep(longer_delay)

        # Summary
        self.logger.info("=" * 60)
        self.logger.info("SCRAPING SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total jobs successfully scraped: {len(all_jobs)}")
        self.logger.info(f"Total jobs failed: {len(failed_jobs)}")
        if all_jobs or failed_jobs:
            success_rate = (len(all_jobs) / (len(all_jobs) + len(failed_jobs)) * 100)
            self.logger.info(f"Success rate: {success_rate:.1f}%")
        
        if failed_jobs:
            self.logger.info("Failed jobs summary:")
            for failed in failed_jobs[:5]:  # Show first 5 failed jobs
                self.logger.info(f"  - Job ID: {failed['job_id']}, Location: {failed['location']}, Reason: {failed['reason']}")

        return all_jobs, failed_jobs
