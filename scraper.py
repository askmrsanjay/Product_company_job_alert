import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
import time
from datetime import datetime
import logging
import re
from urllib.parse import quote

# ----------------- Logging Setup -----------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
# -------------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


class LinkedInScraper:
    def __init__(self, keywords, locations):
        """
        keywords: string
        locations: string or list of strings
        """
        self.keywords = keywords
        if isinstance(locations, str):
            self.locations = [locations]
        else:
            self.locations = locations

        logger.info(f"Initialized scraper for keywords: '{self.keywords}' and locations: {self.locations}")

    def get_job_ids(self, base_url):
        """Extract job IDs from search result page for a given URL"""
        logger.info(f"Fetching job IDs from URL: {base_url}")
        response = requests.get(base_url, headers=HEADERS)
        logger.debug(f"Response status code: {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        jobs = soup.find_all("li")
        logger.debug(f"Found {len(jobs)} job list items in HTML")

        job_ids = []
        for job in jobs:
            try:
                base_card_div = job.find("div", {"class": "base-card"})
                job_id = base_card_div.get("data-entity-urn").split(":")[3]
                job_ids.append(job_id)
                logger.debug(f"Extracted Job ID: {job_id}")
            except Exception as e:
                logger.warning(f"Failed to extract job id from job element: {e}")
                continue

        logger.info(f"Total job IDs extracted: {len(job_ids)}")
        return job_ids

    def extract_responsibilities_and_skills(self, soup):
        """Extract responsibilities, skills, and raw description"""
        logger.debug("Extracting responsibilities and skills...")
        description_div = soup.select_one("div.show-more-less-html__markup")
        responsibilities, skills, raw_description = [], [], None

        if description_div:
            raw_description = description_div.get_text(" ", strip=True)
            current_section = None

            for elem in description_div.children:
                if elem.name == "p":
                    text = elem.get_text(strip=True)
                    if "responsibilit" in text.lower():
                        current_section = "responsibilities"
                        logger.debug("Found Responsibilities section")
                    elif "skill" in text.lower():
                        current_section = "skills"
                        logger.debug("Found Skills section")

                elif elem.name == "ul":
                    bullets = [li.get_text(strip=True) for li in elem.find_all("li")]
                    if current_section == "responsibilities":
                        responsibilities.extend(bullets)
                        logger.debug(f"Responsibilities bullets: {bullets}")
                    elif current_section == "skills":
                        skills.extend(bullets)
                        logger.debug(f"Skills bullets: {bullets}")

        if not responsibilities and not skills and raw_description:
            sentences = [s.strip() for s in re.split(r"[.!?]", raw_description) if s.strip()]
            responsibilities = sentences
            logger.debug("No bullets found, used fallback sentence split")

        return responsibilities, skills, raw_description

    def extract_city_from_description(self, soup):
        """Extract city and full location from job description"""
        location_div = soup.select_one("div.show-more-less-html_markup")
        if location_div:
            text = location_div.get_text(strip=True)
            match = re.search(r'location\s*:\s*"?(.*?)"?$', text, re.IGNORECASE)
            if match:
                full_location = match.group(1).strip()
                city = full_location.split(",")[0].strip()
                return city, full_location
        return None, None

    def get_job_details(self, job_id, location):
        """Extract job details from job posting page"""
        logger.info(f"Fetching details for Job ID: {job_id} at location: {location}")
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        response = requests.get(job_url, headers=HEADERS)
        logger.debug(f"Response status code for job {job_id}: {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        job_post = {}

        def safe_extract(selector, multiple=False):
            try:
                if multiple:
                    values = [s.text.strip() for s in soup.select(selector)]
                    logger.debug(f"Extracted multiple values for {selector}: {values}")
                    return values
                value = soup.select_one(selector).text.strip()
                logger.debug(f"Extracted value for {selector}: {value}")
                return value
            except Exception as e:
                logger.warning(f"Failed to extract {selector}: {e}")
                return None

        # Base job info
        job_post["job_id"] = job_id
        job_post["job_title"] = safe_extract("h2.top-card-layout__title")
        job_post["company_name"] = safe_extract("a.topcard__org-name-link")
        job_post["time_posted"] = safe_extract("span.posted-time-ago__text")
        job_post["num_applicants"] = safe_extract("span.num-applicants__caption")
        job_post["country"] = location

        # Extract city and full location
        city, full_location = self.extract_city_from_description(soup)
        job_post["city"] = city
        job_post["full_location"] = full_location

        # Criteria
        criteria = safe_extract("span.description__job-criteria-text", multiple=True)
        if criteria and len(criteria) >= 4:
            job_post["seniority_level"] = criteria[0]
            job_post["employment_type"] = criteria[1]
            job_post["job_function"] = criteria[2]
            job_post["industries"] = criteria[3]
        else:
            job_post["seniority_level"] = None
            job_post["employment_type"] = None
            job_post["job_function"] = None
            job_post["industries"] = None

        # Responsibilities & skills
        responsibilities, skills, raw_desc = self.extract_responsibilities_and_skills(soup)
        job_post["responsibilities"] = responsibilities
        job_post["skills"] = skills
        job_post["raw_description"] = raw_desc
        
        # ✅ Add source timestamp
        job_post["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Job details extracted for {job_id}")
        return job_post

    def scrape_jobs(self):
        """Main method to scrape jobs for all locations"""
        logger.info("Starting job scraping process for all locations...")
        all_jobs = []

        for loc in self.locations:
            loc_encoded = quote(loc)
            keywords_encoded = quote(self.keywords)
            base_url = f"https://www.linkedin.com/jobs/search?keywords={keywords_encoded}&location={loc_encoded}&position=1&pageNum=0"
            logger.info(f"Scraping jobs for location: {loc} | URL: {base_url}")

            job_ids = self.get_job_ids(base_url)

            for job_id in job_ids:
                details = self.get_job_details(job_id, loc)
                all_jobs.append(details)
                logger.info(f"Appended job details for {job_id} at location {loc}")
                time.sleep(random.uniform(1, 3))

        logger.info(f"Scraping complete. Total jobs scraped across all locations: {len(all_jobs)}")
        return all_jobs
