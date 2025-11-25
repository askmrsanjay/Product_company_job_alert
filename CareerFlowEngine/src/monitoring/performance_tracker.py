"""
Performance tracking for LinkedIn Scraper Pipeline
"""
import time
from datetime import datetime

class PerformanceTracker:
    """Track execution times for jobs and locations"""
    
    def __init__(self):
        self.location_times = {}
        self.job_times = {}
        self.location_start_times = {}
        self.job_start_times = {}
        self.total_start_time = None
        
    def start_total_tracking(self):
        """Start tracking total pipeline time"""
        self.total_start_time = time.time()
        
    def start_location_tracking(self, location):
        """Start tracking time for a specific location"""
        self.location_start_times[location] = time.time()
        
    def end_location_tracking(self, location, jobs_scraped, jobs_failed):
        """End tracking time for a specific location"""
        if location in self.location_start_times:
            duration = time.time() - self.location_start_times[location]
            self.location_times[location] = {
                "duration_seconds": round(duration, 2),
                "jobs_scraped": jobs_scraped,
                "jobs_failed": jobs_failed,
                "jobs_per_second": round(jobs_scraped / duration, 3) if duration > 0 else 0,
                "avg_seconds_per_job": round(duration / jobs_scraped, 2) if jobs_scraped > 0 else 0
            }
            
    def start_job_tracking(self, job_id, location):
        """Start tracking time for a specific job"""
        key = f"{location}_{job_id}"
        self.job_start_times[key] = time.time()
        
    def end_job_tracking(self, job_id, location, success=True):
        """End tracking time for a specific job"""
        key = f"{location}_{job_id}"
        if key in self.job_start_times:
            duration = time.time() - self.job_start_times[key]
            self.job_times[job_id] = {
                "job_id": job_id,
                "location": location,
                "duration_seconds": round(duration, 2),
                "success": success,
                "timestamp": datetime.now().isoformat()
            }
            
    def get_performance_summary(self):
        """Get comprehensive performance summary"""
        total_duration = time.time() - self.total_start_time if self.total_start_time else 0
        
        return {
            "total_pipeline_duration_seconds": round(total_duration, 2),
            "location_performance": self.location_times,
            "job_performance": self.job_times,
            "summary_stats": self._calculate_summary_stats()
        }
        
    def _calculate_summary_stats(self):
        """Calculate summary statistics"""
        if not self.job_times:
            return {}
            
        job_durations = [job["duration_seconds"] for job in self.job_times.values()]
        successful_jobs = [job for job in self.job_times.values() if job["success"]]
        
        return {
            "total_jobs_tracked": len(self.job_times),
            "successful_jobs": len(successful_jobs),
            "failed_jobs": len(self.job_times) - len(successful_jobs),
            "avg_job_duration_seconds": round(sum(job_durations) / len(job_durations), 2),
            "min_job_duration_seconds": round(min(job_durations), 2),
            "max_job_duration_seconds": round(max(job_durations), 2),
            "total_scraping_time_seconds": round(sum(job_durations), 2)
        }
