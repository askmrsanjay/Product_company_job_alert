# Career Flow Engine (LinkedInsight Pipeline)

**Career Flow Engine** is a comprehensive, production-ready LinkedIn job data scraping and analytics platform designed to extract, process, and analyze job market data across global markets.

## 🚀 Key Features

-   **Global Coverage**: Automated job scraping for multiple locations (India, US, Germany, etc.).
-   **Intelligent Scraping**: Enhanced scraper with random delays, user-agent rotation, and error handling to mimic human behavior.
-   **Data Integrity**: Advanced duplicate detection and rigorous data validation.
-   **Performance Monitoring**: Real-time tracking of execution times, memory usage, and success rates.
-   **Multi-Format Export**: Automatically saves data in JSON and Parquet formats for varied use cases.
-   **Interactive Dashboard**: Integrated Streamlit dashboard for visualizing job market trends and insights.
-   **Modular Architecture**: Clean separation of concerns (Scraping, Data Management, Monitoring) for easy maintenance.

## 📂 Project Structure

```
CareerFlowEngine/
├── main.py                                  # Main entry point for the scraping pipeline
├── config.py                                # Configuration settings (Keywords, Locations, Limits)
├── requirements.txt                         # Project dependencies
├── src/
│   ├── scraping/                            # Core scraping logic
│   ├── data_manage/                         # Data processing and validation
│   ├── monitoring/                          # Performance and memory tracking
│   └── utils/                               # Helper functions (logging, file I/O)
├── data/                                    # Output directory for data and logs
├── ETL_Notebooks/                           # Jupyter notebooks for data experimentation
├── Unit Test/                               # Unit tests
├── career-flow-data-dashboard_streamlit/    # Streamlit analytics dashboard
└── markdown/                                # Detailed project documentation
```

## 🛠️ Prerequisites

-   **Python 3.8+**
-   **Google Chrome** (for Selenium/WebDriver)
-   **ChromeDriver** (matching your Chrome version)

## 📦 Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/askmrsanjay/Product_company_job_alert.git
    cd Product_company_job_alert/CareerFlowEngine
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

    *Note: For the dashboard, you may need to install additional requirements found in `career-flow-data-dashboard_streamlit/streamlit-data-app/requirements.txt`.*

3.  **Environment Setup:**
    -   Ensure you have a `.env` file if required (though the current config uses direct variables).
    -   Verify internet connection for scraping.

## 🚦 Usage

### Running the Scraper Pipeline

To start the job scraping process, run the main script:

```bash
python main.py
```

This will:
1.  Initialize the scraping environment.
2.  Scrape jobs based on `config.py` settings.
3.  Process and deduplicate data.
4.  Save results to `data/json` and `data/parquet`.
5.  Generate a performance report.

### Running the Dashboard

To visualize the collected data, launch the Streamlit app:

```bash
cd career-flow-data-dashboard_streamlit/streamlit-data-app
streamlit run app.py
```

## ⚙️ Configuration

You can customize the scraping behavior in `config.py`:

-   **`KEYWORDS`**: logical OR string of job titles to search for.
-   **`LOCATIONS`**: List of countries/cities to scrape.
-   **`MAX_JOBS_PER_LOCATION`**: Limit on jobs to fetch per location.
-   **`DELAY_RANGE`**: Min/Max seconds to wait between requests (to avoid rate limiting).

## 📊 Data Output

The pipeline generates several files in the `data/` directory:

-   **`linkedin_jobs_latest.json`**: Most recent scraped jobs.
-   **`linkedin_jobs_cumulative.json`**: Aggregated dataset with history.
-   **`scraping_stats.json`**: Detailed execution statistics.
-   **`*.parquet`**: Compressed columnar data for efficient analysis.
-   **`logs/`**: Execution logs for debugging.

## 📝 Documentation

For a deep dive into the system architecture, methodology, and research background, refer to the documentation in the `markdown/` folder.

---
*Created by [Your Name]*
