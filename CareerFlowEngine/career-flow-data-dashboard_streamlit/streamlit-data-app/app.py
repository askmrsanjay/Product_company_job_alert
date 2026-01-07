import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.set_page_config(layout="wide")

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData():
    return sqlQuery("select * from career_flow_engine.silver.careerflow_jobs_cleansed")

data = getData()
data = data.sort_values('scraped_at', ascending=False)
data = data.drop_duplicates(subset=['job_id', 'skills'], keep='first')
st.header("CareerFlow Dashboard!!")

# Company filter
companies = data['company_name'].dropna().unique()
company_options = ["Select All"] + list(companies)
selected_companies = st.multiselect("Select company(s):", company_options, default=["Select All"])

# Filter data by selected companies for country options
if "Select All" in selected_companies:
    company_filtered = data
elif selected_companies:
    company_filtered = data[data['company_name'].isin(selected_companies)]
else:
    company_filtered = data.iloc[0:0]

# Country filter options depend on company selection
countries = company_filtered['country'].dropna().unique()
country_options = ["Select All"] + list(countries)
selected_countries = st.multiselect("Select country(s):", country_options, default=["Select All"])

# Now filter by country
if "Select All" in selected_countries:
    country_filtered = company_filtered
elif selected_countries:
    country_filtered = company_filtered[company_filtered['country'].isin(selected_countries)]
else:
    country_filtered = company_filtered.iloc[0:0]

# Skills filter options depend on both company and country selection
skills = country_filtered['skills'].dropna().unique()
skill_options = ["Select All"] + list(skills)
selected_skills = st.multiselect("Select skill(s):", skill_options, default=["Select All"])

# Now filter by skills
if "Select All" in selected_skills:
    filtered_data = country_filtered
elif selected_skills:
    filtered_data = country_filtered[country_filtered['skills'].isin(selected_skills)]
else:
    filtered_data = country_filtered.iloc[0:0]

job_counts = (
    filtered_data.groupby('skills')['job_id']
    .count()
    .reset_index()
    .rename(columns={'job_id': 'count'})
    .sort_values('count', ascending=False)
    .head(10)
)

fig = px.bar(
    job_counts,
    x='count',
    y='skills',
    orientation='h',
    title='Top 10 Skills by Job Count',
    labels={'count': 'Number of Jobs', 'skills': 'Skill'},
    height=600,
    width=700,
    text='count'
)
fig.update_traces(textposition='outside')
fig.update_layout(
    yaxis={'categoryorder':'total ascending'},
    xaxis_title="<b>Number of Jobs</b>",
    yaxis_title="<b>Skill</b>",
    title={'text': "<b>Top 10 Skills by Job Count</b>", 'font': dict(size=16, family='Arial')},
    font=dict(family='Arial', size=14)
)

st.plotly_chart(fig, use_container_width=True)

# Show only selected columns in the data table
selected_columns = ['job_id',
    'company_name', 'country', 'employment_type', 'industries', 'job_function',
    'job_title', 'seniority_level', 'time_posted_ts', 'skills', 'num_applicants_int'
]
st.dataframe(filtered_data[selected_columns], height=600, use_container_width=True)
