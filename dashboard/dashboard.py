import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

# 1. Load Environment Variables (Securely)
load_dotenv()

# 2. Connect to Snowflake
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

conn = init_connection()

# 3. App Title
st.title("🌦️ Live Weather Data Pipeline")
st.markdown("Fetching real-time data from **Snowflake** (Ingested via Airflow on EC2).")

# 4. Fetch Data
query = "SELECT CITY, TEMPERATURE, HUMIDITY, WEATHER_DESCRIPTION, TIMESTAMP FROM WEATHER_DATA ORDER BY TIMESTAMP DESC LIMIT 500"
df = pd.read_sql(query, conn)

# 5. Key Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Records", len(df))
col2.metric("Hottest City", df.loc[df['TEMPERATURE'].idxmax()]['CITY'], f"{df['TEMPERATURE'].max()} °C")
col3.metric("Coldest City", df.loc[df['TEMPERATURE'].idxmin()]['CITY'], f"{df['TEMPERATURE'].min()} °C")

# 6. Charts
st.subheader("Temperature Trends by City")
fig = px.line(df, x='TIMESTAMP', y='TEMPERATURE', color='CITY', title="Temperature over Time")
st.plotly_chart(fig)

st.subheader("Raw Data View")
st.dataframe(df)