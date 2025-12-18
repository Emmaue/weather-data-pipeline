# Start with the official Airflow image
FROM apache/airflow:2.7.1

# Switch to root to install system tools
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy your requirements.txt into the container
COPY requirements.txt /requirements.txt

# Install your Python libraries (Snowflake, etc.)
RUN pip install --no-cache-dir -r /requirements.txt