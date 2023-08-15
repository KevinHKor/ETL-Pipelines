# Use the official Apache Airflow image as the base
FROM apache/airflow:2.4.1

USER root

# Install the Snowflake connector module
USER airflow
RUN pip install snowflake-connector-python

# Copy any additional files or scripts if needed
# COPY my_custom_script.py /path/to/destination/

USER airflow