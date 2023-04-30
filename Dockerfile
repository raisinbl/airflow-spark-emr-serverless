# To build from this file: docker build -t apache/airflow:2.5.3 .
FROM apache/airflow
USER airflow

# make sure you've configured your AWS credentials
# COPY .aws /home/airflow/.aws
COPY airflow.cfg /opt/airflow/airflow.cfg