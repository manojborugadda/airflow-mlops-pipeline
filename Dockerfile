FROM apache/airflow:2.8.1
COPY requirement.txt .
RUN pip install -r requirement.txt