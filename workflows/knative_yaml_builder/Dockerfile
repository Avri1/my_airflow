FROM apache/airflow:2.8.0
RUN pip install opencv-python-headless
COPY knative_yaml_builder/yaml_builder.py /opt/airflow/yaml_builder.py
ENTRYPOINT ["python3", "/opt/airflow/yaml_builder.py"]
