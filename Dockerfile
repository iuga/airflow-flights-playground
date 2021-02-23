FROM apache/airflow:2.0.1
USER root
RUN apt-get update \
    && apt-get install -y unzip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && sudo ./aws/install
USER airflow
COPY requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt
