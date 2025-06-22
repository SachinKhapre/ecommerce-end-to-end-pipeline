#docker/Dockerfile
FROM bitnami/spark:3.4.1

USER root

#Hadoop AWS dependency
ENV SPARK_JARS_PACKAGES=org.apache.hadoop:hadoop-aws:3.3.2

#Working directory
WORKDIR /app

RUN install_packages python3-pip python3-dev

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt