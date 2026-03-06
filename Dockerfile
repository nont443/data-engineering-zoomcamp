FROM mcr.microsoft.com/devcontainers/python:3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre \
    git \
    curl \
    unzip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create a generic folder and download the GCS connector JAR there
RUN mkdir -p /opt/spark-jars && \
    curl -L -o /opt/spark-jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

USER vscode