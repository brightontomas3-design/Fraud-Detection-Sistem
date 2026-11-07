# Image aplikasi: Python 3.11 + Java 17 (default-jre Debian bookworm) + PySpark (Spark lokal di dalam container) + konektor Kafka
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    IVY_HOME=/opt/ivy \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 -r requirements.txt

# unduh konektor Spark-Kafka saat build (bukan saat demo) -> tersimpan di /opt/ivy
RUN python -c "from pyspark.sql import SparkSession; \
s=SparkSession.builder.master('local[1]').config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').config('spark.jars.ivy','/opt/ivy').getOrCreate(); s.stop()"

COPY . .
