FROM apache/spark:3.5.0

USER root
WORKDIR /opt

# Install native BLAS and Python deps (Spark Python 3.8 compatible)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -o Acquire::Check-Valid-Until=false || true && \
    apt-get install -y --no-install-recommends libopenblas-base liblapack3 && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    pip install --no-cache-dir \
        pymongo>=4.6.0 \
        google-cloud-storage>=2.16.0 \
        numpy==1.24.4 \
        grpcio==1.58.0 \
        importlib-metadata==6.8.0

# Copy code into /opt/mlspark/mlspark so imports mlspark.* work
COPY mlspark /opt/mlspark/mlspark
# Include GCS connector jar (already present in repo under spark/jars)
COPY spark/jars/gcs-connector-hadoop3-latest.jar /opt/spark/jars/
# Pre-download spark-bigquery jar to avoid runtime Maven fetch
RUN curl -L -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
    https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar

ENV PYTHONPATH="/opt/mlspark:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"
ENV OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 BLAS_NUM_THREADS=1

# Entrypoint is provided by spark-submit in k8s manifests
USER spark
CMD ["bash"]
