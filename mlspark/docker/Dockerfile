FROM apache/spark:3.5.0

USER root
WORKDIR /opt

# Install Python deps (avoid version conflicts with Spark Python 3.8)
RUN pip install --no-cache-dir \
    pymongo>=4.6.0 \
    google-cloud-storage>=2.16.0 \
    numpy==1.24.4 \
    grpcio==1.58.0

# Copy code into /opt/mlspark/mlspark so imports mlspark.* work
COPY mlspark /opt/mlspark/mlspark
# Include GCS connector jar (already present in repo under spark/jars)
COPY spark/jars/gcs-connector-hadoop3-latest.jar /opt/spark/jars/

ENV PYTHONPATH="/opt/mlspark:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"

# Entrypoint is provided by spark-submit in k8s manifests
USER spark
CMD ["bash"]
