# FROM apache/spark:3.5.0

# USER root

# # Install Java dependencies
# RUN apt-get update && \
#     apt-get install -y wget && \
#     rm -rf /var/lib/apt/lists/*

# # Copy application JAR
# COPY target/stream-kafka-spark-1-jar-with-dependencies.jar /opt/spark-app/app.jar

# # Copy GCS connector
# COPY libs/gcs-connector-hadoop3-2.2.18-shaded.jar /opt/spark/jars/

# # Copy .env file (sẽ override bằng ConfigMap)
# COPY .env /opt/spark-app/.env

# # Create checkpoint directory
# RUN mkdir -p /opt/spark-app/checkpoint && \
#     chown -R 1001:1001 /opt/spark-app

# # Switch back to spark user
# USER 1001

# WORKDIR /opt/spark-app

# # Command will be overridden in K8s deployment
# CMD ["spark-submit", \
#      "--class", "tn.insat.tp3.SparkStructuredStreamingCrypto", \
#      "--master", "local[*]", \
#      "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.mongodb.spark:mongo-spark-connector_2.13:10.2.1", \
#      "/opt/spark-app/app.jar"]

FROM apache/spark:3.5.0

USER root

# Create directories
RUN mkdir -p /opt/spark-app/checkpoint && \
    mkdir -p /opt/spark/jars && \
    chmod -R 777 /opt/spark-app

# Copy application JAR
# COPY target/stream-kafka-spark-1-jar-with-dependencies.jar /opt/spark-app/app.jar
COPY target/stream-kafka-spark-1.jar /opt/spark-app/app.jar

# Copy GCS connector JAR
COPY libs/gcs-connector-hadoop3-2.2.18-shaded.jar /opt/spark/jars/

WORKDIR /opt/spark-app

# Default command (will be overridden in K8s)
CMD ["/opt/spark/bin/spark-submit", \
     "--class", "tn.insat.tp3.SparkStructuredStreamingCrypto", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1", \
     "--conf", "spark.executor.memory=2g", \
     "--conf", "spark.driver.memory=2g", \
     "/opt/spark-app/app.jar"]