# FROM python:3.11-slim

# WORKDIR /app

# # Install dependencies
# RUN pip install --no-cache-dir \
#     kafka-python \
#     websocket-client

# # Copy producer script
# COPY producer/binance_producer.py .

# # Health check
# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
#     CMD python -c "import socket; socket.create_connection(('localhost', 9092), timeout=5)" || exit 1

# CMD ["python", "binance_producer.py"]

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.2 \
    websocket-client==1.6.4

# Copy producer script
COPY producer/binance_producer.py /app/

# Run producer
CMD ["python", "/app/binance_producer.py"]