FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python websocket-client
COPY binance_producer.py .
CMD ["python", "binance_producer.py"]
