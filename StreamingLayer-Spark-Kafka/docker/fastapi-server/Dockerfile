# FROM python:3.11-slim

# WORKDIR /app

# # Install system dependencies
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends gcc && \
#     rm -rf /var/lib/apt/lists/*

# # Copy requirements first (for caching)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy application
# COPY server.py .
# COPY .env .

# # Create non-root user
# RUN useradd -m -u 1001 appuser && \
#     chown -R appuser:appuser /app
# USER appuser

# # Expose port
# EXPOSE 8000

# # Health check
# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
#     CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/')" || exit 1

# CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn[standard]==0.24.0 \
    pymongo==4.6.0 \
    python-dotenv==1.0.0

# Copy server file
COPY server.py /app/

# Expose port
EXPOSE 8000

# Run server
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]