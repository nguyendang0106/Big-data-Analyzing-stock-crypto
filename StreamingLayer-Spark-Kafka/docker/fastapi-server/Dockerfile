FROM python:3.11-slim

WORKDIR /app

# Cài đặt dependencies hệ thống cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements trước để tận dụng layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY server.py .

# Tạo user non-root để tăng tính bảo mật cho container
RUN useradd -m -u 1001 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Healthcheck giúp K8s biết khi nào Pod sẵn sàng nhận traffic
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/') or exit(1)"

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]