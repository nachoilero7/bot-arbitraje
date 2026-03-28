FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY config/ config/
COPY src/ src/
COPY main.py .

# Runtime data directories
RUN mkdir -p data logs

# Non-root user
RUN useradd -m -u 1000 polyedge && chown -R polyedge:polyedge /app
USER polyedge

CMD ["python", "main.py"]
