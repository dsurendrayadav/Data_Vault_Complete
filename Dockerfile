# DataVault Application Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system utilities needed for database diagnostics
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application codebase
COPY . .

# Expose ports: 8000 for FastAPI / Web Dashboard, 8501 for Streamlit Dashboard
EXPOSE 8000 8501

# Default startup runs the unified FastAPI operations server
CMD ["python", "app.py"]
