# Dockerfile for Atlas REST API
# Build: docker build -t cosilico-atlas .
# Run:   docker run -p 8000:8000 -v $(pwd)/atlas.db:/app/atlas.db cosilico-atlas

FROM python:3.14-slim

WORKDIR /app

# Install system dependencies for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi>=0.109 \
    uvicorn>=0.27 \
    pydantic>=2.0 \
    lxml>=5.0 \
    sqlite-utils>=3.35

# Set Python path
ENV PYTHONPATH=/app/src

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/')" || exit 1

# Run the API
CMD ["uvicorn", "atlas.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
