# Bronze CSV -> Parquet ingestion job
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY load.py csv_ingestion_features_checklist.md README.md ./
COPY data/ ./data/
ENV DATA_DIR=/app/data

# args after the image name pass straight to the script (--force, --selftest, --prune-days N ...)
ENTRYPOINT ["python", "load.py"]
