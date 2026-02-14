FROM python:3.11-slim

WORKDIR /app

# Install uv (fast pip replacement)
RUN pip install --no-cache-dir uv

# Install deps (copy first for better Docker cache)
COPY docker/requirements.txt /app/requirements.txt
RUN uv pip install --system -r /app/requirements.txt

# Copy scripts into the image
COPY scripts/ /app/scripts/

# Default command (you can override in docker compose)
ENTRYPOINT ["python", "/app/scripts/download_general_payments.py"]
CMD ["--help"]
