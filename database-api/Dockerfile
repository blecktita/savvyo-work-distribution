FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app
COPY requirements.txt .
# Install system dependencies
RUN apt-get update && apt-get install -y gcc libpq-dev curl \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies

RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
# Create non-root user
RUN useradd -m -u 1000 apiuser && chown -R apiuser:apiuser /app
USER apiuser

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "app:app"]