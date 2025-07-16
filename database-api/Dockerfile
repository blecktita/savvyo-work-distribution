FROM python:3.11-slim-bookworm

# 1. Create a non-root user early
RUN useradd --create-home --shell /bin/bash --uid 1000 apiuser

# 2. Set working directory (owned by apiuser)
WORKDIR /home/apiuser/app
RUN chown apiuser:apiuser ./

# 3. Copy and install dependencies
#   - Copy only requirements.txt first (cacheable layer)
#   - Install system packages + Python packages
COPY --chown=apiuser:apiuser requirements.txt .
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libpq-dev curl \
 && pip install --no-cache-dir -r requirements.txt \
 && apt-get purge -y --auto-remove gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# 4. Copy application code
COPY --chown=apiuser:apiuser app.py .

# 5. Switch to non-root
USER apiuser

# 6. Expose and healthcheck
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

# 7. Launch with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "app:app"]
