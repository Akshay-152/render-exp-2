FROM python:3.11-slim

# Install system dependencies including ffmpeg and CA certificates.
# ca-certificates is REQUIRED for TLS to YouTube/Cloudinary/Firestore —
# the slim image does not include it by default.
RUN apt-get update && apt-get install -y \
    ffmpeg \
    nodejs \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# Upgrade yt-dlp to the latest release at build time so the container always
# has the newest anti-bot workarounds (works around the unpinned requirement).
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir --upgrade yt-dlp

COPY . .

ENV PORT=10000
EXPOSE 10000

# Run as a non-root user for better security/container hygiene.
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# gthread workers with generous threads so status polling stays snappy
# while background upload threads run. Long-running work happens in daemon
# threads inside the app, so gunicorn's worker timeout only guards the HTTP
# layer — requests return immediately now.
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT:-10000} \
    --worker-class gthread --workers 1 --threads 16 \
    --timeout 0 --keep-alive 5 --graceful-timeout 30 \
    --access-logfile - --error-logfile - \
    main:app"]

