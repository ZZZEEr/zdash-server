FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ZDASH_DB_PATH=/app/data/zdash.db \
    ZDASH_LOG_LEVEL=INFO

WORKDIR /app

COPY server/requirements.txt /app/server/requirements.txt
RUN pip install --no-cache-dir -r server/requirements.txt

COPY server/ /app/server/

RUN mkdir -p /app/data

EXPOSE 8850

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8850/api/v1/health')" || exit 1

CMD ["python", "-m", "uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8850", "--workers", "1", "--log-level", "info"]
