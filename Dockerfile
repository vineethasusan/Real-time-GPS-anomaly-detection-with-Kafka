FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONPATH=/app/src
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN useradd --create-home appuser && mkdir -p /app/data && chown -R appuser /app
USER appuser
CMD ["python", "-m", "fleetstream.service"]
