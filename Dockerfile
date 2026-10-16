FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY synthmon ./synthmon

RUN useradd --uid 10001 --no-create-home synthmon
USER 10001

EXPOSE 8080
ENV SYNTHMON_CONFIG=/etc/synthmon/config.yaml
CMD ["uvicorn", "synthmon.app:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8080"]
