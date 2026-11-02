FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN useradd --create-home --uid 10001 app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY receiver.py .

USER app

EXPOSE 9000

CMD ["uvicorn", "receiver:app", "--host", "0.0.0.0", "--port", "9000"]