FROM python:3.13-slim

RUN pip install --no-cache-dir "connexion[swagger-ui,uvicorn]" httpx rfc3339-validator

WORKDIR /app
COPY . /app

EXPOSE 8100
CMD ["uvicorn", "processing:app", "--host", "0.0.0.0", "--port", "8100"]
