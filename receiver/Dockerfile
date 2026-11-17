FROM python:3.13-slim

RUN pip install --no-cache-dir "connexion[swagger-ui,uvicorn]" aiokafka rfc3339-validator

WORKDIR /app
COPY . /app

EXPOSE 8080
CMD ["uvicorn", "receiver:app", "--host", "0.0.0.0", "--port", "8080"]