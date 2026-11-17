FROM python:3.13-slim

RUN pip install --no-cache-dir "connexion[swagger-ui,uvicorn]" sqlalchemy pymysql cryptography rfc3339-validator

WORKDIR /app
COPY . /app

EXPOSE 8090
CMD ["uvicorn", "storage:app", "--host", "0.0.0.0", "--port", "8090"]