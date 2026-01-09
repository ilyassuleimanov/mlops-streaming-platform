FROM python:3.9-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /mlflow_app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train_model.py .

# Порт по умолчанию для трекинг-сервера
EXPOSE 5000
# Порт для выкатки модели, который мы будем использовать
EXPOSE 6000