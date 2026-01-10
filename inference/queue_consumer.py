import os
import json
import time
import redis
import mlflow.sklearn
import pandas as pd
from kafka import KafkaConsumer

# Конфигурация
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = "logistic_regression_movielens"
KAFKA_TOPIC = "inference_queue"

# Подключение к Redis
# DB 0: Feature Store
r_features = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
# DB 2: Predictions Store
r_predictions = redis.Redis(host=REDIS_HOST, port=6379, db=2, decode_responses=True)

def load_model():
    """Загрузка модели из MLflow."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}/latest"
    print(f"Loading model from {model_uri}...")
    try:
        model = mlflow.sklearn.load_model(model_uri)
        print("Model loaded successfully.")
        return model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

def process_message(model, message):
    """Обработка одного сообщения из Kafka."""
    try:
        # 1. Парсинг сообщения
        msg_value = json.loads(message.value)
        request_id = msg_value.get("request_id")
        feature_key = msg_value.get("feature_key")

        if not request_id or not feature_key:
            print(f"Invalid message format: {msg_value}")
            return

        print(f"Processing request {request_id} for user {feature_key}")

        # 2. Получение признаков из Redis (Feature Store)
        features_json = r_features.get(feature_key)
        if not features_json:
            print(f"Features not found for key {feature_key}")
            return
        
        features = json.loads(features_json)
        
        # Подготовка данных для модели
        # Модель ожидает DataFrame с колонками avg_rating, num_movies
        data = {
            "avg_rating": [features["avg_rating"]],
            "num_movies": [features["num_movies"]]
        }
        df = pd.DataFrame(data)

        # 3. Предсказание
        try:
            prediction = model.predict(df)
            result = int(prediction[0])
            print(f"Prediction for {request_id}: {result}")
        except Exception as e:
            print(f"Prediction error: {e}")
            return

        # 4. Сохранение результата в Redis (Predictions Store)
        prediction_record = {
            "request_id": request_id,
            "feature_key": feature_key,
            "prediction": result,
            "processed_at": int(time.time())
        }
        
        result_key = f"prediction_{request_id}"
        r_predictions.set(result_key, json.dumps(prediction_record))
        print(f"Result saved to Redis: {result_key}")

    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print("Starting Streaming Inference Service...")
    
    # Загружаем модель
    model = None
    while model is None:
        model = load_model()
        if model is None:
            print("Retrying model load in 10 seconds...")
            time.sleep(10)

    # Подключаемся к Kafka
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest', # Corrected to earliest
                enable_auto_commit=True,
                group_id='inference_group',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("Connected to Kafka.")
        except Exception as e:
            print(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")
    
    for message in consumer:
        process_message(model, message)

if __name__ == "__main__":
    main()
