"""
Streaming Inference Consumer для MLOps платформы.

Сервис читает запросы на предсказания из Kafka, получает признаки из Redis Feature Store,
выполняет инференс с помощью модели из MLflow и сохраняет результаты в Redis.
"""

import json
import logging
import os
import time
from typing import Any, Optional

import mlflow.sklearn
import pandas as pd
import redis
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = "logistic_regression_movielens"
KAFKA_TOPIC = "inference_queue"

# Параметры повторных попыток
MODEL_LOAD_RETRY_SEC = 10
KAFKA_CONNECT_RETRY_SEC = 5

# Подключение к Redis
# DB 0: Feature Store
r_features = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
# DB 2: Predictions Store
r_predictions = redis.Redis(host=REDIS_HOST, port=6379, db=2, decode_responses=True)


def load_model() -> Optional[Any]:
    """
    Загружает модель из MLflow Model Registry.
    
    Returns:
        Загруженная sklearn-модель или None в случае ошибки.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}/latest"
    logger.info(f"Загрузка модели из {model_uri}...")
    try:
        model = mlflow.sklearn.load_model(model_uri)
        logger.info("Модель успешно загружена.")
        return model
    except Exception as e:
        logger.error(f"Ошибка загрузки модели: {e}")
        return None


def process_message(model: Any, message: ConsumerRecord) -> None:
    """
    Обрабатывает одно сообщение из Kafka.
    
    Args:
        model: Загруженная sklearn-модель для предсказаний.
        message: Сообщение из Kafka с request_id и feature_key.
    """
    try:
        # 1. Парсинг сообщения
        msg_value = json.loads(message.value)
        request_id = msg_value.get("request_id")
        feature_key = msg_value.get("feature_key")

        if not request_id or not feature_key:
            logger.warning(f"Некорректный формат сообщения: {msg_value}")
            return

        logger.info(f"Обработка запроса {request_id} для пользователя {feature_key}")

        # 2. Получение признаков из Redis (Feature Store)
        features_json = r_features.get(feature_key)
        if not features_json:
            logger.warning(f"Признаки не найдены для ключа {feature_key}")
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
            logger.info(f"Предсказание для {request_id}: {result}")
        except Exception as e:
            logger.error(f"Ошибка предсказания: {e}")
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
        logger.info(f"Результат сохранён в Redis: {result_key}")

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


def main() -> None:
    """Главная функция запуска сервиса стримингового инференса."""
    logger.info("Запуск Streaming Inference Service...")
    
    # Загружаем модель с повторными попытками
    model = None
    while model is None:
        model = load_model()
        if model is None:
            logger.info(f"Повторная попытка загрузки модели через {MODEL_LOAD_RETRY_SEC} сек...")
            time.sleep(MODEL_LOAD_RETRY_SEC)

    # Подключаемся к Kafka с повторными попытками
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='inference_group',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            logger.info("Подключено к Kafka.")
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka: {e}. Повтор через {KAFKA_CONNECT_RETRY_SEC} сек...")
            time.sleep(KAFKA_CONNECT_RETRY_SEC)

    logger.info(f"Ожидание сообщений в топике '{KAFKA_TOPIC}'...")
    
    for message in consumer:
        process_message(model, message)


if __name__ == "__main__":
    main()
