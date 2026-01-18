"""
Скрипт обучения и валидации модели для MLOps платформы.

Читает признаки из MinIO, обучает LogisticRegression, валидирует на данных из Redis Feature Store,
логирует метрики и артефакты в MLflow, регистрирует модель в Model Registry.
"""

import json
import logging
import os
from typing import Dict, List, Tuple

import mlflow
import mlflow.sklearn
import pandas as pd
import redis
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
FEATURE_COLS = ['avg_rating', 'num_movies']
TARGET_COL = 'target'
EXPERIMENT_NAME = "MovieLens Recommender"
MODEL_NAME = "logistic_regression_movielens"


def get_storage_options() -> Dict[str, any]:
    """
    Возвращает параметры подключения к S3/MinIO для pandas.
    
    Returns:
        Словарь с ключами key, secret и client_kwargs для boto3.
    """
    return {
        "key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {"endpoint_url": os.environ.get("MLFLOW_S3_ENDPOINT_URL")}
    }


def setup_mlflow() -> None:
    """Настраивает подключение к MLflow Tracking Server."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    logger.info(f"MLflow URI: {tracking_uri}")
    logger.info(f"Эксперимент: {EXPERIMENT_NAME}")
    
    # Проверка переменных окружения
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        logger.warning("AWS_ACCESS_KEY_ID не найден в переменных окружения!")
    
    if not os.environ.get("MLFLOW_S3_ENDPOINT_URL"):
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
    
    logger.info(f"S3 Endpoint: {os.environ.get('MLFLOW_S3_ENDPOINT_URL')}")


def connect_redis() -> redis.Redis:
    """
    Устанавливает подключение к Redis.
    
    Returns:
        Клиент Redis.
        
    Raises:
        redis.exceptions.ConnectionError: При ошибке подключения.
    """
    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))
    
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    redis_client.ping()
    logger.info(f"Подключение к Redis ({redis_host}:{redis_port}) успешно установлено.")
    
    return redis_client


def load_training_data(storage_options: Dict) -> pd.DataFrame:
    """
    Загружает обучающие данные из MinIO.
    
    Args:
        storage_options: Параметры подключения к S3.
        
    Returns:
        DataFrame с обучающими данными.
    """
    train_data_path = "s3://feature-store/train_features.parquet/"
    logger.info(f"Чтение обучающих данных из {train_data_path}...")
    
    train_df = pd.read_parquet(train_data_path, storage_options=storage_options)
    logger.info(f"Обучающие данные успешно загружены. Размер: {train_df.shape}")
    
    return train_df


def validate_on_redis(
    model: LogisticRegression,
    redis_client: redis.Redis,
    storage_options: Dict
) -> Tuple[List[int], List[int]]:
    """
    Валидирует модель, получая признаки из Redis Feature Store.
    
    Args:
        model: Обученная модель.
        redis_client: Клиент Redis.
        storage_options: Параметры подключения к S3.
        
    Returns:
        Кортеж (y_true, y_pred) для расчёта метрик.
    """
    test_data_path = "s3://feature-store/test_features.parquet/"
    
    test_df = pd.read_parquet(test_data_path, storage_options=storage_options)
    validation_data = test_df[['userId', TARGET_COL]].drop_duplicates()
    logger.info(f"Тестовые данные загружены. Пользователей для проверки: {len(validation_data)}")
    
    y_true = []
    y_pred = []
    missing_in_redis = 0
    
    for _, row in validation_data.iterrows():
        user_id = str(row['userId'])
        features_json = redis_client.get(user_id)
        
        if features_json:
            features_dict = json.loads(features_json)
            feature_vector = [features_dict[feat] for feat in FEATURE_COLS]
            prediction = model.predict([feature_vector])[0]
            y_pred.append(prediction)
            y_true.append(row[TARGET_COL])
        else:
            missing_in_redis += 1
    
    if missing_in_redis > 0:
        logger.warning(f"{missing_in_redis} пользователей не найдено в Redis!")
    
    return y_true, y_pred


def calculate_metrics(y_true: List[int], y_pred: List[int]) -> Dict[str, float]:
    """
    Рассчитывает метрики классификации.
    
    Args:
        y_true: Истинные значения.
        y_pred: Предсказанные значения.
        
    Returns:
        Словарь с метриками.
    """
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "roc_auc": roc_auc_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1_score": f1_score(y_true, y_pred, zero_division=0)
    }


def main() -> None:
    """Главная функция обучения, валидации и регистрации модели."""
    logger.info("=" * 30)
    logger.info("Начало скрипта обучения модели...")
    logger.info("=" * 30)

    # 1. Настройка подключений
    setup_mlflow()
    storage_options = get_storage_options()
    
    try:
        redis_client = connect_redis()
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        return

    # 2. Загрузка данных
    logger.info("--- Фаза обучения ---")
    try:
        train_df = load_training_data(storage_options)
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при чтении train данных: {e}")
        raise

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[TARGET_COL]

    # 3. Запуск MLflow Run
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        logger.info(f"MLflow run начат. ID: {run_id}")

        # Обучение модели
        model = LogisticRegression(random_state=42)
        logger.info("Обучение модели LogisticRegression...")
        model.fit(X_train, y_train)
        logger.info("Модель обучена.")

        # Логирование параметров
        mlflow.log_params(model.get_params())

        # 4. Валидация на Redis
        logger.info("--- Фаза валидации ---")
        try:
            y_true, y_pred = validate_on_redis(model, redis_client, storage_options)
        except Exception as e:
            logger.error(f"Ошибка валидации: {e}")
            raise

        # Расчёт и логирование метрик
        metrics = calculate_metrics(y_true, y_pred)
        logger.info(f"Метрики: {metrics}")
        mlflow.log_metrics(metrics)

        # 5. Сохранение модели
        logger.info("Сохранение модели в MLflow (S3)...")
        try:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=MODEL_NAME
            )
            logger.info("Модель успешно сохранена.")
        except Exception as e:
            logger.error(f"ОШИБКА сохранения модели в MLflow: {e}")
            raise

    logger.info("=" * 30)
    logger.info(f"Скрипт завершен успешно. Run ID: {run_id}")
    logger.info("=" * 30)


if __name__ == "__main__":
    main()