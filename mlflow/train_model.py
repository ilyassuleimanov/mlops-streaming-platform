# mlflow/train_model.py

import os
import json
import mlflow
import pandas as pd
import redis
import time

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score, f1_score

def main():
    """
    Основная функция для обучения, валидации и логирования модели.
    """
    print("="*30)
    print("Начало скрипта обучения модели...")
    print("="*30)

    # --- 1. Настройка подключений (Через переменные окружения!) ---
    
    # MLflow Tracking URI
    # Берем из docker-compose (там мы поставили http://127.0.0.1:5000) или дефолт
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    
    # Имя эксперимента
    experiment_name = "MovieLens Recommender"
    mlflow.set_experiment(experiment_name)
    
    print(f"MLflow URI: {tracking_uri}")
    print(f"Эксперимент: {experiment_name}")

    # Настройки S3 / Minio для boto3 (используется внутри MLflow для артефактов)
    # ВАЖНО: Мы больше не задаем их вручную, если они уже есть в среде.
    # Docker-compose передает AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY.
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        print("ВНИМАНИЕ: AWS_ACCESS_KEY_ID не найден в переменных окружения!")
    
    if not os.environ.get("MLFLOW_S3_ENDPOINT_URL"):
        # Если вдруг не задано, пробуем дефолт, но лучше задавать в docker-compose
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
    
    print(f"S3 Endpoint: {os.environ.get('MLFLOW_S3_ENDPOINT_URL')}")
    print(f"S3 Access Key: {os.environ.get('AWS_ACCESS_KEY_ID')}") # Для отладки (в проде так нельзя)

    # Redis
    try:
        redis_host = os.environ.get("REDIS_HOST", "redis")
        redis_port = int(os.environ.get("REDIS_PORT", 6379))
        redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        redis_client.ping()
        print(f"Подключение к Redis ({redis_host}:{redis_port}) успешно установлено.")
    except redis.exceptions.ConnectionError as e:
        print(f"Ошибка подключения к Redis: {e}")
        return

    # --- 2. Обучение модели ---
    
    print("\n--- Фаза обучения ---")
    
    # Путь к обучающим данным в Minio
    train_data_path = "s3://feature-store/train_features.parquet/"
    
    # Опции для pandas, чтобы он мог читать из S3
    storage_options = {
        "key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {"endpoint_url": os.environ.get("MLFLOW_S3_ENDPOINT_URL")}
    }

    try:
        print(f"Чтение обучающих данных из {train_data_path}...")
        train_df = pd.read_parquet(train_data_path, storage_options=storage_options)
        print(f"Обучающие данные успешно загружены. Размер: {train_df.shape}")
    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА при чтении train данных: {e}")
        # Важно упасть с ошибкой, чтобы Airflow это заметил
        raise e

    feature_cols = ['avg_rating', 'num_movies']
    target_col = 'target'
    
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]

    # --- 3. Запуск MLflow Run ---
    # Используем start_run без указания run_id, чтобы создать новый
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"MLflow run начат. ID: {run_id}")

        # Обучение
        model = LogisticRegression(random_state=42)
        print("Обучение модели LogisticRegression...")
        model.fit(X_train, y_train)
        print("Модель обучена.")

        # Логирование параметров
        params = model.get_params()
        mlflow.log_params(params)

        # --- 4. Валидация на Redis ---
        print("\n--- Фаза валидации ---")
        test_data_path = "s3://feature-store/test_features.parquet/"
        
        try:
            test_df = pd.read_parquet(test_data_path, storage_options=storage_options)
            # Уникальные пользователи для валидации
            validation_data = test_df[['userId', target_col]].drop_duplicates()
            print(f"Тестовые данные загружены. Пользователей для проверки: {len(validation_data)}")
        except Exception as e:
            print(f"Ошибка чтения теста: {e}")
            raise e
        
        y_true = []
        y_pred = []
        
        missing_in_redis = 0
        for _, row in validation_data.iterrows():
            user_id = str(row['userId'])
            features_json = redis_client.get(user_id)
            
            if features_json:
                features_dict = json.loads(features_json)
                feature_vector = [features_dict[feat] for feat in feature_cols]
                prediction = model.predict([feature_vector])[0]
                y_pred.append(prediction)
                y_true.append(row[target_col])
            else:
                missing_in_redis += 1
        
        if missing_in_redis > 0:
            print(f"Внимание: {missing_in_redis} пользователей не найдено в Redis!")

        # Расчет метрик
        metrics = {
            "accuracy": accuracy_score(y_true, y_pred),
            "roc_auc": roc_auc_score(y_true, y_pred),
            "precision": precision_score(y_true, y_pred, zero_division=0),
            "recall": recall_score(y_true, y_pred, zero_division=0),
            "f1_score": f1_score(y_true, y_pred, zero_division=0)
        }
        
        print(f"Метрики: {metrics}")
        mlflow.log_metrics(metrics)

        # --- 5. Сохранение модели ---
        print("Сохранение модели в MLflow (S3)...")
        try:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name="logistic_regression_movielens"
            )
            print("Модель успешно сохранена.")
        except Exception as e:
            print(f"ОШИБКА сохранения модели в MLflow: {e}")
            raise e

    print("="*30)
    print(f"Скрипт завершен успешно. Run ID: {run_id}")
    print("="*30)

if __name__ == "__main__":
    main()