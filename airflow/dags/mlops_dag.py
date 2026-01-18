# airflow/dags/mlops_dag.py

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- Переменные ---

# Учетные данные Minio. Airflow получит их из переменных окружения.
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# URL и локальные пути для датасета.
# Для отладки используем small, для сдачи задания нужно будет поменять на ml-latest.zip
DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
ZIP_PATH = "/tmp/ml-latest-small.zip"
EXTRACT_PATH = "/tmp/ml-latest-small" 
MINIO_BUCKET_RAW = "movielens"

# --- Переменные проекта ---
MINIO_BUCKET_ML_DATA = "ml-data"       # Бакет для train/test выборок
MINIO_BUCKET_FEATURES = "feature-store"  # Бакет для посчитанных признаков
MINIO_BUCKET_MLFLOW = "mlflow"         # Бакет для артефактов MLflow

# Параметры Spark
SPARK_MASTER_URL = "spark://spark-master:7077"
SPLIT_DATASET_JOB_PATH = "/opt/airflow/spark-jobs/split_dataset.py"
FEATURE_ENGINEERING_JOB_PATH = "/opt/airflow/spark-jobs/feature_engineering.py"
LOAD_TO_REDIS_JOB_PATH = "/opt/airflow/spark-jobs/load_to_redis.py" # Предполагаем, что этот скрипт будет адаптирован для чтения test_features

# Путь к скрипту обучения модели внутри папки airflow
TRAIN_MODEL_SCRIPT_PATH = "/opt/airflow/mlflow/train_model.py"

# Конфигурация для подключения Spark к S3/Minio, собранная в одну строку
S3_SPARK_CONFIG = (
    f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    f"--conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} "
    f"--conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} "
    f"--conf spark.hadoop.fs.s3a.path.style.access=true "
    f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
)

# --- Аргументы по умолчанию для DAG ---

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --- Определение DAG ---

with DAG(
    dag_id='mlops_platform',
    default_args=default_args,
    description='Full ML pipeline for MovieLens with model training',
    schedule_interval=None,
    catchup=False,
    tags=['mlops', 'platform'],
) as dag:

    # Задача 1: Скачать и распаковать датасет (без изменений)
    get_dataset = BashOperator(
        task_id='get_dataset',
        bash_command=(
            f"curl -o {ZIP_PATH} {DATASET_URL} && "
            f"unzip -o {ZIP_PATH} -d /tmp/"
        ),
    )

    # Задача 2: Загрузить сырые данные в Minio и создать все нужные бакеты
    put_dataset = BashOperator(
        task_id='put_dataset',
        env={
            "MINIO_ROOT_USER": MINIO_ROOT_USER,
            "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
            "MC_CONFIG_DIR": "/tmp/.mc",
            "HOME": "/tmp",
        },
        bash_command=(
            "mc alias set myminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && "
            f"mc mb myminio/{MINIO_BUCKET_RAW} --ignore-existing && "
            f"mc mb myminio/{MINIO_BUCKET_ML_DATA} --ignore-existing && "
            f"mc mb myminio/{MINIO_BUCKET_FEATURES} --ignore-existing && "
            f"mc mb myminio/{MINIO_BUCKET_MLFLOW} --ignore-existing && "
            f"mc cp --recursive {EXTRACT_PATH}/ myminio/{MINIO_BUCKET_RAW}/"
        ),
    )

    # Задача 3: Разделить датасет на train/test
    split_dataset = BashOperator(
        task_id='split_dataset',
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER_URL} "
            f"{S3_SPARK_CONFIG} "
            f"{SPLIT_DATASET_JOB_PATH}"
        ),
    )
    
    # Задача 4.1: Запустить инженерию признаков для ОБУЧАЮЩЕЙ выборки
    features_engineering_train = BashOperator(
        task_id='features_engineering_train',
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER_URL} "
            "--executor-memory 1G "
            "--driver-memory 1G "
            f"{S3_SPARK_CONFIG} "
            f"{FEATURE_ENGINEERING_JOB_PATH} "
            f"--input_path s3a://{MINIO_BUCKET_ML_DATA}/train/ratings.parquet "
            f"--output_path s3a://{MINIO_BUCKET_FEATURES}/train_features.parquet"
        ),
    )

    # Задача 4.2: Запустить инженерию признаков для ТЕСТОВОЙ выборки
    features_engineering_test = BashOperator(
        task_id='features_engineering_test',
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER_URL} "
            "--executor-memory 1G "
            "--driver-memory 1G "
            f"{S3_SPARK_CONFIG} "
            f"{FEATURE_ENGINEERING_JOB_PATH} "
            f"--input_path s3a://{MINIO_BUCKET_ML_DATA}/test/ratings.parquet "
            f"--output_path s3a://{MINIO_BUCKET_FEATURES}/test_features.parquet"
        ),
    )
    
    # Задача 5: Загрузить ТЕСТОВЫЕ признаки в Redis
    # Важно: эта задача выполняется после `features_engineering_test`
    load_features = BashOperator(
        task_id='load_features',
        bash_command=(
            f"spark-submit --master {SPARK_MASTER_URL} "
            f"{S3_SPARK_CONFIG} "
            # Предполагается, что load_to_redis.py будет изменен для чтения
            # из s3a://feature-store/test_features.parquet
            f"{LOAD_TO_REDIS_JOB_PATH}"
        ),
    )

    # Задача 6: Обучить модель
    # Запускается после того, как готовы признаки для обучения И
    # тестовые признаки загружены в Redis для валидации.
    train_and_save_model = BashOperator(
        task_id='train_and_save_model',
        bash_command=(
            # Запускаем скрипт обучения внутри контейнера mlflow
            # `mlops-mlflow` - это имя контейнера из docker-compose.yml
            "docker exec mlops-mlflow python /mlflow_app/train_model.py"
        ),
    )


    # Определение порядка выполнения задач
    get_dataset >> put_dataset >> split_dataset

    # Задачи инженерии признаков запускаются параллельно после разделения датасета
    split_dataset >> [features_engineering_train, features_engineering_test]

    # Загрузка фичей в Redis происходит после того, как тестовые фичи посчитаны
    features_engineering_test >> load_features

    # Обучение модели начинается только после того, как:
    # 1. Готовы обучающие признаки (в Minio)
    # 2. Готовы и загружены в Redis тестовые признаки (для валидации)
    [features_engineering_train, load_features] >> train_and_save_model