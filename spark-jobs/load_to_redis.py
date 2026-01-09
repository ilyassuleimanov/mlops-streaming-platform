# spark-jobs/load_to_redis.py

import os
import json
import redis
from pyspark.sql import SparkSession

def load_partition_to_redis(partition):
    """
    Функция, которая будет выполняться на каждом воркере для загрузки своей партиции данных.
    Эта функция остается БЕЗ ИЗМЕНЕНИЙ.
    """
    # Создаем одно подключение к Redis на всю партицию, а не на каждую строку.
    redis_client = redis.StrictRedis(host='redis', port=6379, decode_responses=True)
    
    for row in partition:
        user_id = str(row['userId'])

        genre_profile_data = row['genre_profile']
        genre_profile_dict = {} # Создаем пустой словарь по умолчанию
        if genre_profile_data is not None:
            # Заполняем словарь, только если данные не None
            genre_profile_dict = {g['genre']: float(g['proportion']) for g in genre_profile_data}

        # Формируем JSON, как того требует задание.
        # Структура genre_profile (массив структур) идеально подходит для этого.
        features = {
            'user_id': int(user_id),
            'avg_rating': float(row['avg_rating']),
            'num_movies': int(row['num_movies']),
            'genre_profile': genre_profile_dict,
            'last_interaction_ts': int(row['last_interaction_ts']),
            'movie_ids': [int(m) for m in row['movie_ids']]
        }

        # Сериализуем в JSON-строку
        features_json = json.dumps(features)

        # Сохраняем в Redis
        redis_client.set(user_id, features_json)

def main():
    """
    Основная функция для загрузки признаков из Parquet в Redis.
    """
    minio_access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    # 1. Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("Load Features to Redis") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark сессия успешно создана.")

    ### ИЗМЕНЕНИЕ 1: Обновляем путь к данным ###
    # Теперь читаем тестовые признаки, которые будем использовать для валидации
    input_path = "s3a://feature-store/test_features.parquet"

    # 3. Чтение признаков из Parquet
    features_df = spark.read.parquet(input_path)
    print(f"Признаки успешно загружены из {input_path}.")

    ### ИЗМЕНЕНИЕ 2: Убираем дубликаты пользователей ###
    # Так как в test_features.parquet признаки дублируются для каждой оценки пользователя,
    # нам нужно оставить только уникальные строки по userId перед загрузкой в Redis.
    # Выбираем только те колонки, которые нужны для сохранения в Redis.
    user_features_df = features_df.select(
        "userId", 
        "avg_rating", 
        "num_movies", 
        "genre_profile", 
        "last_interaction_ts", 
        "movie_ids"
    ).dropDuplicates(['userId'])
    
    print(f"Количество уникальных пользователей для загрузки в Redis: {user_features_df.count()}")

    # 4. Применяем функцию загрузки к каждой партиции данных
    # Используем новый, очищенный от дубликатов DataFrame
    user_features_df.foreachPartition(load_partition_to_redis)
    print("Признаки успешно загружены в Redis.")

    # Завершаем сессию Spark
    spark.stop()

if __name__ == "__main__":
    main()