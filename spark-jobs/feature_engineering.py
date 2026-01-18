# spark-jobs/feature_engineering.py

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, collect_list, collect_set, count, explode, max, split, struct, when
)
from pyspark.sql.types import DoubleType, StringType

def main():
    """
    Основная функция для запуска Spark-задачи по инженерии признаков.
    """
    # 1. Парсинг аргументов командной строки для параметризации
    parser = argparse.ArgumentParser(description="Feature Engineering for MovieLens")
    parser.add_argument('--input_path', required=True, help='Input path for ratings data (train or test in parquet format)')
    parser.add_argument('--output_path', required=True, help='Output path for the feature-engineered data')
    args = parser.parse_args()

    # 2. Настройка SparkSession с поддержкой S3 (Minio)
    spark = SparkSession.builder \
        .appName("Feature Engineering for MovieLens") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Путь к справочнику фильмов остается постоянным
    movies_input_path = "s3a://movielens"

    print(f"Чтение данных о рейтингах из: {args.input_path}")
    print(f"Чтение данных о фильмах из: {movies_input_path}")

    # 3. Загрузка данных и создание целевой переменной
    # Читаем ratings из parquet, который был создан на шаге split_dataset
    ratings_df = spark.read.parquet(args.input_path)
    movies_df = spark.read.csv(f"{movies_input_path}/movies.csv", header=True, inferSchema=True)

    # Создаём бинарную целевую переменную (rating >= 4.0 → положительный класс)
    ratings_df = ratings_df.withColumn("target", when(col("rating") >= 4.0, 1).otherwise(0))
    
    # Очистка от пустых значений
    ratings_df = ratings_df.na.drop(subset=["userId", "movieId"])
    movies_df = movies_df.na.drop(subset=["movieId", "genres"])

    print("Данные успешно загружены, очищены, добавлена целевая переменная.")
    ratings_df.printSchema()

    # 4. Расчёт простых признаков. Используем collect_set для movie_ids
    user_simple_features_df = ratings_df.groupBy("userId").agg(
        avg("rating").alias("avg_rating"),
        count("movieId").alias("num_movies"), # count() будет быстрее, чем countDistinct, если нет дублей
        max("timestamp").alias("last_interaction_ts"),
        collect_set("movieId").alias("movie_ids") # Используем collect_set для уникальности
    )
    user_simple_features_df.cache() # Кэшируем, так как будем использовать num_movies
    print("Простые признаки рассчитаны.")

    # 5. Расчет профиля жанров
    # 5.1. Объединяем оценки с фильмами
    movie_ratings_df = ratings_df.join(movies_df, "movieId", "inner")

    # 5.2. "Взрываем" жанры
    exploded_genres_df = movie_ratings_df.withColumn("genre", explode(split(col("genres"), "\\|")))

    # 5.2.5. Фильтруем строки, где жанр является заглушкой
    exploded_genres_df = exploded_genres_df.filter(col("genre") != "(no genres listed)")

    # 5.3. Считаем количество оценок по каждому жанру для каждого пользователя
    user_genre_counts_df = exploded_genres_df.groupBy("userId", "genre").agg(
        count("movieId").alias("genre_count")
    )
    
    # 5.4. Присоединяем общее количество фильмов (num_movies) из user_simple_features_df
    genre_proportions_df = user_genre_counts_df.join(
        user_simple_features_df.select("userId", "num_movies"),
        "userId"
    )

    # 5.5. Вычисляем долю каждого жанра
    genre_proportions_df = genre_proportions_df.withColumn(
        "proportion", col("genre_count") / col("num_movies")
    )

    # 5.6. Собираем профиль жанров в структуру
    genre_profile_df = genre_proportions_df.groupBy("userId").agg(
        collect_list(
            struct(
                col("genre").cast(StringType()).alias("genre"),
                col("proportion").cast(DoubleType()).alias("proportion")
            )
        ).alias("genre_profile")
    )
    print("Профиль жанров рассчитан.")

    # 6. Объединяем все признаки в один DataFrame
    final_user_features_df = user_simple_features_df.join(genre_profile_df, "userId", "left")
    print("Все признаки по пользователям объединены.")

    # Объединяем признаки пользователя с исходными данными для обучения
    # Это создает готовый для обучения датасет, где каждая строка - это оценка,
    # обогащенная признаками пользователя, который ее поставил.
    training_ready_df = ratings_df.join(final_user_features_df, "userId", "inner")

    print("Финальный датасет для обучения/теста создан. Схема:")
    training_ready_df.printSchema()

    # 7. Сохраняем результат
    training_ready_df.write.mode("overwrite").parquet(args.output_path)
    print(f"Признаки успешно сохранены в {args.output_path}")

    spark.stop()

if __name__ == "__main__":
    main()