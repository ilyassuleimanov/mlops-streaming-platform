# spark-jobs/split_dataset.py

import os
from pyspark.sql import SparkSession

def main():
    """
    Основная функция для запуска Spark-задачи.
    """
    # 1. Настройка SparkSession с поддержкой S3 (Minio)
    # Используем переменные окружения для доступа к Minio,
    # которые должны быть установлены в вашем Spark-окружении (например, в Dockerfile или entrypoint.sh)
    spark = SparkSession.builder \
        .appName("SplitMovieLensDataset") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("SparkSession успешно создана.")

    # 2. Определение путей к данным
    # Исходный бакет, предоставленный вами
    input_bucket = "movielens"
    # Бакет для хранения обработанных данных (train/test)
    output_bucket = "ml-data"
    
    input_path = f"s3a://{input_bucket}/ratings.csv"
    train_output_path = f"s3a://{output_bucket}/train/ratings.parquet"
    test_output_path = f"s3a://{output_bucket}/test/ratings.parquet"

    print(f"Чтение данных из: {input_path}")
    print(f"Путь для сохранения обучающей выборки: {train_output_path}")
    print(f"Путь для сохранения тестовой выборки: {test_output_path}")

    # 3. Чтение данных
    try:
        ratings_df = spark.read.csv(input_path, header=True, inferSchema=True)
        print("Данные успешно прочитаны. Схема:")
        ratings_df.printSchema()
        print(f"Общее количество записей: {ratings_df.count()}")
    except Exception as e:
        print(f"Ошибка при чтении данных из Minio: {e}")
        spark.stop()
        raise 

    # 4. Разделение данных на обучающую и тестовую выборки
    # 80% для обучения, 20% для теста. seed для воспроизводимости
    train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)

    print(f"Количество записей в обучающей выборке: {train_df.count()}")
    print(f"Количество записей в тестовой выборке: {test_df.count()}")

    # 5. Сохранение выборок в Minio в формате Parquet
    # Parquet - эффективный колоночный формат хранения, идеально подходящий для Spark.
    # mode("overwrite") позволяет перезапускать задачу без ошибок.
    try:
        train_df.write.mode("overwrite").parquet(train_output_path)
        print(f"Обучающая выборка успешно сохранена в {train_output_path}")

        test_df.write.mode("overwrite").parquet(test_output_path)
        print(f"Тестовая выборка успешно сохранена в {test_output_path}")
    except Exception as e:
        print(f"Ошибка при сохранении данных в Minio: {e}")

    # 6. Завершение сессии Spark
    spark.stop()
    print("Spark-задача успешно завершена.")


if __name__ == "__main__":
    main()