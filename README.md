# MLOps Streaming Platform

Production-grade платформа машинного обучения для рекомендательных систем, реализующая полный цикл MLOps: от обработки данных до real-time инференса.

---

## Архитектура системы

Платформа реализует гибридную архитектуру обработки данных, состоящую из трёх слоёв:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BATCH LAYER                                      │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐    ┌──────────┐  │
│  │ Airflow  │───▶│  Spark   │───▶│ Feature Engineer │───▶│  MinIO   │  │
│  │   DAG    │    │ Cluster  │    │     (PySpark)    │    │   (S3)   │  │
│  └──────────┘    └──────────┘    └──────────────────┘    └──────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐                   │
│  │  MLflow  │◀───│  Model   │    │      Redis       │                   │
│  │ Registry │    │ Training │───▶│  Feature Store   │                   │
│  └──────────┘    └──────────┘    └──────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       STREAMING LAYER                                    │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐    ┌──────────┐  │
│  │  Kafka   │───▶│ Inference│───▶│    ML Model      │───▶│  Redis   │  │
│  │  Queue   │    │ Consumer │    │   (from MLflow)  │    │Predictions│  │
│  └──────────┘    └──────────┘    └──────────────────┘    └──────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Batch Layer

- **ETL-оркестрация** через Apache Airflow
- **Распределённая обработка данных** на Apache Spark
- **Хранение данных** в S3-совместимом хранилище MinIO

### Serving Layer  

- **Управление ML-экспериментами** через MLflow Tracking
- **Model Registry** для версионирования моделей
- **Feature Store** на базе Redis для быстрого доступа к признакам

### Streaming Layer

- **Асинхронная очередь запросов** через Apache Kafka
- **Real-time инференс** с consumer-сервисом на Python
- **Хранение предсказаний** в Redis

---

## Технологический стек

| Компонент | Технология | Назначение |
|-----------|------------|------------|
| Оркестрация | Apache Airflow 2.8 | Управление ETL-пайплайнами |
| Обработка данных | Apache Spark 3.4 | Распределённые вычисления |
| Хранилище | MinIO | S3-совместимое объектное хранилище |
| Feature Store | Redis 7 | Быстрое key-value хранение признаков |
| ML Platform | MLflow | Tracking, Registry, Model Serving |
| Message Queue | Apache Kafka (KRaft) | Очередь для streaming-инференса |
| Контейнеризация | Docker Compose | Локальный деплой инфраструктуры |

---

## Структура проекта

```
hw4/
├── airflow/
│   ├── Dockerfile          # Образ Airflow + Spark Client
│   ├── dags/
│   │   └── hw4_dag.py      # ML-пайплайн (ETL → Training → Serving)
│   └── requirements.txt
├── spark-jobs/
│   ├── split_dataset.py    # Разделение на train/test
│   ├── feature_engineering.py  # Инженерия признаков
│   └── load_to_redis.py    # Загрузка в Feature Store
├── mlflow/
│   ├── Dockerfile
│   └── train_model.py      # Обучение и регистрация модели
├── inference/
│   ├── Dockerfile
│   └── queue_consumer.py   # Kafka consumer для инференса
├── docker-compose.yml      # Описание всей инфраструктуры
├── run.sh                  # Скрипт полного деплоя
└── verify_streaming.py     # E2E-тест streaming-пайплайна
```

---

## Быстрый старт

### Предварительные требования

- Docker и Docker Compose
- 8+ GB RAM
- Python 3.10+ (для верификации)

### Запуск

1. **Настройка переменных окружения:**

   ```bash
   cp .env.example .env
   # Отредактируйте .env при необходимости
   ```

2. **Полный деплой и запуск пайплайна:**

   ```bash
   ./run.sh
   ```

   Скрипт автоматически:
   - Соберёт Docker-образы
   - Запустит все сервисы
   - Дождётся их готовности (healthcheck)
   - Запустит Airflow DAG
   - Развернёт Model Serving

3. **Проверка streaming-инференса:**

   В папке `verification` находятся скрипты для тестирования.

   ```bash
   cd verification
   pip install -r requirements.txt
   python3 verify_streaming.py
   ```

---

## Компоненты системы

### Apache Airflow

Оркестратор ML-пайплайна. DAG `mlsd_hw4` включает:

- Загрузку датасета MovieLens
- Разделение на train/test через Spark
- Feature Engineering для обеих выборок
- Загрузку признаков в Redis
- Обучение и регистрацию модели в MLflow

### Apache Spark

Выполняет распределённую обработку данных:

- `split_dataset.py` — стратифицированное разделение 80/20
- `feature_engineering.py` — расчёт пользовательских признаков (avg_rating, genre_profile, num_movies)
- `load_to_redis.py` — параллельная загрузка в Redis

### Redis

Двойная роль:

- **DB 0**: Feature Store — признаки пользователей
- **DB 2**: Predictions Store — результаты инференса

### MLflow

- Tracking Server для логирования метрик
- Model Registry для версионирования
- Model Serving на порту 6000

### Apache Kafka

Message broker в KRaft-режиме (без ZooKeeper):

- Topic `inference_queue` для запросов на предсказание
- Kafka UI на порту 8090

### Inference Service

Python-сервис, который:

1. Читает запросы из Kafka
2. Получает признаки из Redis Feature Store
3. Выполняет предсказание моделью из MLflow
4. Сохраняет результат в Redis

---

## Веб-интерфейсы

| Сервис | URL | Credentials |
|--------|-----|-------------|
| Airflow | <http://localhost:8088> | .env |
| MLflow | <http://localhost:5000> | — |
| MinIO Console | <http://localhost:9001> | .env |
| Spark Master | <http://localhost:8080> | — |
| Kafka UI | <http://localhost:8090> | — |

---

## Датасет

Проект использует [MovieLens](https://grouplens.org/datasets/movielens/) — датасет рейтингов фильмов.

Целевая переменная: бинарная классификация (rating ≥ 4.0 → положительный класс).

Признаки пользователя:

- `avg_rating` — средний рейтинг
- `num_movies` — количество оценённых фильмов
- `genre_profile` — профиль предпочтений по жанрам
- `movie_ids` — список оценённых фильмов
