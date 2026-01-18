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
| Оркестрация | Apache Airflow | Управление ETL-пайплайнами |
| Обработка данных | Apache Spark | Распределённые вычисления |
| Хранилище | MinIO | S3-совместимое объектное хранилище |
| Feature Store | Redis | In-memory key-value хранилище признаков |
| ML Platform | MLflow | Tracking, Registry, Model Serving |
| Message Queue | Apache Kafka | Очередь для streaming-инференса |
| Контейнеризация | Docker Compose | Локальный деплой инфраструктуры |

---

## Структура проекта

```
mlops-platform/
├── airflow/
│   ├── Dockerfile          # Образ Airflow + Spark Client
│   ├── dags/
│   │   └── mlops_dag.py    # ML-пайплайн (ETL → Training → Serving)
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

4. **Настройка переменных окружения:**

    ```bash
    cp .env.example .env
    # Отредактируйте .env при необходимости
    ```

5. **Полный деплой и запуск пайплайна:**

    ```bash
    ./run.sh
    ```

    Скрипт автоматически:
    - Соберёт Docker-образы
    - Запустит все сервисы
    - Дождётся их готовности (healthcheck)
    - Запустит Airflow DAG
    - Развернёт Model Serving

6. **Проверка streaming-инференса:**

    В папке `verification` находятся скрипты для тестирования.

    ```bash
    cd verification
    pip install -r requirements.txt
    python3 verify_streaming.py
    ```

---

## Компоненты системы

### Apache Airflow

Оркестратор ML-пайплайна, управляющий процессами загрузки данных, обучения и обновления модели. DAG `mlops_platform`.

### Apache Spark

Выполняет распределённую обработку данных, разделение датасета и инженерию признаков (Feature Engineering).

### Redis

Выполняет роль быстрого хранилища для признаков (Feature Store) и результатов предсказаний (Predictions Store).

### MLflow

Платформа для управления жизненным циклом ML: логирование метрик, реестр моделей и Model Serving.

### Apache Kafka

Брокер сообщений для организации асинхронной очереди запросов на инференс в реальном времени.

### Inference Service

Микросервис, обрабатывающий поток запросов из Kafka и выполняющий предсказания с использованием данных из Feature Store.

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
