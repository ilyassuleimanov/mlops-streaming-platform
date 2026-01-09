# Hybrid RecSys Architecture

High-performance MLOps platform for recommender systems. Implements a hybrid data processing architecture:

- **Batch Layer**: ETL orchestration and Feature Engineering (Airflow + Spark) with Data Lake storage (MinIO).
- **Serving Layer**: Experiment and model management (MLflow), Feature Store (Redis).
- **Inference Layer**: Asynchronous prediction request processing via queues (Redis Queue → Python Consumer).

## Tech Stack

- **Containerization**: Docker Compose
- **Orchestration**: Apache Airflow
- **Data Processing**: Apache Spark
- **Storage**: MinIO (S3 compatible), Redis
- **ML Platform**: MLflow

## Getting Started

1. Copy the environment template:

   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

2. Run the platform:

   ```bash
   ./run.sh
   # This will build images, start services, and run the checking pipeline
   ```

3. Access Interfaces:
   - **Airflow**: <http://localhost:8088>
   - **MLflow**: <http://localhost:5000>
   - **MinIO**: <http://localhost:9001>
   - **Spark Master**: <http://localhost:8080>
