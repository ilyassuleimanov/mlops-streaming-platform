#!/usr/bin/env bash
set -euo pipefail

# --- Загрузка переменных из .env ---
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# --- Переменные окружения (из .env) ---
export MINIO_ROOT_USER
export MINIO_ROOT_PASSWORD
export AIRFLOW_UID=$(id -u)
export IMAGE_TAG
echo "INFO: IMAGE_TAG = ${IMAGE_TAG}"

# --- Переменные для автоматизации ---
MINIO_SERVICE_NAME="minio"
REDIS_SERVICE_NAME="redis"
SPARK_MASTER_SERVICE_NAME="spark-master"
SPARK_WORKER_SERVICE_NAME="spark-worker-1"
AIRFLOW_SERVICE_NAME="airflow"
POSTGRES_SERVICE_NAME="postgres"
MLFLOW_SERVICE_NAME="mlflow"
KAFKA_SERVICE_NAME="kafka"
INFERENCE_SERVICE_NAME="inference"

DAG_ID="mlops_platform"
API_USER="${AIRFLOW_API_USER}"
API_PASS="${AIRFLOW_API_PASS}"
API_EMAIL="${API_USER}@example.com"
AIRFLOW_API_URL="http://localhost:8080/api/v1"


# ---------- Вспомогательная функция для ожидания Healthcheck ----------

wait_healthy() {
    local service_name="$1"
    local timeout="${2:-120}"

    echo "==> Ожидание healthcheck для сервиса '$service_name' (таймаут: ${timeout}s)..."

    local start_time=$(date +%s)
    while true; do
        local status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "mlops-${service_name}" 2>/dev/null || echo "not found")

        if [[ "$status" == "healthy" ]]; then
            echo "✅ Сервис '$service_name' готов."
            return 0
        fi

        local current_time=$(date +%s)
        if (( current_time - start_time > timeout )); then
            echo "❌ Таймаут ожидания healthcheck для сервиса '$service_name'."
            echo "Последние логи:"
            docker compose logs --tail=100 "$service_name"
            exit 1
        fi

        echo -n "."
        sleep 5
    done
}


# --- Основной сценарий ---

echo "--- [ЭТАП 0/7] Очистка окружения ---"
echo "Удаляем старые контейнеры, volumes и локальные образы..."
docker compose down -v --rmi local --remove-orphans || true

echo "--- [ЭТАП 1/7] Сборка Docker-образов ---"
docker compose build

echo -e "\n--- [ЭТАП 2/7] Запуск всех сервисов в фоновом режиме ---"
docker compose up -d

echo -e "\n--- [ЭТАП 3/7] Ожидание полной готовности сервисов ---"
wait_healthy "$POSTGRES_SERVICE_NAME" 60
wait_healthy "$MINIO_SERVICE_NAME" 60
wait_healthy "$REDIS_SERVICE_NAME" 60
wait_healthy "$SPARK_MASTER_SERVICE_NAME" 60
wait_healthy "$SPARK_WORKER_SERVICE_NAME" 60
wait_healthy "$KAFKA_SERVICE_NAME" 60
wait_healthy "$MLFLOW_SERVICE_NAME" 120

wait_healthy "$AIRFLOW_SERVICE_NAME" 300


echo -e "\n--- [ЭТАП 4/7] Создание/Обновление пользователя в Airflow ---"
create_user_with_retry() {
  EMAIL="${API_EMAIL}"
  max_attempts=7
  attempt=1
  sleep_between=5

  while [ $attempt -le $max_attempts ]; do
    echo "[$(date -Iseconds)] Попытка $attempt/$max_attempts: создаём/обновляем пользователя '$API_USER' (email: $EMAIL)..."
    out="$(docker compose exec --user airflow "$AIRFLOW_SERVICE_NAME" bash -lc "\
      airflow users create \
        --username '$API_USER' \
        --password '$API_PASS' \
        --firstname 'Admin' \
        --lastname 'User' \
        --role 'Admin' \
        --email '$EMAIL' 2>&1" || true)"
    echo "=== Вывод создания пользователя ==="; echo "$out"; echo "=== Конец вывода ==="
    if echo "$out" | grep -i -E "created|already exist|already exists|already in the db" >/dev/null 2>&1; then
      echo "ℹ️ Пользователь создан или уже существовал. Проверяем API..."
      http_code="$(docker compose exec --user airflow "$AIRFLOW_SERVICE_NAME" bash -lc "curl -s -o /dev/null -w '%{http_code}' -u '${API_USER}:${API_PASS}' 'http://localhost:8080/api/v1/dags/${DAG_ID}'" || echo "000")"
      if [ "$http_code" = "200" ]; then echo "✅ API аутентификация успешна."; return 0; fi
      if [ "$http_code" = "401" ]; then
        echo "❗ API вернул 401. Попробуем обновить пароль."
        pw_hash="$(docker compose exec --user airflow "$AIRFLOW_SERVICE_NAME" bash -lc "python -c 'from werkzeug.security import generate_password_hash; print(generate_password_hash(\"${API_PASS}\"))'" 2>/dev/null || true)"
        if [ -n "$pw_hash" ]; then
          docker compose exec postgres psql -U airflow -d airflow -c "UPDATE ab_user SET password = '$pw_hash' WHERE username = '${API_USER}';" >/dev/null 2>&1 || true
          http_code2="$(docker compose exec --user airflow "$AIRFLOW_SERVICE_NAME" bash -lc "curl -s -o /dev/null -w '%{http_code}' -u '${API_USER}:${API_PASS}' 'http://localhost:8080/api/v1/dags/${DAG_ID}'" || echo "000")"
          if [ "$http_code2" = "200" ]; then echo "✅ Пароль обновлён, аутентификация успешна."; return 0; fi
        fi
      fi
    elif echo "$out" | grep -i "duplicate key value violates unique constraint \"ab_user_email_uq\"" >/dev/null 2>&1; then
        echo "⚠️ Конфликт по email. Пробуем уникальный email."
        EMAIL="${API_USER}@example.com"
        attempt=$((attempt+1)); sleep $sleep_between; continue
    fi
    echo "❌ Попытка $attempt не удалась."; attempt=$((attempt+1)); sleep $sleep_between
  done
  echo "ОШИБКА: не удалось создать/исправить пользователя."; return 1
}
create_user_with_retry


echo -e "\n--- [ЭТАП 5/7] Ожидание и запуск DAG через REST API ---"
echo "Ожидаем, пока DAG '$DAG_ID' не появится в API..."
ATTEMPTS=30
for i in $(seq 1 $ATTEMPTS); do
    http_code=$(docker compose exec "$AIRFLOW_SERVICE_NAME" curl -s -o /dev/null -w "%{http_code}" -u "${API_USER}:${API_PASS}" "${AIRFLOW_API_URL}/dags/${DAG_ID}")
    if [ "$http_code" -eq 200 ]; then echo "✅ DAG '$DAG_ID' найден."; break; else echo "Ожидаем DAG... ($i/$ATTEMPTS, http: $http_code)"; sleep 10; fi
    if [ $i -eq $ATTEMPTS ]; then echo "❌ DAG '$DAG_ID' не появился в API."; exit 1; fi
done
echo "Включаем (unpause) DAG '$DAG_ID'..."
docker compose exec "$AIRFLOW_SERVICE_NAME" curl -X PATCH -u "${API_USER}:${API_PASS}" "${AIRFLOW_API_URL}/dags/${DAG_ID}" -H "Content-Type: application/json" -d '{"is_paused": false}'
RUN_ID="api_run_$(date +%Y-%m-%dT%H:%M:%S%z)"
echo "Запускаем DAG '$DAG_ID' с run_id = $RUN_ID..."
docker compose exec "$AIRFLOW_SERVICE_NAME" curl -X POST -u "${API_USER}:${API_PASS}" "${AIRFLOW_API_URL}/dags/${DAG_ID}/dagRuns" -H "Content-Type: application/json" -d "{\"dag_run_id\": \"$RUN_ID\"}"
echo "Ожидаем завершения DAG'а..."
sleep 15
while true; do
    status=$(docker compose exec "$AIRFLOW_SERVICE_NAME" curl -s -u "${API_USER}:${API_PASS}" "${AIRFLOW_API_URL}/dags/${DAG_ID}/dagRuns/${RUN_ID}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('state', 'unknown'))")
    if [[ "$status" == "success" ]]; then echo "✅ DAG '$DAG_ID' успешно выполнен."; break;
    elif [[ "$status" == "failed" ]]; then echo "❌ DAG '$DAG_ID' завершился с ошибкой."; exit 1; fi
    printf "Статус DAG'а: %s. Ожидаем...\n" "$status"; sleep 15
done


echo -e "\n--- [ЭТАП 6/7] Запуск Model Serving в MLflow ---"
MODEL_NAME="logistic_regression_movielens"
echo "Запускаем сервер для модели '${MODEL_NAME}' на порту 6000..."
docker compose exec -d "$MLFLOW_SERVICE_NAME" mlflow models serve \
    -m "models:/${MODEL_NAME}/latest" \
    -h 0.0.0.0 \
    -p 6000 \
    --no-conda

echo "Ожидание запуска сервера модели (до 90 секунд)..."
MODEL_SERVER_TIMEOUT=90
MODEL_SERVER_START=$(date +%s)
while true; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:6000/health 2>/dev/null | grep -qE "200|405"; then
        echo "✅ Сервер модели готов."
        break
    fi
    
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - MODEL_SERVER_START))
    if [ $ELAPSED -ge $MODEL_SERVER_TIMEOUT ]; then
        echo "⚠️ Таймаут ожидания сервера модели. Пробуем отправить запрос напрямую..."
        break
    fi
    
    echo -n "."
    sleep 5
done


echo -e "\n--- [ЭТАП 7/7] Завершение ---"
echo "✅ Инфраструктура, пайплайн обучения и инференс-сервис запущены."
echo "Для проверки стримингового предсказания запустите: python3 verify_streaming.py"

echo -e "\n\n🎉🎉🎉 ПРОЕКТ УСПЕШНО ЗАПУЩЕН! 🎉🎉🎉"
echo
echo "Точки доступа к веб-интерфейсам:"
echo " - MinIO Console:    http://localhost:9001 (${MINIO_ROOT_USER}/${MINIO_ROOT_PASSWORD})"
echo " - Spark Master UI:  http://localhost:8080"
echo " - Spark Worker UI:  http://localhost:8081"
echo " - Airflow UI:       http://localhost:8088 (${API_USER}/${API_PASS})"
echo " - MLflow UI:        http://localhost:5000"
echo " - Model Endpoint:   http://localhost:6000/invocations"
echo " - Kafka UI:         http://localhost:8090"
echo " - Kafka:            localhost:29092 (external), kafka:9092 (internal)"
echo " - Redis:            localhost:6379 (no auth)"
