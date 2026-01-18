import json
import time
import uuid
import redis
from kafka import KafkaProducer

# Конфигурация
REDIS_HOST = "localhost" # Скрипт запускается локально, снаружи контейнеров
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092" # Внешний порт Kafka
KAFKA_TOPIC = "inference_queue"

def main():
    print("--- Starting Streaming Verification ---")

    # 1. Подключение к Redis (Feature Store)
    try:
        r_features = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        r_features.ping()
        print("✅ Connected to Redis (Feature Store)")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return

    # 2. Подключение к Redis (Predictions Store)
    try:
        r_predictions = redis.Redis(host=REDIS_HOST, port=6379, db=2, decode_responses=True)
        print("✅ Connected to Redis (Predictions Store)")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return

    # 3. Подключение к Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka Producer")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return

    # 4. Получение тестового пользователя
    # Берем любой ключ из Feature Store
    keys = r_features.keys("*")
    if not keys:
        print("❌ Feature Store (Redis db=0) is empty! Run the Airflow pipeline first.")
        return
    
    # Берем feature_key (например, первый попавшийся)
    # Ключи в load_to_redis.py сохранялись как user_id (просто число)
    feature_key = keys[0]
    print(f"ℹ️  Using feature key (user_id): {feature_key}")

    # 5. Отправка сообщения в Kafka
    request_id = str(uuid.uuid4())
    message = {
        "request_id": request_id,
        "feature_key": feature_key
    }
    
    print(f"🚀 Sending request to Kafka topic '{KAFKA_TOPIC}'...")
    print(f"   Payload: {json.dumps(message)}")
    
    producer.send(KAFKA_TOPIC, message)
    producer.flush()
    print("✅ Message sent.")

    # 6. Ожидание результата в Redis (db=2)
    result_key = f"prediction_{request_id}"
    print(f"⏳ Waiting for result in Redis key: {result_key}...")

    max_retries = 20
    for i in range(max_retries):
        result_json = r_predictions.get(result_key)
        if result_json:
            result = json.loads(result_json)
            print(f"✅ Result found in Redis!")
            print(f"   Prediction: {result.get('prediction')}")
            print(f"   Processed At: {result.get('processed_at')}")
            print("\n🎉 VERIFICATION SUCCESS! The streaming pipeline is working.")
            return
        
        time.sleep(1)
        print(f"   Polling ({i+1}/{max_retries})...")

    print("\n❌ VERIFICATION FAILED. Timeout waiting for result.")
    print("Check 'docker compose logs inference' for errors.")

if __name__ == "__main__":
    main()
