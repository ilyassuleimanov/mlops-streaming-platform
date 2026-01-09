# spark/Dockerfile

# 1. Используем официальный базовый образ Spark с поддержкой Python.
# Версия 3.4.0, как мы выяснили, стабильна и доступна.
FROM apache/spark-py:v3.4.0

# 2. Переключаемся на пользователя root для установки системных пакетов и библиотек.
USER root

# 3. Устанавливаем Python-зависимости из нашего файла requirements.txt.
COPY spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Устанавливаем S3A коннектор для Spark. Это КРИТИЧЕСКИ ВАЖНЫЙ шаг.
# Spark "из коробки" не умеет общаться с S3/Minio по протоколу s3a://.
# Для этого ему нужны специальные Java-библиотеки (JAR-файлы).
# Версии JAR'ов должны быть совместимы с версией Hadoop, встроенной в образ Spark.
# Для Spark 3.4.0 это Hadoop 3.3.4.
# Мы скачиваем их напрямую из репозитория Maven.
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN apt-get update && apt-get install -y wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/spark/jars/ && \
    apt-get remove -y wget && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# 5. Копируем наши Spark-скрипты (jobs) внутрь образа.
# Это соответствует идеологии "запекания" кода в образ, а не монтирования через volumes.
# /opt/spark/jobs - логичное и стандартное место для хранения скриптов.
COPY spark-jobs /opt/spark/jobs