FROM apache/airflow:2.8.1-python3.10

ENV AIRFLOW_VERSION=2.8.1
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
ENV PATH="/home/airflow/.local/bin:${PATH}"

# --- Системные зависимости ---
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    default-jre \
    build-essential \
    python3-dev \
    gettext-base \
    wget \
    postgresql-client \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
RUN curl -sSLo /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-${TARGETARCH}/mc && \
    chmod +x /usr/local/bin/mc

# --- Параметры Spark которые можно править через build-args ---
ARG SPARK_VERSION=3.4.0
ARG HADOOP_PROFILE=hadoop3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# --- СКАЧИВАЕМ SPARK (bin-hadoop3) ---
RUN set -eux; \
    cd /tmp; \
    SPARK_TGZ="spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz"; \
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"; \
    wget -q "$SPARK_URL"; \
    tar -xzf "$SPARK_TGZ" -C /opt; \
    mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE} ${SPARK_HOME}; \
    rm -f "$SPARK_TGZ";

# --- Добавляем hadoop-aws и aws SDK в jars (нужны для s3a://) ---
RUN set -eux; \
    cd /opt/spark/jars; \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar; \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# --- Установка Python requirements проекта ---
COPY airflow/requirements.txt .
RUN pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"

# --- Копирование кода ---
COPY airflow/dags /opt/airflow/dags
COPY spark-jobs /opt/airflow/spark-jobs

# --- Права ---
RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins && \
    chown -R 50000:0 /opt/airflow/dags /opt/airflow/spark-jobs /opt/airflow/logs /opt/airflow/plugins && \
    chmod -R 775 /opt/airflow/dags /opt/airflow/spark-jobs /opt/airflow/logs /opt/airflow/plugins

USER airflow
