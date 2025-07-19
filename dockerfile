FROM bitnami/spark:latest

USER root

# Basic tools
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y \
        curl \
        gnupg \
        ca-certificates \
        ncurses-bin \
        wget


# Add SBT repo and key, then install
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt

# Adding jar files for postgres support
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Adding jar files for kafka streaming support
RUN cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.0/spark-sql-kafka-0-10_2.13-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.5.0/spark-token-provider-kafka-0-10_2.13-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \ 
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar 

# Adding jar files for delta lake support
ENV DELTA_VERSION=4.0.0
ENV SCALA_VERSION=2.13
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -P /opt/bitnami/spark/jars
RUN wget https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -P /opt/bitnami/spark/jars

# Adding AWS for s3 support
# RUN cd /opt/bitnami/spark/jars && \
#     wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
#     wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar


# install uv
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy your project files
COPY pyproject.toml uv.lock /app/
RUN uv sync

# # Default command
# CMD ["spark-shell"]
