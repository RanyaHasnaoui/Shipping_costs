FROM apache/airflow:latest

USER root

# Install OpenJDK 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean
# Set JAVA_HOME environment variable for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
# Install Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3

RUN curl -LO https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow
RUN pip install --no-cache-dir pymysql pyspark