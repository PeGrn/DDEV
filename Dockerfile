FROM apache/airflow:2.7.1

USER root

# Installer les dépendances Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

# Définir JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Installer Spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Ajouter Spark au PATH
ENV PATH $PATH:${SPARK_HOME}/bin

# Retourner à l'utilisateur airflow pour installer les packages Python
USER airflow

# Installer boto3 et autres dépendances
RUN pip install --user boto3 requests