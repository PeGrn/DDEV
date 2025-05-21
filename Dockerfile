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

# Créer les répertoires nécessaires pour les JARs
RUN mkdir -p /opt/airflow/dags/jars

# Télécharger les JARs nécessaires
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -P /opt/airflow/dags/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P /opt/airflow/dags/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/airflow/dags/jars/

# Copier les JARs dans le répertoire de Spark
RUN cp /opt/airflow/dags/jars/*.jar ${SPARK_HOME}/jars/

# Ajouter Spark au PATH
ENV PATH $PATH:${SPARK_HOME}/bin

# Retourner à l'utilisateur airflow pour installer les packages Python
USER airflow

# Installer boto3 et autres dépendances
RUN pip install --user boto3 requests