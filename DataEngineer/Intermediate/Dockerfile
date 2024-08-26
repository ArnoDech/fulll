# Utilisation de l'image bitnami de spark
FROM bitnami/spark:latest

# Installation des dépendances nécessaires pour `apt-get`
USER root
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    gnupg && \
    rm -rf /var/lib/apt/lists/*

# Installation de Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

#Installation des différents packages Python
RUN pip3 install pandas
RUN pip3 install numpy
RUN pip3 install pyarrow
RUN pip3 install pyspark

# Options java pour run image bitnami
ENV SPARK_DAEMON_JAVA_OPTS="-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties"
ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties"

# Créer le répertoire de configuration de Spark
RUN mkdir -p /opt/bitnami/spark/conf

# Créer le fichier log4j.properties avec la configuration désirée
RUN echo 'log4j.rootCategory=ERROR, console' > /opt/bitnami/spark/conf/log4j.properties && \
    echo 'log4j.logger.org.apache.hadoop=ERROR' >> /opt/bitnami/spark/conf/log4j.properties

# Copier le code du projet dans le conteneur
WORKDIR /app
COPY . /app

# Commande par défaut pour exécuter le script
CMD ["python3", "main.py"]
