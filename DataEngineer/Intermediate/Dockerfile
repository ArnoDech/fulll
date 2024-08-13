# Partir de l'image de base openjdk 22
FROM apache/spark:3.5.1

# Installer les dépendances nécessaires pour `apt-get`
USER root
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    gnupg && \
    rm -rf /var/lib/apt/lists/*

# Installer Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
#&& python3 -m ensurepip \
#&& pip3 install --no-cache --upgrade pip setuptools

RUN pip3 install numpy
RUN pip3 install pandas

# Copier le code du projet dans le conteneur
WORKDIR /app
COPY . /app

# Commande par défaut pour exécuter le script
CMD ["python3", "test.py"]
