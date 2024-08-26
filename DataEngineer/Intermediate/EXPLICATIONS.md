## Réponse au test "Intermediate Data Engineer test"


### Choix technologiques, justification et implémentation :
- Utilisation d'une plateforme (de référence) de streaming moderne Apache Spark pour la réalisation de l'intégration des données dans la base de données sqlite.
- Avantage d'Apache Spark : pouvoir faire du streaming (micro-batching) dans le cas de récupération de données en temps réel face à des solutions qui tournent simplement en batching.
- Utilisation de Docker pour le déploiement/lecture du code (conteneurisation du code).
- Versionning CI/CD GIT.
- Préconisations : utilisation d'une moderne data stack pour monitoring et running du job (Apache Airflow/Databricks par exemple).


### Explication du code :
- Le code se compose de 4 fichiers .py :
    - le fichier main.py qui permet de runner le code
    - le fichier pipeline.py qui s'occupe de récupérer les données du fichier .csv, de les transformer et de les ajouter à la base de données sqlite .db
    - le fichier test.py qui s'occupe de faire des tests sur la base de données, afin de vérifier qu'elle est bien intègre
    - le fichier kpis.py qui s'occupe de calculer les KPIs demandés dans le fichier README.md
- Le code se compose d'un fichier format Docker (Dockerfile) qui va conteneuriser le code (configuration spécifique pour Spark)
- de l'ajout du jar sqlite qui permet de lire, charger, manipuler une base de données sqlite

### Run du code :
- le fichier Fizzbuzz.py peut se lire directement sur sa machine (pas besoin de configuration de python spécifique)

Pour l'exécution du pipeline de données :
- Disposer de docker sur sa machine (ou le télécharger ici [Docker](https://www.docker.com/products/docker-desktop/))
- Descendre dans l'arborescence du projet (hiring\DataEngineer\Intermediate) via le terminal python
- Construire une image avec la commande suivante : docker build -t sparkapp .
- Run de l'image avec la commande suivante : docker run -t sparkapp

Le fichier main.py s'exécutera alors et réalisera l'intégration complète des données : extractions, transformations, nettoyages, tests, calculs des KPIs.
