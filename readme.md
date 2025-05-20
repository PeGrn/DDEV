# NYC Taxi Data Pipeline Project

Ce projet implémente un pipeline de données modulaire et évolutif pour traiter les données des trajets en taxi de New York et les données météorologiques en temps réel. Le pipeline comprend:

- Traitement batch des données historiques des trajets en taxi (PySpark)
- Traitement en streaming (simulé) des données météorologiques (PySpark Streaming)
- Stockage des données dans un data lake (MinIO)
- Entrepôt de données (PostgreSQL)
- Orchestration des flux de travail avec Airflow
- Modélisation des données avec dbt

## Prérequis

- Docker et Docker Compose
- Un compte API OpenWeatherMap (gratuit)

## Structure du projet

```
nyc_taxi_project/
│
├── docker-compose.yml
├── dags/
│   ├── taxi_batch_dag.py
│   └── weather_streaming_dag.py
│
├── scripts/
│   ├── download_taxi_data.py
│   ├── fetch_weather_data.py
│   ├── taxi_transform.py
│   └── weather_transform.py
│
├── spark/
│   ├── apps/
│   └── data/
│
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── schema.yml
│   │   ├── sources.yml
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── profiles.yml
│
└── README.md
```

## Installation et configuration

1. Clonez ce dépôt:

   ```bash
   git clone https://github.com/votre-username/nyc-taxi-project.git
   cd nyc-taxi-project
   ```

2. Configurez votre clé API OpenWeatherMap:

   - Inscrivez-vous sur [OpenWeatherMap](https://openweathermap.org/api) pour obtenir une clé API gratuite
   - Ouvrez `scripts/fetch_weather_data.py` et remplacez `YOUR_API_KEY` par votre clé API

3. Créez les répertoires nécessaires:

   ```bash
   mkdir -p airflow dags plugins scripts spark/apps spark/data dbt/models/staging dbt/models/intermediate dbt/models/marts
   ```

4. Copiez les scripts dans les répertoires appropriés:

   ```bash
   cp scripts/* dags/scripts/
   ```

5. Téléchargez le pilote JDBC PostgreSQL:
   ```bash
   mkdir -p dags/jars
   wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -P dags/jars/
   ```

## Démarrage

1. Lancez l'environnement Docker:

   ```bash
   docker-compose up -d
   ```

2. Initialisez la base de données PostgreSQL:

   ```bash
   cat initdb.sql | docker exec -i nyc_taxi_postgres psql -U postgres
   ```

3. Créez le bucket MinIO:

   - Accédez à l'interface MinIO à l'adresse http://localhost:9001
   - Connectez-vous avec les identifiants `minio` / `minio123`
   - Créez un bucket nommé `nyc-taxi-data`

4. Accédez à l'interface Airflow:
   - Ouvrez http://localhost:8080 dans votre navigateur
   - Connectez-vous avec les identifiants par défaut (airflow/airflow)
   - Activez les DAGs `yellow_taxi_batch_pipeline` et `weather_streaming_pipeline`

## Pipeline de données

1. **Ingestion des données** (Airflow DAGs):

   - `yellow_taxi_batch_pipeline` télécharge et transforme les données des taxis
   - `weather_streaming_pipeline` récupère et traite les données météo

2. **Transformation des données** (PySpark):

   - `taxi_transform.py` nettoie et transforme les données des taxis
   - `weather_transform.py` traite les données météo

3. **Modélisation des données** (dbt):
   ```bash
   docker exec -it dbt dbt run
   ```

## Analyses

Une fois le pipeline exécuté, vous pouvez interroger les données transformées en PostgreSQL:

- `trip_enriched`: Données de trajet enrichies avec les informations météo
- `trip_summary_per_hour`: Statistiques des trajets agrégées par heure et catégorie météo
- `high_value_customers`: Identification des clients à haute valeur

## Questions d'analyse

Pour répondre aux questions analytiques:

- Utilisez l'interface PostgreSQL pour exécuter des requêtes SQL sur les tables générées
- Consultez le modèle `trip_enriched` pour les analyses croisant les données de taxi et de météo
- Explorez les tendances par heure et par météo avec le modèle `trip_summary_per_hour`
