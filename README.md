# Medical Data Lakehouse

Projet de **Data Lakehouse médical** basé sur des données synthétiques issues de [Synthea](https://synthetichealth.github.io/downloads.html). Il illustre l'ingestion de sources hétérogènes, le traitement des données avec Apache Spark et leur stockage dans des tables Apache Iceberg.

## Architecture

```text
Sources : PostgreSQL, CSV, JSONL
              |
              v
      Airflow : orchestration
              |
              v
      Spark : traitement des données
              |
              v
       Bronze : données ingérées
              |
              v
       Silver : nettoyage et contrôle qualité
              |                \
              |                 -> Quarantine : données rejetées
              v
       Gold : indicateurs analytiques
              |
              v
       Spark SQL : exploration et analyse
```

Les tables Iceberg et leurs métadonnées sont stockées dans **RustFS**, un stockage objet compatible S3. Un catalogue Iceberg REST permet de référencer et de gérer les tables.

Apache Airflow orchestre les traitements : il déclenche l'ingestion, contrôle son résultat et lance les transformations Silver et Gold lorsque cela est nécessaire.

## Technologies

- **Apache Spark / PySpark** : ingestion, transformations et requêtes SQL.
- **Apache Iceberg** : format de table, métadonnées, snapshots et Time Travel.
- **RustFS** : stockage objet compatible S3.
- **Catalogue Iceberg REST** : gestion des références aux tables Iceberg.
- **PostgreSQL** : source de données relationnelles.
- **Apache Airflow** : planification et orchestration des traitements.
- **JupyterLab** : développement et exécution des notebooks.
- **Docker Compose** : déploiement local des services.

## Contenu du projet

```text
medical-lakehouse/
├── compose.yaml
├── compose.airflow.yaml
├── compose.runner.yaml
├── docker/
│   └── spark-jupyter/
│       └── Dockerfile
├── airflow/
│   └── dags/
│       ├── 01_premier_dag_medical.py
│       └── medical_lakehouse_pipeline.py
└── notebooks/
    ├── importation_donnees.ipynb
    ├── test_infra.ipynb
    ├── Partie1_Bronze.ipynb
    ├── Partie2_Silver.ipynb
    ├── Partie3_Gold.ipynb
    ├── Partie4_Iceberg_Snapchots.ipynb
    ├── data/
    └── jobs/
        ├── lakehouse_common.py
        ├── bronze_incremental.py
        ├── silver_refresh.py
        ├── gold_refresh.py
        └── spark_job_runner.py
```

Les fichiers Compose ont des rôles complémentaires :

- `compose.yaml` démarre les services du Lakehouse.
- `compose.airflow.yaml` ajoute les composants Airflow.
- `compose.runner.yaml` ajoute le service qui permet à Airflow de lancer les traitements Spark.

Les fichiers Python du dossier `notebooks/jobs/` sont les traitements exécutables utilisés par la pipeline automatisée. Les notebooks restent disponibles pour l'apprentissage, les tests et l'exploration.

## Données utilisées

Le projet utilise principalement les fichiers Synthea suivants :

- `patients.csv` : informations sur les patients.
- `encounters.csv` : consultations et rencontres médicales.
- `observations.csv` : observations enregistrées.

Le fichier `encounters.csv` alimente les tables de consultations du Lakehouse.

Les données Synthea sont synthétiques et sont utilisées à des fins pédagogiques.

## Pipeline de traitement

### 1. Bronze — Ingestion

Les données sont lues depuis PostgreSQL et les fichiers CSV/JSONL, puis intégrées dans les tables Bronze d'Iceberg.

Le traitement incrémental compare les identifiants ou les empreintes des lignes avec ceux déjà présents afin d'éviter les réinsertions.

### 2. Silver — Nettoyage et qualité

Les données Bronze sont contrôlées et transformées. Les lignes qui ne respectent pas les règles de validation sont enregistrées dans les tables Quarantine avec un motif de rejet.

### 3. Gold — Analyse

Les données Silver servent à calculer des indicateurs analytiques, notamment :

- le nombre de consultations par catégorie ;
- l'activité mensuelle ;
- le nombre d'observations par type ;
- la répartition des patients par genre.

### 4. Iceberg — Historique des tables

Les snapshots et les métadonnées Iceberg permettent d'étudier l'évolution des tables et les possibilités de consultation de versions antérieures.

## Apache Airflow

### Rôle d'Airflow

Apache Airflow est utilisé pour **orchestrer et planifier la pipeline**, et non pour effectuer lui-même les calculs distribués.

Le DAG `medical_lakehouse_pipeline` organise les traitements dans le bon ordre :

1. Exécuter l'ingestion incrémentale Bronze.
2. Examiner le résultat de l'ingestion.
3. Actualiser Silver et Quarantine si de nouvelles données ont été détectées ou si les tables dérivées doivent être créées.
4. Actualiser Gold après la réussite de Silver.
5. Terminer l'exécution et enregistrer son état.

Le service `spark-job-runner` permet à Airflow de demander l'exécution des scripts Spark.

### Détection automatique des nouvelles données

Le DAG est configuré pour vérifier les sources **toutes les cinq minutes** lorsqu'il est activé.

Lorsqu'une nouvelle donnée est détectée, le traitement ajoute les nouveaux enregistrements à Bronze, puis actualise Silver et Gold.

Lorsqu'aucune nouveauté n'est détectée et que les tables dérivées existent déjà, les transformations Silver et Gold sont ignorées.

Il s'agit d'une vérification périodique : le traitement n'est pas déclenché instantanément à chaque insertion dans PostgreSQL.

### Interface Airflow

L'interface Web est accessible à l'adresse :

[http://localhost:8080](http://localhost:8080)

Depuis cette interface, il est possible de :

- consulter les DAG disponibles ;
- activer ou mettre en pause la planification ;
- déclencher une exécution manuelle ;
- suivre les tâches dans la vue Graph ;
- consulter les journaux de chaque tâche ;
- identifier les erreurs et les échecs.

Les identifiants de connexion sont ceux définis dans la configuration Airflow du projet.

## Démarrage rapide

### Prérequis

- Docker Desktop installé et démarré.
- WSL disponible si le projet est exécuté sous Windows.
- Fichiers de configuration Docker et variables d'environnement préparés.

### 1. Préparer le fichier `.env`

À la racine du projet, créer un fichier `.env` contenant les clés d'accès locales utilisées par RustFS :

```dotenv
RUSTFS_ACCESS_KEY=your_local_access_key
RUSTFS_SECRET_KEY=your_local_secret_key
```

Remplacer les valeurs d'exemple par les valeurs correspondant à votre configuration.

### 2. Démarrer les services

Depuis le terminal, se placer dans le dossier `medical-lakehouse/`, puis vérifier la configuration :

```bash
docker compose \
  -f compose.yaml \
  -f compose.airflow.yaml \
  -f compose.runner.yaml \
  config --quiet
```

Démarrer ensuite les services :

```bash
docker compose \
  -f compose.yaml \
  -f compose.airflow.yaml \
  -f compose.runner.yaml \
  up -d --build
```

### 3. Accéder aux interfaces

| Service | Adresse |
|---|---|
| JupyterLab | [http://localhost:8888](http://localhost:8888) |
| Airflow | [http://localhost:8080](http://localhost:8080) |
| Console RustFS | [http://localhost:9001](http://localhost:9001) |
| API Iceberg REST | [http://localhost:8181/v1/config](http://localhost:8181/v1/config) |
| PostgreSQL depuis Windows | `localhost:5433` |

Les identifiants et les tokens sont ceux définis dans les fichiers Compose et les variables d'environnement du projet.

### 4. Exécuter les traitements

Pour le travail pédagogique, exécuter les notebooks dans l'ordre :

1. Préparation et importation des données.
2. Partie 1 — Bronze.
3. Partie 2 — Silver.
4. Partie 3 — Gold.
5. Partie 4 — Snapshots Iceberg.

Pour l'exécution automatisée, ouvrir Airflow, rechercher `medical_lakehouse_pipeline`, puis déclencher une première exécution manuelle. Après validation du workflow, activer le DAG pour démarrer les vérifications planifiées.

### 5. Vérifier les conteneurs

```bash
docker compose \
  -f compose.yaml \
  -f compose.airflow.yaml \
  -f compose.runner.yaml \
  ps
```

### 6. Arrêter les services

```bash
docker compose \
  -f compose.yaml \
  -f compose.airflow.yaml \
  -f compose.runner.yaml \
  down
```