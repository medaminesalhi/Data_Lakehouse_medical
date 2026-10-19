# Medical Data Lakehouse

Projet de **Data Lakehouse médical** basé sur des données synthétiques [Synthea](https://synthetichealth.github.io/downloads.html). Il illustre l'ingestion de sources hétérogènes, la transformation des données avec **Apache Spark** et leur stockage dans des tables **Apache Iceberg**.

## Architecture

```text
Sources (PostgreSQL, CSV, JSONL)
                |
                v
        Bronze : données brutes
                |
                v
        Silver : nettoyage et contrôle qualité
                |           \
                |            -> Quarantine : données rejetées
                v
        Gold : indicateurs analytiques
                |
                v
             Spark SQL
```

Les tables Iceberg et leurs métadonnées sont stockées dans **RustFS**, un stockage objet compatible S3. Un **catalogue Iceberg REST** permet de référencer les tables.

## Technologies

- **Apache Spark / PySpark** : ingestion, transformations et requêtes SQL.
- **Apache Iceberg** : format de table, métadonnées, snapshots et Time Travel.
- **RustFS** : stockage objet compatible S3.
- **PostgreSQL** : source de données relationnelles.
- **JupyterLab** : exécution des notebooks.
- **Docker Compose** : déploiement local des services.

## Contenu du projet

```text
medical-lakehouse/
├── compose.yaml
├── docker/
│   └── spark-jupyter/
│       └── Dockerfile
└── notebooks/
    ├── importation_donnees.ipynb
    ├── test_infra.ipynb
    ├── Partie1_Bronze.ipynb
    ├── Partie2_Silver.ipynb
    ├── Partie3_Gold.ipynb
    └── Partie4_Iceberg_Snapchots.ipynb
```

Les traitements utilisent les fichiers **`patients.csv`**, **`encounters.csv`** et **`observations.csv`** issus de Synthea. Le fichier `encounters.csv` alimente les tables de rencontres médicales nommées `consultations` dans le Lakehouse.

## Démarrage rapide

1. Installer et démarrer **Docker Desktop**.
2. Créer un fichier `.env` à la racine du projet contenant les variables `RUSTFS_ACCESS_KEY` et `RUSTFS_SECRET_KEY` avec des identifiants locaux de votre choix.
3. Démarrer les conteneurs :

```bash
docker compose up -d --build
```

4. Ouvrir **JupyterLab** sur http://localhost:8888 (token local défini dans `compose.yaml`).
5. Accéder à la console **RustFS** sur http://localhost:9001 et créer le bucket `medical-warehouse` avant les écritures Iceberg.
6. Exécuter les notebooks dans l'ordre : préparation des données, **Bronze**, **Silver**, **Gold**, puis exploration facultative des snapshots.

Pour arrêter les services :

```bash
docker compose down
```

