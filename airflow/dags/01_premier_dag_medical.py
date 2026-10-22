"""Premier DAG pédagogique du Medical Data Lakehouse (Airflow 2.10.5).

Démonstration uniquement : aucune connexion à PostgreSQL, Spark ou Iceberg.
Copier ce fichier dans medical-lakehouse/airflow/dags/.
"""

import logging
from collections import Counter
from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def demarrer():
    # Première tâche : signaler le début de notre workflow.
    log.info("Démarrage du premier DAG médical.")
    log.info("Il s'agit d'une démonstration, sans accès aux vraies données.")
    sleep(2)


def simuler_analyse():
    # Deuxième tâche : travailler sur trois consultations fictives.
    categories = ["ambulatoire", "urgence", "ambulatoire"]
    resultat = Counter(categories)

    log.info("Nombre de consultations fictives : %d", len(categories))
    for categorie, nombre in sorted(resultat.items()):
        log.info("Catégorie %s : %d consultation(s)", categorie, nombre)
    sleep(2)


def terminer():
    # Troisième tâche : afficher la fin du workflow.
    log.info("Toutes les tâches ont été exécutées dans le bon ordre.")
    log.info("Premier DAG médical terminé avec succès !")


with DAG(
    dag_id="demo_01_premier_dag_medical",
    description="Premier workflow Airflow : 3 tâches Python simples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,           # Exécution manuelle pour ce premier exercice
    catchup=False,          # Aucun rattrapage des périodes passées
    default_args={"retries": 1},
    tags=["medical", "demonstration"],
) as dag:
    tache_1 = PythonOperator(
        task_id="demarrer",
        python_callable=demarrer,
    )

    tache_2 = PythonOperator(
        task_id="simuler_analyse",
        python_callable=simuler_analyse,
    )

    tache_3 = PythonOperator(
        task_id="terminer",
        python_callable=terminer,
    )

    # Airflow ne lance la tâche suivante que si la précédente a réussi.
    tache_1 >> tache_2 >> tache_3
