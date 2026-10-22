
import json
import logging
import urllib.error
import urllib.request
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

LOGGER = logging.getLogger(__name__)
RUNNER_URL = "http://spark-job-runner:8090/run"


def execute_spark_job(job_name, **context):
    """Demande au runner d'exécuter un script Spark."""
    payload = json.dumps({"job": job_name}).encode("utf-8")

    request = urllib.request.Request(
        RUNNER_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=7500) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise AirflowException(
            f"Impossible de joindre le runner Spark {RUNNER_URL}: {exc}"
        ) from exc

    # Transmettre les journaux Spark aux journaux Airflow.
    for line in (result.get("stdout") or "").splitlines():
        if line.strip():
            LOGGER.info("[spark:%s] %s", job_name, line)

    for line in (result.get("stderr") or "").splitlines():
        if line.strip():
            LOGGER.info("[spark:%s:stderr] %s", job_name, line)

    if not result.get("success"):
        raise AirflowException(
            f"Le job Spark {job_name} a échoué : "
            f"returncode={result.get('returncode')}, "
            f"error={result.get('error')}. "
            "Consultez les journaux de la tâche."
        )

    if job_name == "bronze_incremental":
        summary = None

        for line in (result.get("stdout") or "").splitlines():
            if line.startswith("PIPELINE_SUMMARY="):
                summary = json.loads(line.split("=", 1)[1])

        if summary is None:
            raise AirflowException(
                "Le job Bronze n'a pas produit PIPELINE_SUMMARY."
            )

        LOGGER.info("Résumé ingestion Bronze : %s", summary)
        return summary

    return {"success": True, "job": job_name}


def choose_next_step(**context):
    """Décide si Silver et Gold doivent être recalculées."""
    summary = context["ti"].xcom_pull(
        task_ids="ingestion_bronze"
    ) or {}

    total_new = int(summary.get("total_new", 0))
    refresh_required = bool(summary.get("refresh_required", False))

    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}

    force_refresh = str(
        conf.get("force_refresh", False)
    ).strip().lower() in {"true", "1", "yes", "oui"}

    if force_refresh or total_new > 0 or refresh_required:
        LOGGER.info(
            "Actualisation requise : force_refresh=%s, "
            "total_new=%s, refresh_required=%s",
            force_refresh,
            total_new,
            refresh_required,
        )
        return "actualiser_silver"

    LOGGER.info(
        "Aucune nouveauté et aucune table dérivée manquante : "
        "Silver et Gold ne seront pas recalculées."
    )
    return "aucune_nouvelle_donnee"


with DAG(
    dag_id="medical_lakehouse_pipeline",
    description=(
        "Ingestion incrémentale PostgreSQL/CSV/JSONL vers Iceberg, "
        "puis Silver et Gold."
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["medical", "lakehouse", "spark", "iceberg"],
) as dag:

    ingestion_bronze = PythonOperator(
        task_id="ingestion_bronze",
        python_callable=execute_spark_job,
        op_kwargs={"job_name": "bronze_incremental"},
        execution_timeout=timedelta(hours=2),
    )

    decide = BranchPythonOperator(
        task_id="decider_si_nouvelles_donnees",
        python_callable=choose_next_step,
    )

    actualiser_silver = PythonOperator(
        task_id="actualiser_silver",
        python_callable=execute_spark_job,
        op_kwargs={"job_name": "silver_refresh"},
        execution_timeout=timedelta(hours=2),
    )

    actualiser_gold = PythonOperator(
        task_id="actualiser_gold",
        python_callable=execute_spark_job,
        op_kwargs={"job_name": "gold_refresh"},
        execution_timeout=timedelta(hours=2),
    )

    aucune_nouvelle_donnee = EmptyOperator(
        task_id="aucune_nouvelle_donnee"
    )

    fin_pipeline = EmptyOperator(
        task_id="fin_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    ingestion_bronze >> decide
    decide >> [actualiser_silver, aucune_nouvelle_donnee]
    actualiser_silver >> actualiser_gold >> fin_pipeline
    aucune_nouvelle_donnee >> fin_pipeline
