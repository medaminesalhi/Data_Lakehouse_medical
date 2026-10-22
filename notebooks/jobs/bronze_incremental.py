
"""
Ingestion Bronze incrémentale pour le laboratoire médical.

Sources :
- Patients : PostgreSQL, clé patient_id.
- Consultations : CSV, clé Id.
- Observations : CSV converti en JSONL, dédoublonnage par empreinte.
"""
import csv
import glob
import json
import os
import shutil
from pathlib import Path

from pyspark.sql import functions as F
from lakehouse_common import create_spark, align_to_table, hash_rows


# 1. Emplacements des données
DATA = Path("/workspace/notebooks/data")
SYNTHEA = DATA / "synthea"
PREPARED = DATA / "prepared"

INCOMING_ENCOUNTERS = DATA / "incoming" / "encounters"
INCOMING_OBSERVATIONS = DATA / "incoming" / "observations"

PROCESSED_ENCOUNTERS = DATA / "processed" / "encounters"
PROCESSED_OBSERVATIONS = DATA / "processed" / "observations"


# 2. Tables Bronze dans Iceberg
PATIENTS_TABLE = "medical.bronze.patients"
CONSULTATIONS_TABLE = "medical.bronze.consultations"
OBSERVATIONS_TABLE = "medical.bronze.observations"


# 3. Connexion PostgreSQL
JDBC_URL = os.getenv(
    "PG_JDBC_URL",
    "jdbc:postgresql://postgres:5432/hospital"
)
PG_USER = os.getenv("PGUSER", "user")
PG_PASSWORD = os.getenv("PGPASSWORD", "0000")
PG_TABLE = os.getenv("PG_TABLE", "public.patients_source")


def read_patients(spark):
    """Lit la table patients_source depuis PostgreSQL."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", PG_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def assert_unique_key(df, key, label):
    """Refuse les clés absentes ou associées à des lignes contradictoires."""
    if key not in df.columns:
        raise ValueError(
            f"Clé {key!r} absente de {label}. "
            f"Colonnes disponibles : {df.columns}"
        )

    null_keys = df.filter(
        F.col(key).isNull()
        | (F.trim(F.col(key).cast("string")) == "")
    ).limit(1).count()

    if null_keys:
        raise ValueError(
            f"{label} contient une clé {key!r} vide ou nulle."
        )

    columns = [
        F.col(f"`{c.replace('`', '``')}`")
        for c in df.columns
    ]

    variants = (
        df.groupBy(key)
        .agg(
            F.countDistinct(
                F.to_json(F.struct(*columns))
            ).alias("_variants")
        )
        .filter(F.col("_variants") > 1)
        .limit(1)
        .count()
    )

    if variants:
        raise ValueError(
            f"{label} contient plusieurs lignes différentes "
            f"avec la même clé {key!r}. "
            "Le lot est refusé pour éviter une perte silencieuse."
        )


def append_new_by_key(spark, source_df, table_name, key, label):
    """Ajoute uniquement les clés absentes de la table Iceberg."""
    assert_unique_key(source_df, key, label)

    raw_source_count = source_df.count()
    source_df = source_df.dropDuplicates([key])
    source_count = source_df.count()

    # Première exécution : création de la table.
    if not spark.catalog.tableExists(table_name):
        (
            source_df.writeTo(table_name)
            .using("iceberg")
            .create()
        )

        inserted = spark.table(table_name).count()
        print(f"Création {table_name} : {inserted} lignes.")

        return {
            "source_rows": source_count,
            "new_rows": inserted,
        }

    # Exécutions suivantes : comparaison avec les clés existantes.
    target = spark.table(table_name)
    source = align_to_table(source_df, target)

    target_keys = (
        target.select(F.col(key))
        .where(F.col(key).isNotNull())
        .dropDuplicates([key])
    )

    # left_anti conserve les lignes source sans clé correspondante dans Bronze.
    new_rows = source.join(
        target_keys,
        on=key,
        how="left_anti"
    )

    new_count = new_rows.count()

    if new_count > 0:
        new_rows.writeTo(table_name).append()

    print(
        f"{label} : lignes lues={raw_source_count}, "
        f"lignes distinctes={source_count}, "
        f"nouvelles lignes={new_count}, "
        f"table={table_name}"
    )

    return {
        "source_rows": raw_source_count,
        "new_rows": new_count,
    }


def refresh_observations_jsonl():
    """Convertit observations.csv en JSONL sans modifier ses valeurs."""
    source = SYNTHEA / "observations.csv"
    destination = PREPARED / "observations.jsonl"

    if not source.is_file():
        raise FileNotFoundError(
            f"Fichier source introuvable : {source}"
        )

    PREPARED.mkdir(parents=True, exist_ok=True)

    count = 0

    with source.open(
        "r", encoding="utf-8-sig", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError(
                f"Le CSV ne contient pas d'en-tête : {source}"
            )

        with destination.open("w", encoding="utf-8") as json_file:
            for row in reader:
                json_file.write(
                    json.dumps(row, ensure_ascii=False) + "\n"
                )
                count += 1

    print(f"JSONL préparé depuis observations.csv : {count} lignes")
    return destination


def move_to_processed(paths, destination_dir):
    """Archive les fichiers entrants après une ingestion réussie."""
    destination_dir.mkdir(parents=True, exist_ok=True)

    for raw_path in paths:
        source = Path(raw_path)

        if not source.exists():
            continue

        destination = destination_dir / source.name

        if destination.exists():
            destination = destination_dir / (
                f"{source.stem}_{os.getpid()}{source.suffix}"
            )

        shutil.move(str(source), str(destination))

        print(
            f"Fichier archivé : {source.name} -> {destination}"
        )


def main():
    # Créer les dossiers nécessaires.
    for folder in [
        INCOMING_ENCOUNTERS,
        INCOMING_OBSERVATIONS,
        PROCESSED_ENCOUNTERS,
        PROCESSED_OBSERVATIONS,
    ]:
        folder.mkdir(parents=True, exist_ok=True)

    # Lister les fichiers arrivés depuis la dernière ingestion.
    incoming_encounter_files = sorted(
        glob.glob(str(INCOMING_ENCOUNTERS / "*.csv"))
    )
    incoming_observation_files = sorted(
        glob.glob(str(INCOMING_OBSERVATIONS / "*.jsonl"))
    )

    spark = create_spark("MedicalLakehouse-Bronze-Incremental")
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS medical.bronze")

        # SOURCE 1 : PostgreSQL -> Bronze patients
        df_patients = read_patients(spark)

        result_patients = append_new_by_key(
            spark,
            df_patients,
            PATIENTS_TABLE,
            "patient_id",
            "Patients PostgreSQL",
        )

        # SOURCE 2 : CSV -> Bronze consultations
        base_encounters = SYNTHEA / "encounters.csv"

        if not base_encounters.is_file():
            raise FileNotFoundError(
                f"Fichier source introuvable : {base_encounters}"
            )

        encounter_paths = [
            str(base_encounters)
        ] + incoming_encounter_files

        df_consultations = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(encounter_paths)
        )

        result_consultations = append_new_by_key(
            spark,
            df_consultations,
            CONSULTATIONS_TABLE,
            "Id",
            "Consultations CSV",
        )

        # SOURCE 3 : CSV -> JSONL -> Bronze observations
        prepared_json = refresh_observations_jsonl()

        observation_paths = [
            str(prepared_json)
        ] + incoming_observation_files

        df_observations = spark.read.json(observation_paths)

        if not spark.catalog.tableExists(OBSERVATIONS_TABLE):
            df_hashed = hash_rows(
                df_observations,
                df_observations.columns
            )

            new_observations = (
                df_hashed
                .dropDuplicates(["_row_hash"])
                .drop("_row_hash")
            )

            (
                new_observations.writeTo(OBSERVATIONS_TABLE)
                .using("iceberg")
                .create()
            )

            inserted_observations = new_observations.count()

            result_observations = {
                "source_rows": df_observations.count(),
                "new_rows": inserted_observations,
            }

            print(
                f"Création {OBSERVATIONS_TABLE} : "
                f"{inserted_observations} lignes."
            )

        else:
            target_observations = spark.table(OBSERVATIONS_TABLE)

            aligned_observations = align_to_table(
                df_observations,
                target_observations
            )

            columns = target_observations.columns

            source_hashed = (
                hash_rows(aligned_observations, columns)
                .dropDuplicates(["_row_hash"])
            )

            target_hashed = hash_rows(
                target_observations.select(*columns),
                columns
            )

            existing_hashes = (
                target_hashed
                .select("_row_hash")
                .dropDuplicates(["_row_hash"])
            )

            new_observations = (
                source_hashed
                .join(
                    existing_hashes,
                    on="_row_hash",
                    how="left_anti"
                )
                .drop("_row_hash")
            )

            inserted_observations = new_observations.count()

            if inserted_observations > 0:
                new_observations.writeTo(
                    OBSERVATIONS_TABLE
                ).append()

            result_observations = {
                "source_rows": df_observations.count(),
                "new_rows": inserted_observations,
            }

            print(
                f"Observations JSONL : "
                f"source={result_observations['source_rows']}, "
                f"nouvelles lignes={inserted_observations}, "
                f"table={OBSERVATIONS_TABLE}"
            )

        # Archiver les fichiers entrants après les trois ingestions.
        move_to_processed(
            incoming_encounter_files,
            PROCESSED_ENCOUNTERS
        )

        move_to_processed(
            incoming_observation_files,
            PROCESSED_OBSERVATIONS
        )

        # Résumé destiné à Airflow.
        total_new = (
            result_patients["new_rows"]
            + result_consultations["new_rows"]
            + result_observations["new_rows"]
        )

        tables_to_check = [
            "medical.silver.patients",
            "medical.silver.consultations",
            "medical.silver.observations",
            "medical.quarantine.patients",
            "medical.quarantine.consultations",
            "medical.quarantine.observations",
            "medical.gold.consultations_par_categorie",
            "medical.gold.activite_mensuelle",
            "medical.gold.analyses_par_type",
            "medical.gold.patients_par_genre",
        ]

        refresh_required = not all(
            spark.catalog.tableExists(table)
            for table in tables_to_check
        )

        summary = {
            "new": {
                "patients": result_patients["new_rows"],
                "consultations": result_consultations["new_rows"],
                "observations": result_observations["new_rows"],
            },
            "total_new": total_new,
            "refresh_required": refresh_required,
        }

        print(
            "PIPELINE_SUMMARY="
            + json.dumps(summary, ensure_ascii=False)
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
