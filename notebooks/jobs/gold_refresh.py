
"""Recalcule et actualise les tables analytiques Gold depuis Silver."""
from pyspark.sql import functions as F

from lakehouse_common import create_spark, write_full_refresh


def main():
    spark = create_spark("MedicalLakehouse-Gold-Refresh")
    spark.sparkContext.setLogLevel("WARN")

    try:
        tables_silver = {
            "patients": "medical.silver.patients",
            "consultations": "medical.silver.consultations",
            "observations": "medical.silver.observations",
        }

        for nom, table in tables_silver.items():
            if not spark.catalog.tableExists(table):
                raise ValueError(f"Table Silver absente : {table}")
            print(f"{table}: {spark.table(table).count()} lignes")

        df_patients = spark.table(tables_silver["patients"])
        df_consultations = spark.table(tables_silver["consultations"])
        df_observations = spark.table(tables_silver["observations"])

        required = {
            "patients": {"patient_id", "gender"},
            "consultations": {
                "consultation_id", "patient_id", "date_debut", "categorie"
            },
            "observations": {
                "observation_id", "patient_id", "categorie", "code",
                "description", "type_observation",
            },
        }

        for name, df in [
            ("patients", df_patients),
            ("consultations", df_consultations),
            ("observations", df_observations),
        ]:
            missing = required[name] - set(df.columns)
            if missing:
                raise ValueError(
                    f"Colonnes Silver manquantes pour {name}: {sorted(missing)}"
                )

        spark.sql("CREATE NAMESPACE IF NOT EXISTS medical.gold")

        # 1. Consultations par catégorie
        df_gold_categories = spark.sql("""
            WITH consultations AS (
                SELECT
                    consultation_id,
                    patient_id,
                    COALESCE(
                        NULLIF(TRIM(categorie), ''),
                        'Non renseignée'
                    ) AS categorie
                FROM medical.silver.consultations
            )
            SELECT
                categorie,
                COUNT(*) AS nombre_consultations,
                COUNT(DISTINCT patient_id) AS nombre_patients
            FROM consultations
            GROUP BY categorie
        """)

        write_full_refresh(
            spark,
            df_gold_categories,
            "medical.gold.consultations_par_categorie"
        )

        # 2. Activité mensuelle
        df_gold_mensuel = spark.sql("""
            SELECT
                DATE_FORMAT(date_debut, 'yyyy-MM') AS mois,
                COUNT(*) AS nombre_consultations,
                COUNT(DISTINCT patient_id) AS nombre_patients
            FROM medical.silver.consultations
            WHERE date_debut IS NOT NULL
            GROUP BY DATE_FORMAT(date_debut, 'yyyy-MM')
        """)

        write_full_refresh(
            spark,
            df_gold_mensuel,
            "medical.gold.activite_mensuelle"
        )

        # 3. Observations par type
        df_gold_analyses = spark.sql("""
            WITH observations AS (
                SELECT
                    patient_id,
                    COALESCE(
                        NULLIF(TRIM(categorie), ''),
                        'Non renseignée'
                    ) AS categorie,
                    code,
                    COALESCE(
                        NULLIF(TRIM(description), ''),
                        'Sans description'
                    ) AS description,
                    COALESCE(
                        NULLIF(TRIM(type_observation), ''),
                        'Non renseigné'
                    ) AS type_observation
                FROM medical.silver.observations
            )
            SELECT
                categorie,
                code,
                description,
                type_observation,
                COUNT(*) AS nombre_observations,
                COUNT(DISTINCT patient_id) AS nombre_patients
            FROM observations
            GROUP BY categorie, code, description, type_observation
        """)

        write_full_refresh(
            spark,
            df_gold_analyses,
            "medical.gold.analyses_par_type"
        )

        # 4. Patients par genre
        df_gold_genre = spark.sql("""
            WITH patients AS (
                SELECT
                    COALESCE(
                        NULLIF(TRIM(gender), ''),
                        'Non renseigné'
                    ) AS genre
                FROM medical.silver.patients
            )
            SELECT
                genre,
                COUNT(*) AS nombre_patients
            FROM patients
            GROUP BY genre
        """)

        write_full_refresh(
            spark,
            df_gold_genre,
            "medical.gold.patients_par_genre"
        )

        # 5. Contrôler les totaux calculés
        checks = [
            (
                "consultations par catégorie",
                spark.table("medical.silver.consultations").count(),
                spark.sql("""
                    SELECT COALESCE(SUM(nombre_consultations), 0) AS n
                    FROM medical.gold.consultations_par_categorie
                """).first()["n"],
            ),
            (
                "activité mensuelle",
                spark.table("medical.silver.consultations")
                .filter(F.col("date_debut").isNotNull()).count(),
                spark.sql("""
                    SELECT COALESCE(SUM(nombre_consultations), 0) AS n
                    FROM medical.gold.activite_mensuelle
                """).first()["n"],
            ),
            (
                "observations par type",
                spark.table("medical.silver.observations").count(),
                spark.sql("""
                    SELECT COALESCE(SUM(nombre_observations), 0) AS n
                    FROM medical.gold.analyses_par_type
                """).first()["n"],
            ),
            (
                "patients par genre",
                spark.table("medical.silver.patients").count(),
                spark.sql("""
                    SELECT COALESCE(SUM(nombre_patients), 0) AS n
                    FROM medical.gold.patients_par_genre
                """).first()["n"],
            ),
        ]

        for label, expected, actual in checks:
            print(
                f"CONTROLE GOLD {label}: "
                f"Silver={expected}, Gold={actual}"
            )

            if expected != actual:
                raise ValueError(
                    f"Contrôle Gold échoué ({label}): "
                    f"Silver={expected}, Gold={actual}"
                )

        for table in [
            "medical.gold.consultations_par_categorie",
            "medical.gold.activite_mensuelle",
            "medical.gold.analyses_par_type",
            "medical.gold.patients_par_genre",
        ]:
            print(f"{table}: {spark.table(table).count()} lignes")

        print("GOLD_REFRESH_SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
