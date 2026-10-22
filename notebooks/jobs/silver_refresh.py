
"""Actualisation complète des tables Silver et Quarantine depuis Bronze."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lakehouse_common import create_spark, write_full_refresh


def verifier_colonnes(df, colonnes, source):
    manquantes = [c for c in colonnes if c not in df.columns]
    if manquantes:
        raise ValueError(
            f"Colonnes manquantes dans {source}: {manquantes}. "
            f"Colonnes disponibles: {df.columns}"
        )


def convertir_timestamp(nom_colonne):
    return F.to_timestamp(F.trim(F.col(nom_colonne)))


def main():
    spark = create_spark("MedicalLakehouse-Silver-Refresh")
    spark.sparkContext.setLogLevel("WARN")

    try:
        noms_bronze = {
            "patients": "medical.bronze.patients",
            "consultations": "medical.bronze.consultations",
            "observations": "medical.bronze.observations",
        }

        for nom, table in noms_bronze.items():
            if not spark.catalog.tableExists(table):
                raise ValueError(f"Table Bronze absente : {table}")
            print(f"{nom}: {spark.table(table).count()} lignes Bronze")

        df_patients_bronze = spark.table(noms_bronze["patients"])
        df_consultations_bronze = spark.table(noms_bronze["consultations"])
        df_observations_bronze = spark.table(noms_bronze["observations"])

        # PATIENTS : normaliser les champs et contrôler la date de naissance.
        verifier_colonnes(
            df_patients_bronze,
            ["patient_id", "birthdate", "gender", "city"],
            "Bronze patients",
        )

        df_patients_prepares = (
            df_patients_bronze
            .select(
                F.trim(F.col("patient_id")).alias("patient_id"),
                F.trim(F.col("birthdate")).alias("birthdate_source"),
                F.upper(F.trim(F.col("gender"))).alias("gender"),
                F.trim(F.col("city")).alias("city"),
            )
            .withColumn("birthdate", F.to_date(F.col("birthdate_source")))
            .withColumn(
                "motif_rejet",
                F.when(
                    F.col("patient_id").isNull()
                    | (F.col("patient_id") == ""),
                    "Identifiant patient manquant",
                )
                .when(
                    F.col("birthdate").isNull(),
                    "Date de naissance invalide",
                )
                .when(
                    F.col("birthdate") > F.current_date(),
                    "Date de naissance future",
                ),
            )
        )

        patients_invalides = df_patients_prepares.filter(
            F.col("motif_rejet").isNotNull()
        )

        fenetre_patients = Window.partitionBy("patient_id").orderBy(
            F.col("birthdate_source"), F.col("gender"), F.col("city")
        )

        patients_classes = (
            df_patients_prepares
            .filter(F.col("motif_rejet").isNull())
            .withColumn("rang", F.row_number().over(fenetre_patients))
        )

        df_patients_silver = (
            patients_classes
            .filter(F.col("rang") == 1)
            .select("patient_id", "birthdate", "gender", "city")
        )

        patients_doublons = (
            patients_classes
            .filter(F.col("rang") > 1)
            .drop("rang")
            .withColumn(
                "motif_rejet",
                F.lit("Identifiant patient en double"),
            )
        )

        df_patients_rejets = patients_invalides.unionByName(
            patients_doublons
        )

        # CONSULTATIONS : convertir les dates et vérifier le patient.
        verifier_colonnes(
            df_consultations_bronze,
            [
                "Id", "PATIENT", "START", "STOP",
                "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
            ],
            "Bronze consultations",
        )

        df_consultations_prepares = (
            df_consultations_bronze
            .select(
                F.trim(F.col("Id")).alias("consultation_id"),
                F.trim(F.col("PATIENT")).alias("patient_id"),
                F.trim(F.col("START")).alias("debut_source"),
                F.trim(F.col("STOP")).alias("fin_source"),
                F.trim(F.col("ENCOUNTERCLASS")).alias("categorie"),
                F.trim(F.col("CODE")).alias("code"),
                F.trim(F.col("DESCRIPTION")).alias("description"),
            )
            .withColumn(
                "date_debut", convertir_timestamp("debut_source")
            )
            .withColumn(
                "date_fin", convertir_timestamp("fin_source")
            )
        )

        df_ids_patients = (
            df_patients_silver
            .select("patient_id")
            .withColumn("patient_existe", F.lit(True))
        )

        df_consultations_controlees = (
            df_consultations_prepares
            .join(df_ids_patients, on="patient_id", how="left")
            .withColumn(
                "motif_rejet",
                F.when(
                    F.col("consultation_id").isNull()
                    | (F.col("consultation_id") == ""),
                    "Identifiant consultation manquant",
                )
                .when(
                    F.col("patient_id").isNull()
                    | (F.col("patient_id") == ""),
                    "Patient manquant",
                )
                .when(
                    F.col("patient_existe").isNull(),
                    "Patient absent de Silver",
                )
                .when(
                    F.col("date_debut").isNull(),
                    "Date de début invalide",
                )
                .when(
                    F.col("fin_source").isNotNull()
                    & (F.col("fin_source") != "")
                    & F.col("date_fin").isNull(),
                    "Date de fin invalide",
                )
                .when(
                    F.col("date_fin") < F.col("date_debut"),
                    "Fin antérieure au début",
                ),
            )
            .drop("patient_existe")
        )

        consultations_invalides = df_consultations_controlees.filter(
            F.col("motif_rejet").isNotNull()
        )

        fenetre_consultations = Window.partitionBy(
            "consultation_id"
        ).orderBy("date_debut", "patient_id", "code", "description")

        consultations_classes = (
            df_consultations_controlees
            .filter(F.col("motif_rejet").isNull())
            .withColumn("rang", F.row_number().over(fenetre_consultations))
        )

        df_consultations_silver = (
            consultations_classes
            .filter(F.col("rang") == 1)
            .select(
                "consultation_id", "patient_id", "date_debut",
                "date_fin", "categorie", "code", "description",
            )
        )

        consultations_doublons = (
            consultations_classes
            .filter(F.col("rang") > 1)
            .drop("rang")
            .withColumn(
                "motif_rejet",
                F.lit("Identifiant consultation en double"),
            )
        )

        df_consultations_rejets = consultations_invalides.unionByName(
            consultations_doublons
        )

        # OBSERVATIONS : calculer un identifiant et vérifier les relations.
        verifier_colonnes(
            df_observations_bronze,
            [
                "DATE", "PATIENT", "ENCOUNTER", "CATEGORY",
                "CODE", "DESCRIPTION", "VALUE", "UNITS", "TYPE",
            ],
            "Bronze observations",
        )

        colonnes_bronze = [
            F.col(f"`{nom.replace('`', '``')}`")
            for nom in df_observations_bronze.columns
        ]

        df_observations_prepares = (
            df_observations_bronze
            .select(
                F.sha2(
                    F.to_json(F.struct(*colonnes_bronze)), 256
                ).alias("observation_id"),
                F.trim(F.col("PATIENT")).alias("patient_id"),
                F.trim(F.col("ENCOUNTER")).alias("consultation_id"),
                F.trim(F.col("DATE")).alias("date_source"),
                F.trim(F.col("CATEGORY")).alias("categorie"),
                F.trim(F.col("CODE")).alias("code"),
                F.trim(F.col("DESCRIPTION")).alias("description"),
                F.trim(F.col("VALUE")).alias("valeur"),
                F.trim(F.col("UNITS")).alias("unite"),
                F.trim(F.col("TYPE")).alias("type_observation"),
            )
            .withColumn(
                "date_observation",
                convertir_timestamp("date_source"),
            )
            .withColumn(
                "valeur_numerique",
                F.col("valeur").cast("double"),
            )
        )

        df_observations_controlees = (
            df_observations_prepares
            .join(df_ids_patients, on="patient_id", how="left")
            .withColumn(
                "motif_rejet",
                F.when(
                    F.col("patient_id").isNull()
                    | (F.col("patient_id") == ""),
                    "Patient manquant",
                )
                .when(
                    F.col("patient_existe").isNull(),
                    "Patient absent de Silver",
                )
                .when(
                    F.col("date_observation").isNull(),
                    "Date d'observation invalide",
                )
                .when(
                    F.col("code").isNull() | (F.col("code") == ""),
                    "Code d'observation manquant",
                ),
            )
            .drop("patient_existe")
        )

        observations_invalides = df_observations_controlees.filter(
            F.col("motif_rejet").isNotNull()
        )

        fenetre_observations = Window.partitionBy(
            "observation_id"
        ).orderBy("date_observation", "patient_id")

        observations_classes = (
            df_observations_controlees
            .filter(F.col("motif_rejet").isNull())
            .withColumn("rang", F.row_number().over(fenetre_observations))
        )

        df_observations_silver = (
            observations_classes
            .filter(F.col("rang") == 1)
            .select(
                "observation_id", "patient_id", "consultation_id",
                "date_observation", "categorie", "code", "description",
                "valeur", "unite", "type_observation",
                "valeur_numerique",
            )
        )

        observations_doublons = (
            observations_classes
            .filter(F.col("rang") > 1)
            .drop("rang")
            .withColumn(
                "motif_rejet",
                F.lit("Ligne d'observation strictement identique"),
            )
        )

        df_observations_rejets = observations_invalides.unionByName(
            observations_doublons
        )

        # Actualiser Silver et Quarantine depuis l'ensemble de Bronze.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS medical.silver")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS medical.quarantine")

        outputs = [
            (df_patients_silver, "medical.silver.patients"),
            (df_patients_rejets, "medical.quarantine.patients"),
            (df_consultations_silver, "medical.silver.consultations"),
            (df_consultations_rejets, "medical.quarantine.consultations"),
            (df_observations_silver, "medical.silver.observations"),
            (df_observations_rejets, "medical.quarantine.observations"),
        ]

        for df, table in outputs:
            write_full_refresh(spark, df, table)
            print(f"{table}: {spark.table(table).count()} lignes")

        # Contrôle de réconciliation des lignes.
        for nom in ["patients", "consultations", "observations"]:
            n_bronze = spark.table(f"medical.bronze.{nom}").count()
            n_silver = spark.table(f"medical.silver.{nom}").count()
            n_rejets = spark.table(
                f"medical.quarantine.{nom}"
            ).count()

            print(
                f"CONTROLE {nom}: Bronze={n_bronze}, "
                f"Silver={n_silver}, Quarantine={n_rejets}"
            )

            if n_bronze != n_silver + n_rejets:
                raise ValueError(
                    f"Contrôle de qualité incohérent pour {nom}: "
                    f"{n_bronze} != {n_silver} + {n_rejets}"
                )

        print("SILVER_REFRESH_SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
