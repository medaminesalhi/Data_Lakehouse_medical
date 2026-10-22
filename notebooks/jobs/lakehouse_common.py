
import os
from pyspark.sql import SparkSession, functions as F


def create_spark(app_name: str) -> SparkSession:
    """Crée une session Spark configurée pour le catalogue Iceberg médical."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.medical", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.medical.type", "rest")
        .config("spark.sql.catalog.medical.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.medical.warehouse", "s3://medical-warehouse/")
        .config("spark.sql.catalog.medical.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.medical.s3.endpoint", "http://rustfs:9000")
        .config("spark.sql.catalog.medical.s3.path-style-access", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def align_to_table(df, target):
    """Vérifie le schéma et aligne les colonnes sur une table existante."""
    source_cols = set(df.columns)
    target_cols = set(target.columns)

    missing = sorted(target_cols - source_cols)
    extra = sorted(source_cols - target_cols)

    if missing or extra:
        raise ValueError(
            "Schéma source différent du schéma existant. "
            f"Colonnes manquantes={missing}; "
            f"colonnes inattendues={extra}."
        )

    result = df.select(*target.columns)

    for field in target.schema.fields:
        result = result.withColumn(
            field.name,
            F.col(field.name).cast(field.dataType)
        )

    return result.select(*target.columns)


def write_full_refresh(spark, df, table_name: str):
    """
    Crée une table si elle n'existe pas ;
    sinon remplace son contenu.
    Réservé aux tables dérivées Silver, Quarantine et Gold.
    """
    if spark.catalog.tableExists(table_name):
        df.writeTo(table_name).overwrite(F.lit(True))
        print(f"Table actualisée : {table_name}")
    else:
        df.writeTo(table_name).using("iceberg").create()
        print(f"Table créée : {table_name}")


def hash_rows(df, columns):
    """Calcule une empreinte stable du contenu de chaque ligne."""
    fields = [
        F.col(f"`{name.replace('`', '``')}`")
        for name in columns
    ]

    return df.withColumn(
        "_row_hash",
        F.sha2(F.to_json(F.struct(*fields)), 256)
    )
