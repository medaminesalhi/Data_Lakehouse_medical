
# Premier job Spark exécuté sans Jupyter

from pyspark.sql import SparkSession

# Démarrer Spark avec notre catalogue Iceberg
spark = (
    SparkSession.builder
    .appName("TestSparkAirflow")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.medical",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.medical.type", "rest")
    .config(
        "spark.sql.catalog.medical.uri",
        "http://iceberg-rest:8181"
    )
    .config(
        "spark.sql.catalog.medical.warehouse",
        "s3://medical-warehouse/"
    )
    .config(
        "spark.sql.catalog.medical.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.medical.s3.endpoint",
        "http://rustfs:9000"
    )
    .config(
        "spark.sql.catalog.medical.s3.path-style-access",
        "true"
    )
    .getOrCreate()
)

try:
    print("Version Spark :", spark.version)

    # Vérifier les tables Bronze existantes
    print("=== TABLES BRONZE ===")

    spark.sql(
        "SHOW TABLES IN medical.bronze"
    ).show(truncate=False)

    print("Job Spark terminé avec succès !")

finally:
    # Libérer les ressources Spark
    spark.stop()
