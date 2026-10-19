
FROM apache/spark:3.5.5-java17

USER root

# Installer Jupyter et les utilitaires
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
    jupyterlab==4.4.10 \
    ipykernel==6.30.1

# Ajouter la prise en charge d'Apache Iceberg
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    -o /opt/spark/jars/iceberg-spark-runtime.jar

# Ajouter les bibliothèques S3 d'Iceberg
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar \
    -o /opt/spark/jars/iceberg-aws-bundle.jar

# Driver JDBC PostgreSQL
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    -o /opt/spark/jars/postgresql-jdbc.jar

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_LOCAL_IP=127.0.0.1

RUN mkdir -p /workspace/notebooks && \
    chown -R 185:0 /workspace

USER spark

WORKDIR /workspace/notebooks

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
