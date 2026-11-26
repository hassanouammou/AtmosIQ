
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round, current_timestamp

# 1. Initialiser Spark
spark = (
    SparkSession.builder
    .appName("AtmosIQ-Silver-to-Gold")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .getOrCreate()
)

# 2. Configurer MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.endpoint",
    os.getenv("MINIO_ENDPOINT", "http://atmosiq-minio:9000")
)
hadoop_conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

try:
    # 3. Lire la table Hudi Silver
    silver_df = (
        spark.read
        .format("hudi")
        .load("s3a://silver/meteo/")
    )

    # 4. Calculer les statistiques par station
    gold_df = (
        silver_df
        .groupBy("station_id")
        .agg(
            round(avg("temperature"), 2).alias("avg_temperature"),
            round(avg("humidity"), 2).alias("avg_humidity"),
            count("*").alias("total_measures")
        )
        .withColumn("updated_at", current_timestamp())
    )

    # 5. Configurer Apache Hudi
    gold_options = {
        "hoodie.table.name": "meteo_gold",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "station_id",
        "hoodie.datasource.write.precombine.field": "updated_at",
        "hoodie.datasource.write.keygenerator.class":
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning": "false",
    }

    # 6. Écrire les statistiques dans Gold
    (
        gold_df.write
        .format("hudi")
        .options(**gold_options)
        .mode("append")
        .save("s3a://gold/meteo/")
    )

    print("SUCCESS: AtmosIQ Silver -> Hudi Gold")

    # 7. Vérifier les statistiques
    result = spark.read.format("hudi").load("s3a://gold/meteo/")

    result.select(
        "station_id",
        "avg_temperature",
        "avg_humidity",
        "total_measures"
    ).orderBy("station_id").show()

finally:
    spark.stop()
