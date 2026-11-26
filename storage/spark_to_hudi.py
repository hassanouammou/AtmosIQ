
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2

# 1. Initialiser Spark avec Apache Hudi
spark = (
    SparkSession.builder
    .appName("AtmosIQ-Bronze-to-Silver")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()
)

# 2. Configurer MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://atmosiq-minio:9000"))
hadoop_conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

try:
    # 3. Lire les fichiers JSON Bronze
    bronze_df = (
        spark.read
        .option("multiLine", "true")
        .json("s3a://bronze/meteo/*/*.json")
    )

    # 4. Nettoyer et valider les mesures
    silver_df = (
        bronze_df
        .select("station_id", "timestamp", "temperature", "humidity")
        .filter(
            col("station_id").isNotNull()
            & col("timestamp").isNotNull()
            & col("temperature").between(-40, 60)
            & col("humidity").between(0, 100)
        )
        .withColumn(
            "record_id",
            sha2(concat_ws("|", col("station_id"), col("timestamp")), 256)
        )
    )

    # 5. Configurer Apache Hudi
    hudi_options = {
        "hoodie.table.name": "meteo_silver",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "record_id",
        "hoodie.datasource.write.precombine.field": "timestamp",
        "hoodie.datasource.write.keygenerator.class":
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning": "false",
    }

    # 6. Enregistrer dans MinIO Silver
    (
        silver_df.write
        .format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save("s3a://silver/meteo/")
    )

    print("SUCCESS: AtmosIQ Bronze -> Hudi Silver")

finally:
    spark.stop()
