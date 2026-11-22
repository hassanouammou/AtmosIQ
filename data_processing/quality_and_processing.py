from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Démarrer la session Spark
spark = SparkSession.builder.appName("AtmosIQ-Weather-Processing").getOrCreate()

# 2. Charger les données météo en DataFrame avec détection des types
df = spark.read.csv("data_processing/sample_weather.csv", header=True, inferSchema=True)

print("--- Données brutes ---")
df.show(truncate=False)

# 3. Contrôle qualité : marquer les lignes valides vs suspectes/aberrantes
df_checked = df.withColumn(
    "is_valid",
    (F.col("temperature").between(-40, 60))
    & (F.col("humidity").between(0, 100))
    & (F.col("pressure").between(870, 1085))
)

print("--- Après contrôle qualité ---")
df_checked.show(truncate=False)

# 4. Garder uniquement les mesures valides et calculer des agrégations par station
df_clean = df_checked.filter(F.col("is_valid") == True)

df_summary = df_clean.groupBy("station_id").agg(
    F.count("*").alias("nb_mesures_valides"),
    F.round(F.avg("temperature"), 2).alias("temp_moyenne"),
    F.round(F.avg("humidity"), 2).alias("humidite_moyenne"),
    F.round(F.avg("pressure"), 2).alias("pression_moyenne")
)

print("--- Synthèse par station (données propres) ---")
df_summary.show(truncate=False)

spark.stop()