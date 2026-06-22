from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — dim_stade
# Clé surrogate : stade_id = abs(hash(nom_stade, ville))
# Dérivé depuis l'union matchs_joues + matchs_non_joues
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Dim_Stade")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df_joues     = spark.read.parquet(f"{SILVER_PATH}/matchs_joues")
df_non_joues = spark.read.parquet(f"{SILVER_PATH}/matchs_non_joues")

stades = (
    df_joues.select("stade", "ville")
    .union(df_non_joues.select("stade", "ville"))
    .filter(F.col("stade").isNotNull() | F.col("ville").isNotNull())
    .dropDuplicates(["stade", "ville"])
)

dim_stade = stades.select(
    F.abs(F.hash(
        F.coalesce(F.col("stade"), F.lit("")),
        F.coalesce(F.col("ville"),  F.lit(""))
    )).alias("stade_id"),
    F.col("stade").alias("nom_stade"),
    F.col("ville"),
)

dim_stade.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.dim_stade", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : dim_stade ({dim_stade.count()} lignes)")
spark.stop()
