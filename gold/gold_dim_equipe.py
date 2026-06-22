from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — dim_equipe
# Clé naturelle : equipe_id
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Dim_Equipe")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df = spark.read.parquet(f"{SILVER_PATH}/equipes")

dim_equipe = df.select(
    F.col("equipe_id"),
    F.col("nom_equipe"),
    F.col("nom_court"),
    F.col("code_tla"),
    F.col("pays"),
    F.col("code_pays"),
    F.col("logo_url"),
    F.col("couleurs_club"),
    F.col("site_web"),
    F.col("selectionneur"),
    F.col("nationalite_selectionneur"),
    F.col("derniere_maj"),
)

dim_equipe.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.dim_equipe", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : dim_equipe ({dim_equipe.count()} lignes)")
spark.stop()
