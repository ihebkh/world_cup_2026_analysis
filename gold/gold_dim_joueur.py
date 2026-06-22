from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — dim_joueur
# Clé naturelle : joueur_id
# FK : equipe_id → dim_equipe
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Dim_Joueur")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df = spark.read.parquet(f"{SILVER_PATH}/joueurs")

dim_joueur = df.select(
    F.col("joueur_id"),
    F.col("equipe_id"),
    F.col("nom"),
    F.col("prenom"),
    F.col("nom_complet"),
    F.col("date_naissance"),
    F.col("nationalite"),
    F.col("position"),
    F.col("numero_maillot"),
    F.col("derniere_maj"),
)

dim_joueur.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.dim_joueur", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : dim_joueur ({dim_joueur.count()} lignes)")
spark.stop()
