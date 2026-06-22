from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — fact_buteur
# Granularité : 1 ligne par (joueur, equipe)
# FK : joueur_id → dim_joueur
#      equipe_id → dim_equipe
# Mesures : buts, assists, matchs_joues, penaltys, buts_par_match
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Fact_Buteur")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df = spark.read.parquet(f"{SILVER_PATH}/buteurs")

fact_buteur = df.select(
    F.col("joueur_id"),
    F.col("equipe_id"),
    F.col("buts"),
    F.col("assists"),
    F.col("matchs_joues"),
    F.col("penaltys"),
    F.col("buts_par_match"),
    F.col("derniere_maj"),
)

fact_buteur.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.fact_buteur", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : fact_buteur ({fact_buteur.count()} lignes)")
spark.stop()
