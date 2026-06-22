from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — fact_match
# Union matchs_joues + matchs_non_joues
# FK : date_id     → dim_date
#      equipe_*_id → dim_equipe
#      stade_id    → dim_stade  (hash surrogate, même logique que gold_dim_stade)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Fact_Match")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

COLS = [
    "match_id", "competition", "journee", "statut",
    "date_heure", "match_date",
    "equipe_domicile_id", "equipe_domicile",
    "equipe_exterieur_id", "equipe_exterieur",
    "score_domicile_mi_temps", "score_exterieur_mi_temps",
    "score_domicile_final", "score_exterieur_final",
    "stade", "ville", "arbitre", "derniere_maj",
    "is_match_joue", "total_buts", "resultat_match", "vainqueur_equipe_id",
]

df_joues     = spark.read.parquet(f"{SILVER_PATH}/matchs_joues").select(*COLS)
df_non_joues = spark.read.parquet(f"{SILVER_PATH}/matchs_non_joues").select(*COLS)

df_all = df_joues.union(df_non_joues)

fact_match = df_all.select(
    F.col("match_id"),

    # FK → dim_date
    F.date_format(F.col("match_date"), "yyyyMMdd").cast("int").alias("date_id"),

    # FK → dim_equipe
    F.col("equipe_domicile_id"),
    F.col("equipe_exterieur_id"),
    F.col("vainqueur_equipe_id"),

    # FK → dim_stade  (même hash que gold_dim_stade)
    F.abs(F.hash(
        F.coalesce(F.col("stade"), F.lit("")),
        F.coalesce(F.col("ville"),  F.lit(""))
    )).alias("stade_id"),

    # Mesures et attributs du match
    F.col("competition"),
    F.col("journee"),
    F.col("statut"),
    F.col("date_heure"),
    F.col("score_domicile_mi_temps"),
    F.col("score_exterieur_mi_temps"),
    F.col("score_domicile_final"),
    F.col("score_exterieur_final"),
    F.col("total_buts"),
    F.col("resultat_match"),
    F.col("is_match_joue"),
    F.col("arbitre"),
    F.col("derniere_maj"),
)

fact_match.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.fact_match", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : fact_match ({fact_match.count()} lignes)")
spark.stop()
