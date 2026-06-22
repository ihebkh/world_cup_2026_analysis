from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — fact_classement
# Granularité : 1 ligne par (equipe, groupe)
# FK : equipe_id → dim_equipe
# Mesures : points, victoires, nuls, defaites, buts_pour, buts_contre, difference_buts
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Fact_Classement")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df = spark.read.parquet(f"{SILVER_PATH}/classement_group_stage")

fact_classement = df.select(
    F.col("equipe_id"),
    F.col("groupe"),
    F.col("position"),
    F.col("matchs_joues"),
    F.col("victoires"),
    F.col("nuls"),
    F.col("defaites"),
    F.col("buts_pour"),
    F.col("buts_contre"),
    F.col("difference_buts"),
    F.col("points"),
    F.col("forme"),
    F.col("derniere_maj"),
)

fact_classement.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.fact_classement", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : fact_classement ({fact_classement.count()} lignes)")
spark.stop()
