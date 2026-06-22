from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import JDBC_URL, JDBC_PROPERTIES, SILVER_PATH, PG_SCHEMA

# ============================================================
# GOLD — dim_date
# Clé surrogate : date_id = YYYYMMDD (entier)
# Dérivé depuis toutes les dates de matchs
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Dim_Date")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df_joues     = spark.read.parquet(f"{SILVER_PATH}/matchs_joues")
df_non_joues = spark.read.parquet(f"{SILVER_PATH}/matchs_non_joues")

dates = (
    df_joues.select("match_date")
    .union(df_non_joues.select("match_date"))
    .filter(F.col("match_date").isNotNull())
    .dropDuplicates(["match_date"])
)

dim_date = dates.select(
    F.date_format(F.col("match_date"), "yyyyMMdd").cast("int").alias("date_id"),
    F.col("match_date").alias("date_complete"),
    F.dayofmonth(F.col("match_date")).alias("jour"),
    F.month(F.col("match_date")).alias("mois"),
    F.date_format(F.col("match_date"), "MMMM").alias("nom_mois"),
    F.year(F.col("match_date")).alias("annee"),
    F.dayofweek(F.col("match_date")).alias("num_jour_semaine"),
    F.date_format(F.col("match_date"), "EEEE").alias("nom_jour_semaine"),
    F.weekofyear(F.col("match_date")).alias("semaine_annee"),
).orderBy("date_id")

dim_date.write \
    .option("truncate", "true") \
    .option("cascadeTruncate", "true") \
    .jdbc(url=JDBC_URL, table=f"{PG_SCHEMA}.dim_date", mode="overwrite", properties=JDBC_PROPERTIES)

print(f"Gold OK : dim_date ({dim_date.count()} lignes)")
spark.stop()
