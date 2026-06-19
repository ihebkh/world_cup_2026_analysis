from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SILVER LAYER
# Lecture : Bronze Parquet
# Sortie  : Silver Parquet
# ============================================================

spark = (
    SparkSession.builder
    .appName("Silver_Joueurs")
    .getOrCreate()
)

BRONZE_PATH = r"./bronze"
SILVER_PATH = r"./silver"


def clean_string_columns(df):
    for column_name, column_type in df.dtypes:
        if column_type == "string":
            df = df.withColumn(column_name, F.trim(F.col(column_name)))
            df = df.withColumn(
                column_name,
                F.when(
                    (F.col(column_name) == "") |
                    (F.lower(F.col(column_name)).isin("nan", "none", "null")),
                    F.lit(None)
                ).otherwise(F.col(column_name))
            )
    return df


def to_int(column_name):
    return F.col(column_name).cast("int")


def to_long(column_name):
    return F.col(column_name).cast("long")


def to_date(column_name):
    return F.to_date(F.col(column_name), "yyyy-MM-dd")


def to_timestamp_seconds(column_name):
    return F.to_timestamp(F.col(column_name), "yyyy-MM-dd HH:mm:ss")


def to_timestamp_minutes(column_name):
    return F.to_timestamp(F.col(column_name), "yyyy-MM-dd HH:mm")

# =========================
# LECTURE BRONZE
# =========================

df = spark.read.parquet(f"{BRONZE_PATH}/joueurs")
df = clean_string_columns(df)

# =========================
# TRANSFORMATION SILVER
# =========================

df_silver = (
    df.select(
        to_long("joueur_id").alias("joueur_id"),
        to_long("team_id").alias("equipe_id"),
        F.col("equipe").alias("nom_equipe"),
        F.col("nom"),
        F.col("prenom"),
        F.col("nom_complet"),
        to_date("date_naissance").alias("date_naissance"),
        F.col("nationalite"),
        F.col("position"),
        to_int("numero_maillot").alias("numero_maillot"),
        to_timestamp_seconds("derniere_maj").alias("derniere_maj"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_ingestion_timestamp"),
        F.col("_batch_id")
    )
    .filter(F.col("joueur_id").isNotNull())
    .dropDuplicates(["joueur_id"])
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/joueurs")

print("Silver OK : joueurs")
spark.stop()
