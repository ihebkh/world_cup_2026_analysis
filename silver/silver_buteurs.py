from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SILVER LAYER - Buteurs
# Lecture : ../bronze/buteurs  (parquet)
# Sortie  : ./buteurs          (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Silver_Buteurs")
    .getOrCreate()
)

BRONZE_PATH = r"../bronze"
SILVER_PATH = r"."


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


def to_int(c):  return F.col(c).cast("int")
def to_long(c): return F.col(c).cast("long")
def to_date(c): return F.to_date(F.col(c), "yyyy-MM-dd")
def to_ts_s(c): return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")


df = spark.read.parquet(f"{BRONZE_PATH}/buteurs")
df = clean_string_columns(df)

df_silver = (
    df.select(
        to_long("joueur_id").alias("joueur_id"),
        F.col("nom").alias("nom_joueur"),
        F.col("nationalite"),
        F.col("position"),
        to_date("date_naissance").alias("date_naissance"),
        to_long("equipe_id").alias("equipe_id"),
        F.col("equipe").alias("nom_equipe"),
        F.coalesce(to_int("buts"),         F.lit(0)).alias("buts"),
        F.coalesce(to_int("assists"),      F.lit(0)).alias("assists"),
        F.coalesce(to_int("matchs_joues"), F.lit(0)).alias("matchs_joues"),
        F.coalesce(to_int("penaltys"),     F.lit(0)).alias("penaltys"),
        to_ts_s("derniere_maj").alias("derniere_maj"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_ingestion_timestamp"),
        F.col("_batch_id"),
    )
    .filter(F.col("joueur_id").isNotNull())
    .dropDuplicates(["joueur_id", "equipe_id"])
    .withColumn(
        "buts_par_match",
        F.when(F.col("matchs_joues") > 0, F.round(F.col("buts") / F.col("matchs_joues"), 2))
         .otherwise(F.lit(0.0))
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/buteurs")

print("Silver OK : buteurs")
spark.stop()
