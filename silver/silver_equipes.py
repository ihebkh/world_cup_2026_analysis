from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SILVER LAYER - Equipes
# Lecture : ../bronze/equipes  (parquet)
# Sortie  : ./equipes          (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Silver_Equipes")
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


def to_int(c):       return F.col(c).cast("int")
def to_long(c):      return F.col(c).cast("long")
def to_date(c):      return F.to_date(F.col(c), "yyyy-MM-dd")
def to_ts_s(c):      return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")
def to_ts_m(c):      return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm")


df = spark.read.parquet(f"{BRONZE_PATH}/equipes")
df = clean_string_columns(df)

df_silver = (
    df.select(
        to_long("team_id").alias("equipe_id"),
        F.col("nom").alias("nom_equipe"),
        F.col("nom_court"),
        F.upper(F.col("code_tla")).alias("code_tla"),
        F.col("pays"),
        F.upper(F.col("code_pays")).alias("code_pays"),
        F.col("logo_url"),
        F.col("couleurs_club"),
        F.col("site_web"),
        F.col("selectionneur"),
        F.col("nationalite_selectionneur"),
        to_ts_s("derniere_maj").alias("derniere_maj"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_ingestion_timestamp"),
        F.col("_batch_id"),
    )
    .filter(F.col("equipe_id").isNotNull())
    .dropDuplicates(["equipe_id"])
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/equipes")

print("Silver OK : equipes")
spark.stop()
