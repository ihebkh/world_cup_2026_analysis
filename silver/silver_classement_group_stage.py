from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SILVER LAYER - Classement Group Stage
# Lecture : ../bronze/classement_group_stage  (parquet)
# Sortie  : ./classement_group_stage          (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Silver_Classement_Group_Stage")
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
def to_ts_s(c): return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")


df = spark.read.parquet(f"{BRONZE_PATH}/classement_group_stage")
df = clean_string_columns(df)

df_silver = (
    df.select(
        F.col("groupe"),
        to_int("position").alias("position"),
        to_long("equipe_id").alias("equipe_id"),
        F.col("equipe").alias("nom_equipe"),
        to_int("matchs_joues").alias("matchs_joues"),
        to_int("victoires").alias("victoires"),
        to_int("nuls").alias("nuls"),
        to_int("defaites").alias("defaites"),
        to_int("buts_pour").alias("buts_pour"),
        to_int("buts_contre").alias("buts_contre"),
        to_int("difference_buts").alias("difference_buts"),
        to_int("points").alias("points"),
        F.col("forme"),
        to_ts_s("derniere_maj").alias("derniere_maj"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_ingestion_timestamp"),
        F.col("_batch_id"),
    )
    .filter(F.col("equipe_id").isNotNull())
    .dropDuplicates(["groupe", "equipe_id"])
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/classement_group_stage")

print("Silver OK : classement_group_stage")
spark.stop()
