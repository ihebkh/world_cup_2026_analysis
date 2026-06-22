from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# SILVER LAYER - Matchs Non Joues
# Lecture : ../bronze/matchs_non_joues  (parquet)
# Sortie  : ./matchs_non_joues          (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Silver_Matchs_Non_Joues")
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
def to_ts_m(c): return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm")
def to_ts_s(c): return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")


df = spark.read.parquet(f"{BRONZE_PATH}/matchs_non_joues")
df = clean_string_columns(df)

date_heure_col = to_ts_m("date_heure")

df_silver = (
    df.select(
        to_long("match_id").alias("match_id"),
        F.col("competition"),
        to_int("journee").alias("journee"),
        F.upper(F.col("statut")).alias("statut"),
        date_heure_col.alias("date_heure"),
        F.to_date(date_heure_col).alias("match_date"),
        to_long("equipe_domicile_id").alias("equipe_domicile_id"),
        F.col("equipe_domicile"),
        to_long("equipe_exterieur_id").alias("equipe_exterieur_id"),
        F.col("equipe_exterieur"),
        to_int("score_domicile_mi_temps").alias("score_domicile_mi_temps"),
        to_int("score_exterieur_mi_temps").alias("score_exterieur_mi_temps"),
        to_int("score_domicile_final").alias("score_domicile_final"),
        to_int("score_exterieur_final").alias("score_exterieur_final"),
        F.col("stade"),
        F.col("ville"),
        F.col("arbitre"),
        to_ts_s("derniere_maj").alias("derniere_maj"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_ingestion_timestamp"),
        F.col("_batch_id"),
    )
    .filter(F.col("match_id").isNotNull())
    .dropDuplicates(["match_id"])
    .withColumn("is_match_joue", F.lit(False))
    .withColumn("total_buts",         F.lit(None).cast("int"))
    .withColumn("resultat_match",     F.lit(None).cast("string"))
    .withColumn("vainqueur_equipe_id", F.lit(None).cast("long"))
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/matchs_non_joues")

print("Silver OK : matchs_non_joues")
spark.stop()
