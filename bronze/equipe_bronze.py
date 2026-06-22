from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid
import os

# ============================================================
# BRONZE LAYER - Equipes
# Source CSV  : ../DATA/equipes.csv
# Sortie      : ./equipes  (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Bronze_Equipes")
    .getOrCreate()
)

SOURCE_PATH = r"../DATA"
BRONZE_PATH = r"."

CSV_FILE   = "equipes.csv"
TABLE_NAME = "equipes"
BATCH_ID   = str(uuid.uuid4())

input_path  = os.path.join(SOURCE_PATH, CSV_FILE)
output_path = os.path.join(BRONZE_PATH, TABLE_NAME)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(input_path)
)

df_bronze = (
    df.withColumn("_source_file", lit(CSV_FILE))
      .withColumn("_source_path", input_file_name())
      .withColumn("_ingestion_timestamp", current_timestamp())
      .withColumn("_batch_id", lit(BATCH_ID))
)

df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Bronze OK : {CSV_FILE} -> {output_path}")
spark.stop()
