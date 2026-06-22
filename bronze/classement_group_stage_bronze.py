from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid
import os

# ============================================================
# BRONZE LAYER - Classement Group Stage
# Source CSV  : ../DATA/classements/classement_group_stage.csv
# Sortie      : ./classement_group_stage  (parquet dans ce même dossier)
# ============================================================

spark = (
    SparkSession.builder
    .appName("Bronze_Classement_Group_Stage")
    .getOrCreate()
)

SOURCE_PATH = r"../DATA/classements"
BRONZE_PATH = r"."

CSV_FILE   = "classement_group_stage.csv"
TABLE_NAME = "classement_group_stage"
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
