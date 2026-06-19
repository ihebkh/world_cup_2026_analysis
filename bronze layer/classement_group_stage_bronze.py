from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid
import os

# ============================================================
# BRONZE LAYER - Classement Group Stage
# Source CSV  : classement_group_stage.csv
# Sortie      : Parquet
# ============================================================

spark = (
    SparkSession.builder
    .appName("Bronze_Classement_Group_Stage")
    .getOrCreate()
)

# =========================
# CONFIGURATION
# =========================

# Dossier contenant les fichiers CSV
SOURCE_PATH = r"./data/classements"

# Dossier Bronze Layer
BRONZE_PATH = r"./bronze"

CSV_FILE = "classement_group_stage.csv"
TABLE_NAME = "classement_group_stage"
BATCH_ID = str(uuid.uuid4())

input_path = os.path.join(SOURCE_PATH, CSV_FILE)
output_path = os.path.join(BRONZE_PATH, TABLE_NAME)

# =========================
# LECTURE CSV
# =========================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")  # Bronze = données brutes, tout en string
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(input_path)
)

# =========================
# AJOUT COLONNES TECHNIQUES
# =========================

df_bronze = (
    df.withColumn("_source_file", lit(CSV_FILE))
      .withColumn("_source_path", input_file_name())
      .withColumn("_ingestion_timestamp", current_timestamp())
      .withColumn("_batch_id", lit(BATCH_ID))
)

# =========================
# ECRITURE PARQUET
# =========================

(
    df_bronze.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Bronze OK : {CSV_FILE} -> {output_path}")

spark.stop()
