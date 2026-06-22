PG_HOST     = "localhost"
PG_PORT     = "5432"
PG_DATABASE = "worldcup2026"
PG_USER     = "postgres"
PG_PASSWORD = "admin"
PG_SCHEMA   = "public"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

JDBC_PROPERTIES = {
    "user":   PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

SILVER_PATH = r"../silver"

METADATA_COLS = [
    "_source_file",
    "_source_path",
    "_ingestion_timestamp",
    "_batch_id",
    "_silver_processed_timestamp",
]
