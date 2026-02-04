import logging
import sys
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("streaming_pipeline")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    # Data lake paths
    DATALAKE_PARQUET_PATH = "abfss://raw@account.dfs.core.windows.net/customers/parquet/"
    DATALAKE_CSV_PATH = "abfss://raw@account.dfs.core.windows.net/customers/csv/"

    # JDBC configuration (placeholders)
    JDBC_URL = "jdbc:sqlserver://your-sql-server:1433;databaseName=your_db"
    JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    JDBC_USER = "username"
    JDBC_PASSWORD = "password"
    JDBC_TABLE = "dbo.customers"

    # Kafka / Event Hub configuration (placeholders)
    KAFKA_BOOTSTRAP_SERVERS = "your-kafka-bootstrap:9092"
    KAFKA_TOPIC = "customer-events"
    KAFKA_STARTING_OFFSETS = "latest"  # or "earliest"

    # MongoDB configuration (placeholders)
    MONGO_URI = "mongodb://username:password@your-mongo-host:27017"
    MONGO_DATABASE = "analytics"
    MONGO_CURATED_COLLECTION = "customers_curated"
    MONGO_QUARANTINE_COLLECTION = "customers_quarantine"

    # Misc
    APP_NAME = "CustomerStreamingPipeline"
    CHECKPOINT_LOCATION = "abfss://checkpoint@account.dfs.core.windows.net/customer_pipeline/checkpoints/"


# ---------------------------------------------------------------------------
# Spark session creation
# ---------------------------------------------------------------------------

def create_spark_session(config: Config) -> SparkSession:
    logger.info("Creating Spark session")
    spark = (
        SparkSession.builder.appName(config.APP_NAME)
        .config("spark.mongodb.write.connection.uri", config.MONGO_URI)
        .config("spark.mongodb.write.database", config.MONGO_DATABASE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Helper functions - masking
# ---------------------------------------------------------------------------

@F.udf(T.StringType())
def mask_email(email: str) -> str:
    if email is None:
        return None
    try:
        username, domain = email.split("@", 1)
        if len(username) <= 2:
            masked_username = "*" * len(username)
        else:
            masked_username = username[0] + "*" * (len(username) - 2) + username[-1]
        return f"{masked_username}@{domain}"
    except Exception:
        return None


@F.udf(T.StringType())
def mask_phone(phone: str) -> str:
    if phone is None:
        return None
    digits = "".join([c for c in phone if c.isdigit()])
    if len(digits) <= 4:
        return "*" * len(digits)
    masked = "*" * (len(digits) - 4) + digits[-4:]
    return masked


@F.udf(T.StringType())
def mask_national_id(national_id: str) -> str:
    if national_id is None:
        return None
    if len(national_id) <= 4:
        return "*" * len(national_id)
    return "*" * (len(national_id) - 4) + national_id[-4:]


# ---------------------------------------------------------------------------
# Helper functions - cleansing
# ---------------------------------------------------------------------------

def cleanse_common_fields(df: DataFrame) -> DataFrame:
    """
    Applying common cleansing rules:
    - Trim string columns
    - Standardize date formats
    - Cast types where needed
    - Handle nulls
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Example: standardize event_time to timestamp
    if "event_time" in df.columns:
        df = df.withColumn(
            "event_time",
            F.to_timestamp("event_time"),
        )

    # Example: cast amount to double and fill nulls with 0.0
    if "amount" in df.columns:
        df = df.withColumn("amount", F.col("amount").cast(T.DoubleType()))
        df = df.fillna({"amount": 0.0})

    # Example: fill null status with 'UNKNOWN'
    if "status" in df.columns:
        df = df.fillna({"status": "UNKNOWN"})

    return df


def remove_duplicates(df: DataFrame, key_cols: list) -> DataFrame:
    if not key_cols:
        return df
    return df.dropDuplicates(key_cols)


# ---------------------------------------------------------------------------
# Helper functions - governance / validation
# ---------------------------------------------------------------------------

def apply_governance_rules(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Applying data quality rules and split into valid and invalid records.
    Rules:
      - id, event_time, customer_id not null
      - amount >= 0
      - status in allowed list
    """
    required_cols = ["id", "event_time", "customer_id"]
    allowed_status = ["NEW", "ACTIVE", "INACTIVE", "CLOSED", "UNKNOWN"]

    df_with_flags = df

    # Required fields not null
    for col in required_cols:
        if col in df_with_flags.columns:
            df_with_flags = df_with_flags.withColumn(
                f"{col}_not_null", F.col(col).isNotNull()
            )
        else:
            df_with_flags = df_with_flags.withColumn(f"{col}_not_null", F.lit(False))

    # Amount >= 0
    if "amount" in df_with_flags.columns:
        df_with_flags = df_with_flags.withColumn(
            "amount_valid",
            (F.col("amount") >= 0.0) | F.col("amount").isNull(),
        )
    else:
        df_with_flags = df_with_flags.withColumn("amount_valid", F.lit(True))

    # Status in allowed list
    if "status" in df_with_flags.columns:
        df_with_flags = df_with_flags.withColumn(
            "status_valid",
            F.col("status").isin(allowed_status),
        )
    else:
        df_with_flags = df_with_flags.withColumn("status_valid", F.lit(True))

    # Overall validity
    validity_expr = (
        F.col("id_not_null")
        & F.col("event_time_not_null")
        & F.col("customer_id_not_null")
        & F.col("amount_valid")
        & F.col("status_valid")
    )

    df_with_flags = df_with_flags.withColumn("is_valid", validity_expr)

    # Reason for invalidity
    df_with_flags = df_with_flags.withColumn(
        "invalid_reason",
        F.when(~F.col("id_not_null"), F.lit("MISSING_ID"))
        .when(~F.col("event_time_not_null"), F.lit("MISSING_EVENT_TIME"))
        .when(~F.col("customer_id_not_null"), F.lit("MISSING_CUSTOMER_ID"))
        .when(~F.col("amount_valid"), F.lit("INVALID_AMOUNT"))
        .when(~F.col("status_valid"), F.lit("INVALID_STATUS"))
        .otherwise(F.lit(None)),
    )

    valid_df = df_with_flags.filter(F.col("is_valid") == True).drop(
        "id_not_null",
        "event_time_not_null",
        "customer_id_not_null",
        "amount_valid",
        "status_valid",
    )

    invalid_df = df_with_flags.filter(F.col("is_valid") == False).drop(
        "id_not_null",
        "event_time_not_null",
        "customer_id_not_null",
        "amount_valid",
        "status_valid",
    )

    return valid_df, invalid_df


# ---------------------------------------------------------------------------
# Helper functions - masking application
# ---------------------------------------------------------------------------

def apply_masking(df: DataFrame) -> DataFrame:
    if "email" in df.columns:
        df = df.withColumn("email_masked", mask_email(F.col("email")))
    if "phone" in df.columns:
        df = df.withColumn("phone_masked", mask_phone(F.col("phone")))
    if "national_id" in df.columns:
        df = df.withColumn("national_id_masked", mask_national_id(F.col("national_id")))
    return df


# ---------------------------------------------------------------------------
# Read from sources
# ---------------------------------------------------------------------------

def read_datalake_parquet(spark: SparkSession, config: Config) -> DataFrame:
    logger.info("Reading Parquet data from data lake with schema inference")
    df = (
        spark.read.format("parquet")
        .option("mergeSchema", "true")
        .load(config.DATALAKE_PARQUET_PATH)
    )
    df = df.withColumn("source_system", F.lit("datalake_parquet"))
    return df


def read_datalake_csv(spark: SparkSession, config: Config) -> DataFrame:
    logger.info("Reading CSV data from data lake with explicit schema")
    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("phone", T.StringType(), True),
            T.StructField("national_id", T.StringType(), True),
            T.StructField("event_time", T.StringType(), True),
            T.StructField("amount", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
        ]
    )

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(config.DATALAKE_CSV_PATH)
    )
    df = df.withColumn("source_system", F.lit("datalake_csv"))
    return df


def read_jdbc_table(spark: SparkSession, config: Config) -> DataFrame:
    logger.info("Reading data from JDBC source")
    df = (
        spark.read.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("driver", config.JDBC_DRIVER)
        .option("user", config.JDBC_USER)
        .option("password", config.JDBC_PASSWORD)
        .option("dbtable", config.JDBC_TABLE)
        .load()
    )

    # Align schema with other sources
    # Example: rename columns and cast types
    df = (
        df.withColumnRenamed("CustomerId", "customer_id")
        .withColumnRenamed("CustomerEmail", "email")
        .withColumnRenamed("CustomerPhone", "phone")
        .withColumnRenamed("NationalId", "national_id")
        .withColumnRenamed("EventTime", "event_time")
        .withColumnRenamed("Amount", "amount")
        .withColumnRenamed("Status", "status")
    )

    df = df.withColumn("id", F.col("id").cast(T.StringType()))
    df = df.withColumn("source_system", F.lit("jdbc_db"))
    return df


def read_kafka_stream(spark: SparkSession, config: Config) -> DataFrame:
    logger.info("Reading streaming data from Kafka/Event Hub")
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .load()
    )

    # Parse JSON payload from 'value' column
    json_schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("phone", T.StringType(), True),
            T.StructField("national_id", T.StringType(), True),
            T.StructField("event_time", T.StringType(), True),
            T.StructField("amount", T.DoubleType(), True),
            T.StructField("status", T.StringType(), True),
        ]
    )

    parsed_df = (
        raw_stream_df.selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("json_data", F.from_json("json_str", json_schema))
        .select("json_data.*")
    )

    parsed_df = parsed_df.withColumn("source_system", F.lit("eventhub_kafka"))
    return parsed_df


# ---------------------------------------------------------------------------
# Schema alignment & unification
# ---------------------------------------------------------------------------

def align_and_union_batch_sources(
    parquet_df: DataFrame, csv_df: DataFrame, jdbc_df: DataFrame
) -> DataFrame:
    logger.info("Aligning schemas and unioning batch sources")

    # Ensure all have the same set of columns
    all_columns = [
        "id",
        "customer_id",
        "email",
        "phone",
        "national_id",
        "event_time",
        "amount",
        "status",
        "source_system",
    ]

    def select_all(df: DataFrame) -> DataFrame:
        for col in all_columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None).cast(T.StringType()))
        # Cast amount to double explicitly
        df = df.withColumn("amount", F.col("amount").cast(T.DoubleType()))
        return df.select(*all_columns)

    parquet_aligned = select_all(parquet_df)
    csv_aligned = select_all(csv_df)
    jdbc_aligned = select_all(jdbc_df)

    unified_df = parquet_aligned.unionByName(csv_aligned).unionByName(jdbc_aligned)
    return unified_df


# ---------------------------------------------------------------------------
# MongoDB writers
# ---------------------------------------------------------------------------

def write_batch_to_mongo(df: DataFrame, config: Config, collection: str, mode: str = "append") -> None:
    logger.info("Writing batch DataFrame to MongoDB collection '%s'", collection)
    (
        df.write.format("mongodb")
        .mode(mode)
        .option("uri", config.MONGO_URI)
        .option("database", config.MONGO_DATABASE)
        .option("collection", collection)
        .save()
    )


# ---------------------------------------------------------------------------
# Streaming foreachBatch logic
# ---------------------------------------------------------------------------

def process_stream_batch(
    batch_df: DataFrame,
    batch_id: int,
    config: Config,
    static_batch_df: DataFrame,
) -> None:
    logger.info("Processing micro-batch %s", batch_id)

    if batch_df.rdd.isEmpty():
        logger.info("Micro-batch %s is empty, skipping", batch_id)
        return

    # Align streaming batch with static schema
    all_columns = [
        "id",
        "customer_id",
        "email",
        "phone",
        "national_id",
        "event_time",
        "amount",
        "status",
        "source_system",
    ]

    for col in all_columns:
        if col not in batch_df.columns:
            batch_df = batch_df.withColumn(col, F.lit(None).cast(T.StringType()))
    batch_df = batch_df.withColumn("amount", F.col("amount").cast(T.DoubleType()))
    batch_df = batch_df.select(*all_columns)

    # Union streaming micro-batch with static batch data
    unified_df = static_batch_df.unionByName(batch_df)

    # Cleansing
    unified_df = cleanse_common_fields(unified_df)
    unified_df = remove_duplicates(unified_df, ["id"])

    # Masking
    unified_df = apply_masking(unified_df)

    # Governance
    valid_df, invalid_df = apply_governance_rules(unified_df)

    # Write to MongoDB
    write_batch_to_mongo(valid_df, config, config.MONGO_CURATED_COLLECTION, mode="append")
    if invalid_df.count() > 0:
        write_batch_to_mongo(
            invalid_df, config, config.MONGO_QUARANTINE_COLLECTION, mode="append"
        )


# ---------------------------------------------------------------------------
# Main application logic
# ---------------------------------------------------------------------------

def main():
    config = Config()
    spark = create_spark_session(config)

    try:
        # -----------------------------
        # Read batch sources
        # -----------------------------
        parquet_df = read_datalake_parquet(spark, config)
        csv_df = read_datalake_csv(spark, config)
        jdbc_df = read_jdbc_table(spark, config)

        # -----------------------------
        # Align and union batch sources
        # -----------------------------
        batch_unified_df = align_and_union_batch_sources(parquet_df, csv_df, jdbc_df)

        # Apply cleansing, masking, governance for initial batch load
        batch_unified_df = cleanse_common_fields(batch_unified_df)
        batch_unified_df = remove_duplicates(batch_unified_df, ["id"])
        batch_unified_df = apply_masking(batch_unified_df)
        batch_valid_df, batch_invalid_df = apply_governance_rules(batch_unified_df)

        # Initial batch write to MongoDB
        write_batch_to_mongo(
            batch_valid_df,
            config,
            config.MONGO_CURATED_COLLECTION,
            mode="overwrite",
        )
        if batch_invalid_df.count() > 0:
            write_batch_to_mongo(
                batch_invalid_df,
                config,
                config.MONGO_QUARANTINE_COLLECTION,
                mode="overwrite",
            )

        # Cache static batch unified DF for use in streaming micro-batches
        static_batch_df = batch_unified_df.select(
            "id",
            "customer_id",
            "email",
            "phone",
            "national_id",
            "event_time",
            "amount",
            "status",
            "source_system",
        ).cache()

        # -----------------------------
        # Read streaming source
        # -----------------------------
        kafka_stream_df = read_kafka_stream(spark, config)

        # -----------------------------
        # Streaming write with foreachBatch
        # -----------------------------
        query = (
            kafka_stream_df.writeStream.foreachBatch(
                lambda df, batch_id: process_stream_batch(
                    df, batch_id, config, static_batch_df
                )
            )
            .outputMode("update")
            .option("checkpointLocation", config.CHECKPOINT_LOCATION)
            .start()
        )

        logger.info("Streaming query started. Awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.exception("Pipeline failed with exception: %s", e)
        raise
    finally:
        logger.info("Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()
