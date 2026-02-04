# customer_streaming_pipeline.py

import json
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
# Load JSON config
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    logger.info(f"Loading config from {path}")
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Spark session creation
# ---------------------------------------------------------------------------

def create_spark_session(config: dict) -> SparkSession:
    logger.info("Creating Spark session")
    spark = (
        SparkSession.builder.appName(config["app_name"])
        .config("spark.mongodb.write.connection.uri", config["mongodb"]["uri"])
        .config("spark.mongodb.write.database", config["mongodb"]["database"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Masking UDFs
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
    return "*" * (len(digits) - 4) + digits[-4:]


@F.udf(T.StringType())
def mask_national_id(national_id: str) -> str:
    if national_id is None:
        return None
    if len(national_id) <= 4:
        return "*" * len(national_id)
    return "*" * (len(national_id) - 4) + national_id[-4:]


# ---------------------------------------------------------------------------
# Cleansing
# ---------------------------------------------------------------------------

def cleanse_common_fields(df: DataFrame) -> DataFrame:
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    if "event_time" in df.columns:
        df = df.withColumn("event_time", F.to_timestamp("event_time"))

    if "amount" in df.columns:
        df = df.withColumn("amount", F.col("amount").cast(T.DoubleType()))
        df = df.fillna({"amount": 0.0})

    if "status" in df.columns:
        df = df.fillna({"status": "UNKNOWN"})

    return df


def remove_duplicates(df: DataFrame, key_cols: list) -> DataFrame:
    return df.dropDuplicates(key_cols)


# ---------------------------------------------------------------------------
# Governance
# ---------------------------------------------------------------------------

def apply_governance_rules(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    required_cols = ["id", "event_time", "customer_id"]
    allowed_status = ["NEW", "ACTIVE", "INACTIVE", "CLOSED", "UNKNOWN"]

    df = df.withColumn("id_not_null", F.col("id").isNotNull())
    df = df.withColumn("event_time_not_null", F.col("event_time").isNotNull())
    df = df.withColumn("customer_id_not_null", F.col("customer_id").isNotNull())
    df = df.withColumn("amount_valid", (F.col("amount") >= 0) | F.col("amount").isNull())
    df = df.withColumn("status_valid", F.col("status").isin(allowed_status))

    df = df.withColumn(
        "is_valid",
        F.col("id_not_null")
        & F.col("event_time_not_null")
        & F.col("customer_id_not_null")
        & F.col("amount_valid")
        & F.col("status_valid")
    )

    df = df.withColumn(
        "invalid_reason",
        F.when(~F.col("id_not_null"), "MISSING_ID")
        .when(~F.col("event_time_not_null"), "MISSING_EVENT_TIME")
        .when(~F.col("customer_id_not_null"), "MISSING_CUSTOMER_ID")
        .when(~F.col("amount_valid"), "INVALID_AMOUNT")
        .when(~F.col("status_valid"), "INVALID_STATUS")
    )

    valid_df = df.filter("is_valid = true")
    invalid_df = df.filter("is_valid = false")

    return valid_df, invalid_df


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------

def apply_masking(df: DataFrame) -> DataFrame:
    if "email" in df.columns:
        df = df.withColumn("email_masked", mask_email("email"))
    if "phone" in df.columns:
        df = df.withColumn("phone_masked", mask_phone("phone"))
    if "national_id" in df.columns:
        df = df.withColumn("national_id_masked", mask_national_id("national_id"))
    return df


# ---------------------------------------------------------------------------
# Read sources
# ---------------------------------------------------------------------------

def read_datalake_parquet(spark, config):
    return (
        spark.read.format("parquet")
        .load(config["datalake"]["parquet_path"])
        .withColumn("source_system", F.lit("datalake_parquet"))
    )


def read_datalake_csv(spark, config):
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("phone", T.StringType(), True),
        T.StructField("national_id", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("amount", T.StringType(), True),
        T.StructField("status", T.StringType(), True)
    ])

    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(config["datalake"]["csv_path"])
        .withColumn("source_system", F.lit("datalake_csv"))
    )


def read_jdbc_table(spark, config):
    jdbc = config["jdbc"]

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc["url"])
        .option("driver", jdbc["driver"])
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("dbtable", jdbc["table"])
        .load()
    )

    df = df.withColumn("source_system", F.lit("jdbc_db"))
    return df


def read_kafka_stream(spark, config):
    kafka = config["kafka"]

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka["bootstrap_servers"])
        .option("subscribe", kafka["topic"])
        .option("startingOffsets", kafka["starting_offsets"])
        .load()
    )

    json_schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("phone", T.StringType(), True),
        T.StructField("national_id", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("status", T.StringType(), True)
    ])

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("json_data", F.from_json("json_str", json_schema))
        .select("json_data.*")
        .withColumn("source_system", F.lit("eventhub_kafka"))
    )

    return parsed


# ---------------------------------------------------------------------------
# MongoDB writer
# ---------------------------------------------------------------------------

def write_to_mongo(df, config, collection, mode="append"):
    (
        df.write.format("mongodb")
        .mode(mode)
        .option("uri", config["mongodb"]["uri"])
        .option("database", config["mongodb"]["database"])
        .option("collection", collection)
        .save()
    )


# ---------------------------------------------------------------------------
# Streaming foreachBatch
# ---------------------------------------------------------------------------

def process_stream_batch(batch_df, batch_id, config, static_df):
    if batch_df.rdd.isEmpty():
        return

    all_cols = static_df.columns

    for col in all_cols:
        if col not in batch_df.columns:
            batch_df = batch_df.withColumn(col, F.lit(None))

    batch_df = batch_df.select(all_cols)

    unified = static_df.unionByName(batch_df)

    unified = cleanse_common_fields(unified)
    unified = remove_duplicates(unified, ["id"])
    unified = apply_masking(unified)

    valid_df, invalid_df = apply_governance_rules(unified)

    write_to_mongo(valid_df, config, config["mongodb"]["curated_collection"])
    write_to_mongo(invalid_df, config, config["mongodb"]["quarantine_collection"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config("configs/config.json")
    spark = create_spark_session(config)

    parquet_df = read_datalake_parquet(spark, config)
    csv_df = read_datalake_csv(spark, config)
    jdbc_df = read_jdbc_table(spark, config)

    batch_df = parquet_df.unionByName(csv_df).unionByName(jdbc_df)
    batch_df = cleanse_common_fields(batch_df)
    batch_df = remove_duplicates(batch_df, ["id"])
    batch_df = apply_masking(batch_df)

    valid_df, invalid_df = apply_governance_rules(batch_df)

    write_to_mongo(valid_df, config, config["mongodb"]["curated_collection"], mode="overwrite")
    write_to_mongo(invalid_df, config, config["mongodb"]["quarantine_collection"], mode="overwrite")

    static_df = batch_df.cache()

    stream_df = read_kafka_stream(spark, config)

    query = (
        stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_stream_batch(df, batch_id, config, static_df)
        )
        .outputMode("update")
        .option("checkpointLocation", config["checkpoint_location"])
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
