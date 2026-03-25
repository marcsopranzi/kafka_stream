"""PySpark Structured Streaming consumer for the healthcare log pipeline.

Reads masked events from Kafka, enriches them with ingestion metadata, and
writes to Cassandra.  Designed to run as a stateless K8s Deployment replica;
horizontal scaling is handled by Kafka consumer group partition assignment
(max useful replicas == number of topic partitions).

All configuration is via environment variables so the same image runs both
locally and in Kubernetes without rebuilding.

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS:    Broker list (default: localhost:9092)
    KAFKA_TOPIC_NAME:           Source topic (default: log-topic)
    KAFKA_CONSUMER_GROUP:       Consumer group shared by all replicas (default: spark-consumer)
    KAFKA_STARTING_OFFSETS:     earliest | latest (default: earliest — process backlog)
    CASSANDRA_HOST:             Cassandra hostname (default: localhost)
    CASSANDRA_PORT:             Cassandra port (default: 9042)
    CASSANDRA_KEYSPACE:         Target keyspace (default: logs_keyspace)
    CASSANDRA_TABLE:            Target table (default: logs_table)
    CASSANDRA_ENABLED:          Write to Cassandra when "true" (default: true)
    CHECKPOINT_DIR:             Spark checkpoint path (default: /tmp/spark-checkpoints)
    TRIGGER_INTERVAL:           Micro-batch interval (default: 5 seconds)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Config — all from environment; no hardcoded values
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC_NAME", "log-topic")
# Shared consumer group — Kafka assigns one partition per replica,
# enabling clean horizontal scaling without duplicate processing.
KAFKA_CONSUMER_GROUP: str = os.environ.get("KAFKA_CONSUMER_GROUP", "spark-consumer")
KAFKA_STARTING_OFFSETS: str = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")

CASSANDRA_HOST: str = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT: str = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE: str = os.environ.get("CASSANDRA_KEYSPACE", "logs_keyspace")
CASSANDRA_TABLE: str = os.environ.get("CASSANDRA_TABLE", "logs_table")
CASSANDRA_ENABLED: bool = os.environ.get("CASSANDRA_ENABLED", "true").lower() == "true"

# /tmp is always writable in K8s pods; use a persistent volume or S3 in production.
CHECKPOINT_DIR: str = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
TRIGGER_INTERVAL: str = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
# DLQ output path for records that fail JSON schema parsing.
DLQ_OUTPUT_PATH: str = os.environ.get("DLQ_OUTPUT_PATH", "/tmp/spark-dlq/logs")

# ---------------------------------------------------------------------------
# Schema
# Schema uses nullable=True on all fields so the consumer handles schema
# drift gracefully (PERMISSIVE parse mode) — unknown fields in the JSON
# payload are silently ignored; missing fields resolve to null.
#
# full_name is PHI but arrives pre-masked as sha256:... from the producer.
# Add new source fields here as the upstream schema evolves.
# ---------------------------------------------------------------------------

_SCHEMA = StructType(
    [
        StructField("log_id", StringType(), True),
        StructField("log_region", StringType(), True),
        StructField("log_size", IntegerType(), True),
        StructField("log_datetime", StringType(), True),
        StructField("full_name", StringType(), True),  # PHI — masked upstream
    ]
)


# ---------------------------------------------------------------------------
# Cassandra sink
# ---------------------------------------------------------------------------


def write_to_cassandra(batch_df, batch_id: int) -> None:  # type: ignore[no-untyped-def]
    """Write a micro-batch DataFrame to Cassandra.

    Args:
        batch_df: The current micro-batch as a Spark DataFrame.
        batch_id: Monotonically increasing batch identifier from Spark.
    """
    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .option("spark.cassandra.connection.host", CASSANDRA_HOST)
        .option("spark.cassandra.connection.port", CASSANDRA_PORT)
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", CASSANDRA_TABLE)
        .save()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print(
        f"Starting Spark consumer | topic={KAFKA_TOPIC} "
        f"group={KAFKA_CONSUMER_GROUP} brokers={KAFKA_BOOTSTRAP_SERVERS}"
    )

    spark = (
        SparkSession.builder.appName(f"stream-consumer-{KAFKA_CONSUMER_GROUP}")
        .config("spark.sql.debug.maxToStringFields", "100")
        # Cassandra connector config
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka — kafka.group.id enables Kafka-native partition assignment
    # across replicas so each pod handles a disjoint subset of partitions.
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_CONSUMER_GROUP)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Deserialise JSON preserving the original string for DLQ attribution.
    # PERMISSIVE mode (default): from_json returns NULL for the entire struct
    # when the top-level JSON is unparseable — these rows are routed to the DLQ.
    with_parsed = raw_df.selectExpr("CAST(value AS STRING) AS json_value").select(
        fn.col("json_value"),
        fn.from_json(fn.col("json_value"), _SCHEMA).alias("data"),
    )

    # ── Dead Letter Queue split ───────────────────────────────────────────────
    # data IS NULL  → from_json could not parse the JSON at all (structural error)
    # data IS NOT NULL → well-formed record; proceed to enrichment
    dlq_df = with_parsed.filter(fn.col("data").isNull()).select(
        fn.col("json_value"),
        fn.lit("json_schema_parse_failure").alias("dlq_reason"),
        fn.current_timestamp().alias("dlq_timestamp"),
    )

    valid_df = with_parsed.filter(fn.col("data").isNotNull()).select("data.*")

    # Enrich with pipeline metadata
    enriched_df = valid_df.select(
        fn.expr("uuid()").alias("ingestion_id"),
        fn.col("log_id"),
        fn.col("log_region"),
        fn.col("log_size"),
        fn.col("log_datetime"),
        fn.col("full_name"),
        fn.current_timestamp().alias("ingestion_time"),
    )

    enriched_df.printSchema()

    # ── Sink 1: console (always on — useful for local dev and K8s log streaming)
    console_stream = (
        enriched_df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("update")
        .option("truncate", "false")
        .format("console")
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/console",
        )
        .start()
    )

    # ── Sink 2: Cassandra (toggled via CASSANDRA_ENABLED env var)
    #    Disable when running locally without Cassandra or in unit tests.
    #    In production: always enable and point at a real Cassandra cluster.
    if CASSANDRA_ENABLED:
        (
            enriched_df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
            .outputMode("update")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/cassandra")
            .foreachBatch(write_to_cassandra)
            .start()
        )

    # ── Sink 3: JSON files
    #    Uses /tmp in K8s (ephemeral). In production replace with S3 / GCS path.
    (
        enriched_df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .format("json")
        .option("path", os.environ.get("JSON_OUTPUT_PATH", "/tmp/spark-output/logs"))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/json")
        .start()
    )

    # ── Sink 4 (DLQ): console — makes DLQ records visible in pod logs
    (
        dlq_df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("update")
        .option("truncate", "false")
        .format("console")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq-console")
        .start()
    )

    # ── Sink 5 (DLQ): JSON files — durable record for ops triage and replay
    #    In production replace DLQ_OUTPUT_PATH with an S3 / GCS path monitored
    #    by an alerting system.
    (
        dlq_df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .format("json")
        .option("path", DLQ_OUTPUT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq-json")
        .start()
    )

    console_stream.awaitTermination()
