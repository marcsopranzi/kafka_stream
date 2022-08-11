# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 consumer_spark.py
# kafka-topics --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic logs-topic2
# cqlsh -u cassandra -p cassandra
# create keyspace logs_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
# use logs_keyspace;
# create table logs_keyspace.logs_table (ingestion_id text primary key, log_id text ,log_region text ,log_size int ,log_datetime text ,ingestion_time timestamp);

from pyspark.sql import SparkSession
#
from pyspark.sql.types import *
import pyspark.sql.functions as fn
import uuid
from config import parameters

TOPIC_NAME = parameters["KAFKA_TOPIC_NAME"]
KAFKA_BOOTSTRAP_SERVERS = parameters["KAFKA_BOOTSTRAP_SERVER"]

cassandra_connection_host = parameters["cassandra_connection_host"]
cassandra_connection_port = parameters["cassandra_connection_port"]
cassandra_keyspace_name = parameters["cassandra_keyspace_name"]
cassandra_table_name = parameters["cassandra_table_name"]


def write_to_cassandra(current_df, batch_id):

    current_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("spark.cassandra.connection.host", cassandra_connection_host) \
        .option("spark.cassandra.connection.port", cassandra_connection_port) \
        .option("keyspace", cassandra_keyspace_name) \
        .option("table", cassandra_table_name) \
        .save()



if __name__ == "__main__":
    print("Ingestion Started...")

    spark = SparkSession \
        .builder \
        .appName("Stream Data") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([StructField('log_id', StringType(), True),
                         StructField('log_region', StringType(), True),
                         StructField('log_size', IntegerType(), True),
                         StructField('log_datetime', StringType(), True)
                         ])

    logs_df_1 = logs_df.selectExpr("CAST(value AS STRING)")

    logs_df_2 = logs_df_1. \
        select(fn.from_json(fn.col("value"), schema).
               alias("logs_json"))

    uuidUdf = fn.udf(lambda: str(uuid.uuid4()), StringType())

    logs_df_3 = logs_df_2.select('logs_json.*').withColumn(
        "ingestion_id", uuidUdf()).withColumn("ingestion_time", fn.current_timestamp())

    logs_df_4 = logs_df_3.select(["ingestion_id",
                                  'log_id',
                                  'log_region',
                                  'log_size',
                                  'log_datetime',
                                  'ingestion_time'])

    logs_stream_df = logs_df_4.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    logs_df_4.writeStream \
        .trigger(processingTime='5 seconds') \
        .format("json") \
        .option("path", "data/json/logs") \
        .option("checkpointLocation", "data/checkpoint/logs") \
        .start()

    logs_df_4.printSchema()

    logs_df_4.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .start()

    logs_stream_df.awaitTermination()
