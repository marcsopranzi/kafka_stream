from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC_NAME = "test"
KAFKA_BOOTSTRAP_SERVERS_URI = 'localhost:9092'

if __name__ == "__main__":
    print("Ingestion Started...")

    spark = SparkSession \
        .builder \
        .appName("Live Stream Data") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_URI) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([ \
                            StructField('transaction_id', StringType(), True), \
                            StructField('transaction_region', StringType(), True), \
                            StructField('transaction_size', StringType(), True), \
                            StructField('transaction_datetime', StringType(), True) \
                        ])

    logs_df_1 = logs_df.selectExpr("CAST(value AS STRING)")

    logs_df_2 = logs_df_1. \
            select(from_json(col("value"), schema). \
            alias("logs_json"))

    logs_df_3 = logs_df_2.select("logs_json.*")

    logs_df_3.printSchema()

    logs_stream_df = logs_df_3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()


    logs_stream_df.awaitTermination()



