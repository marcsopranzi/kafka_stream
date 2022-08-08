#  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 consumer_spark.py
# create keyspace logs_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
# use logs_keyspace;
# create table logs_keyspace.logs_table (transaction_id text ,transaction_region text ,transaction_size int ,transaction_datetime text ,tran_date text ,id text primary key);

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
# import org.apache.spark.sql.cassandra._

from pyspark.sql.types import *
import pyspark.sql.functions as fn

import uuid
import config

TOPIC_NAME = "test"
KAFKA_BOOTSTRAP_SERVERS_URI = 'localhost:9092'

# Cassandra Cluster Details
cassandra_connection_host = "localhost"
cassandra_connection_port = "9042"
cassandra_keyspace_name = "logs_keyspace"
cassandra_table_name = "logs_table"


def save_to_cassandra_table(current_df, epoc_id):
    print("Inside save_to_cassandra_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    current_df \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("spark.cassandra.connection.host", cassandra_connection_host) \
    .option("spark.cassandra.connection.port", cassandra_connection_port) \
    .option("keyspace", cassandra_keyspace_name) \
    .option("table", cassandra_table_name) \
    .save()
    print("Exit out of save_to_cassandra_table function")


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

    schema = StructType([StructField('transaction_id', StringType(), True),
                         StructField('transaction_region', StringType(), True),
                         StructField('transaction_size', IntegerType(), True),
                         StructField('transaction_datetime',
                                     StringType(), True)
                         ])

    logs_df_1 = logs_df.selectExpr("CAST(value AS STRING)")

    logs_df_2 = logs_df_1. \
        select(fn.from_json(fn.col("value"), schema).
               alias("logs_json"))
    
    logs_df_3 = logs_df_2.select('logs_json.*')

    uuidUdf= fn.udf(lambda : str(uuid.uuid4()),StringType()) 

    logs_df_4 = logs_df_3.withColumn("tran_date", fn.from_unixtime(fn.col("transaction_datetime"), "yyyy-MM-dd HH:mm:ss")).withColumn("id",uuidUdf())

    logs_stream_df = logs_df_4 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()  

    logs_df_4.writeStream \
      .trigger(processingTime='5 seconds') \
      .format("json") \
      .option("path", "data/json/trans_detail_raw_data") \
      .option("checkpointLocation", "data/checkpoint/trans_detail_raw_data") \
      .start()
    
    logs_df_4.printSchema()

    print('\nlogs_df_4\n',type(logs_df_4))

    logs_df_4.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .foreachBatch(save_to_cassandra_table) \
        .start()    

    # logs_df_4.write.cassandraFormat(cassandra_keyspace_name, cassandra_table_name)\
    #     .mode("append")\
    #     .save()
   
    
    
    logs_stream_df.awaitTermination()
