#  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 consumer_spark.py 
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn


TOPIC_NAME = "test"
KAFKA_BOOTSTRAP_SERVERS_URI = 'localhost:9092'

# MongoDB Cluster Details
mongodb_host_name = "kafkacluster.i1qem1x.mongodb.net"
mongodb_port_no = "27017"
mongodb_user_name = ""
mongodb_password = ""
mongodb_database_name = "kafkaCluster"
mongodb_collection_name = "regions_traffic"
spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

def save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name):
    print("Inside save_to_mongodb_collection function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mongodb_collection_name: " + mongodb_collection_name)
    
    current_df.write.format("mongo") \
        .mode("append") \
        .option("uri", spark_mongodb_output_uri) \
        .option("database", mongodb_database_name) \
        .option("collection", mongodb_collection_name) \
        .save()
    print("Exit out of save_to_mongodb_collection function")

def write_mongo_row(df, epoch_id):
    mongoURL = spark_mongodb_output_uri
    df.write.format("mongo").mode("append").option("uri",mongoURL).save()

if __name__ == "__main__":
    print("Ingestion Started...")

    spark = SparkSession \
        .builder \
        .appName("Live Stream Data") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # sc = SparkContext()
    # sparkC = SparkSession(sc)   

    logs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_URI) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([ StructField('transaction_id', StringType(), True), \
                          StructField('transaction_region', StringType(), True), \
                          StructField('transaction_size', IntegerType(), True), \
                          StructField('transaction_datetime', StringType(), True) \
                        ])

    logs_df_1 = logs_df.selectExpr("CAST(value AS STRING)")

    logs_df_2 = logs_df_1. \
            select(from_json(col("value"), schema). \
            alias("logs_json"))

    logs_df_3 = logs_df_2.select("logs_json.*").withColumn("timestamp", fn.current_timestamp())

    logs_stream_df = logs_df_3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    region_df = logs_df_3.\
                    withWatermark("timestamp", "1 minutes").\
                    groupby('transaction_region').agg(fn.sum('transaction_size').alias('transaction_size'))

    # logs_stream_df = region_df \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()
    
    # print('count: \n', logs_stream_df.count())

    try:
            
        print('-----------------------------------------------------SAVING DF')
        query=region_df.writeStream.foreachBatch(write_mongo_row).start()
        print('----------------------------------------------------DF SAVED')
    except Exception as e:
        print('--------------------------------------------failure')
        print(e)
        print('---------------------------------------------error finished')

    logs_stream_df.awaitTermination()
