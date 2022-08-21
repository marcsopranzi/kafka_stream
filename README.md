# Real Stream Process

## Project Focus

The project focus is to provide an already running test environemnt, with compatible versions of Spark, Kafka and Cassandra. While the Kafka producer does generate dummy data, this could be easily adapted to plug in to a real API using [Apache Nifi](https://nifi.apache.org/), [AWS Glue](https://aws.amazon.com/glue/), etc.

## Requirements
To be able to run this code you need to have installed Java 8, Docker, Spark 3.0.0 and Python 3.10. The pipeline has been tested using [Ubuntu 22.04 LTS Jammy Jellyfish](https://releases.ubuntu.com/22.04/) You should be able to run the code with a backwards compatibily up to [Ubunut 18.04.6 LTS](https://releases.ubuntu.com/18.04/) If you are using a Mac, you should switch the `apt get` commands to `brew`.

## Instalation
Once you have cloned the repo you can execute the "setup_env" and this will install the needed dependencies to run the project locally.

## Execution
Kafka and Cassandra come available as Docker images and can be launched with `docker-compose up`. After a moment if you run `docker ps` you should see 3 containers running: Cassandra, Zookeeper and Bootstrap server. To create your own topics and database you can log into the kafka container and create a topic. You can find notes on how to quikly get started with this [Kafka guide](https://kafka.apache.org/quickstart). You can log into the Cassandra image with `cassandra`as both username and password. 
You can update your hosts, ports and names inside the `config.py` Once done you can start your producer with `python3 producer.py` and kick off your consumer with `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 consumer_spark.py`.

## Notes
The checkpoint option saves data in your local drive, as it is good practice to save the data phisically to prevent data losses in case of infra issues. Depending on which data you use, the space can be used up very quickly so always keep checking it. 
