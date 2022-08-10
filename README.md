# Real Stream Process

[![Kafka-Python](https://img.shields.io/pypi/pyversions/kafka-python.svg)](https://pypi.python.org/pypi/kafka-python)
[![Numpy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)](https://pypi.org/project/numpy/)
[![Docker Pulls](https://img.shields.io/badge/cassandra-%231287B1.svg?style=for-the-badge&logo=apache-cassandra&logoColor=white)](https://hub.docker.com/_/cassandra)
[![Docker Pulls](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://hub.docker.com/r/confluentinc/cp-zookeeper/tags)
[![Docker Pulls](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://hub.docker.com/r/confluentinc/cp-server)

## Project Focus

The project focus is to provide a test environemnt already running and with compatible versions of Spark, Kafka and Cassandra. While the Kafka producer does generate dummy data this could be easily adapted to plug in to a real API using [Apache Nifi](https://nifi.apache.org/), [AWS Glue](https://aws.amazon.com/glue/)

## Requirements.
To be able to run this code you need to have installed Docker, Spark 3.0.0 and Python 3.10.

## Instalation
Once you cloned the repo you can execute the "setup_env" and this will install you the needed dependencies to run the project locally.

## Notes
Kafka and Cassandra come available as a Docker images and can be launched with `docker-compose -d`. After a moment if you run `docker ps` you should see 3 containers running: Cassandra, Zookeeper and Bootstrap server. To create your own topics and database you can just loging to the kafka container and crea a topic, you can find notes on how to quikly get started with this [Kafka guide](https://kafka.apache.org/quickstart). The Cassandra image can be logged in with user name and password both `cassandra`.
