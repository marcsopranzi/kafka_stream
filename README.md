# Real Stream Process

[![Kafka-Python](https://img.shields.io/pypi/pyversions/kafka-python.svg)](https://pypi.python.org/pypi/kafka-python)
[![Numpy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)](https://pypi.org/project/numpy/)
[![Docker Pulls](https://img.shields.io/badge/cassandra-%231287B1.svg?style=for-the-badge&logo=apache-cassandra&logoColor=white)](https://hub.docker.com/_/cassandra)
[![Docker Pulls](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://hub.docker.com/r/confluentinc/cp-zookeeper/tags)
[![Docker Pulls](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://hub.docker.com/r/confluentinc/cp-server)

## Project Focus

The project focus is to provide a test environemnt already running and with compatible versions of Spark, Kafka and Cassandra. While the Kafka producer does generate dummy data this could be easily adapted to plug in to a real API using [Apache Nifi](https://nifi.apache.org/), [AWS Glue](https://aws.amazon.com/glue/)